"""Scenario D: one product of an external project loaded incrementally, run after run.

A product whose Bronze source is incremental and bounded (each run extracts at
most `rows_per_run` rows past its cursor) is run `runs` times on the same
stage. Every run must add exactly one slice to each incremental Silver entity,
keep its primary key unique, and leave each mirrored Gold table as large as its
Silver table. The per-run layer times then show what a merge costs as Silver
and Gold grow. A run killed during Bronze, its retry, and a full refresh follow.

The platform is installed from the project's own Deployment Profile, so the
deployment measured is the one the project declares.

The stage's source bindings come from this process's environment, under the
names the loader itself reads (`SOURCES__<SOURCE>__<KEY>`): values never
appear in the result or a log.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from olf.infra.e2e._shell import E2EConfig, kubectl
from olf.infra.e2e._trino import stage_catalog_name
from openlakeforge_domain import FULL_REFRESH_TAG
from openlakeforge_domain.inventory import Domain, Product, Source

from olf_bench import _pipeline, _resources
from olf_bench._dagster import dagster_client
from olf_bench._record import BenchError, BenchRecord, _output
from olf_bench._scenarios import DEPLOY_PHASES, Session, Target, _gold_facts, _query, olf_command

TERMINAL_STATUSES = frozenset({"SUCCESS", "FAILURE", "CANCELED"})
KILL_POLL_SECONDS = 2.0


@dataclass(frozen=True)
class Entity:
    """A Silver table of the product's domain and the Bronze resource it reads."""

    namespace: str
    name: str
    incremental: bool
    primary_key: tuple[str, ...]

    @property
    def label(self) -> str:
        return f"{self.namespace}.{self.name}"


def binding_manifest(sources: Sequence[Source], environ: Mapping[str, str]) -> dict[str, Any]:
    """A Kubernetes List of every source's ConfigMap and Secret, valued from `environ`.

    A source declaring no runtime keys needs no binding. Every missing variable
    is named at once, so one failed run lists them all.
    """
    items: list[dict[str, Any]] = []
    missing: list[str] = []
    for source in sources:
        for kind, name, keys, field in (
            ("ConfigMap", source.config_map_name, source.runtime_config, "data"),
            ("Secret", source.credentials_secret_name, source.runtime_secrets, "stringData"),
        ):
            if not keys:
                continue
            values = {}
            for key in keys:
                variable = source.runtime_env_name(key)
                if not environ.get(variable):
                    missing.append(variable)
                values[key] = environ.get(variable, "")
            items.append({"apiVersion": "v1", "kind": kind, "metadata": {"name": name}, field: values})
    if missing:
        raise BenchError("set the stage's source bindings in the environment: " + ", ".join(missing))
    return {"apiVersion": "v1", "kind": "List", "items": items}


def entities(cfg: E2EConfig, domain: Domain) -> list[Entity]:
    sources = {source.name: source for source in cfg.inventory.sources}
    result = []
    for entity in cfg.inventory.floe_entities(domain):
        load = sources[entity.source].load_of(entity.resource)
        result.append(Entity(domain.silver_namespace, entity.name, load.incremental, tuple(load.primary_key)))
    return result


def check_growth(
    before: Mapping[str, int], after: Mapping[str, int], *, expected: Sequence[int], label: str
) -> dict[str, int]:
    """Each table's growth, which must be one of `expected`; an empty `expected` only forbids shrinking."""
    growth = {name: after[name] - before.get(name, 0) for name in after}
    wrong = [
        f"{name} grew by {rows}" for name, rows in growth.items() if (rows not in expected if expected else rows < 0)
    ]
    if wrong:
        allowed = " or ".join(str(rows) for rows in expected) if expected else "a non-negative count"
        raise BenchError(f"{label}: {'; '.join(wrong)}, expected {allowed}")
    return growth


def check_mirrors(silver: Mapping[str, Any], gold: Mapping[str, Any], mirrors: Mapping[str, str]) -> None:
    """Each Gold table named in `mirrors` holds exactly as many rows as the Silver table it is paired with."""
    wrong = []
    for gold_table, silver_table in mirrors.items():
        gold_rows = _rows_of(gold, gold_table)
        silver_rows = _rows_of(silver, silver_table)
        if gold_rows != silver_rows:
            wrong.append(f"{gold_table} has {gold_rows} rows, {silver_table} has {silver_rows}")
    if wrong:
        raise BenchError("Gold does not mirror Silver: " + "; ".join(wrong))


def incremental(
    record: BenchRecord,
    target: Target,
    *,
    session: Session,
    project: Path,
    product_id: str,
    runs: int,
    rows_per_run: int,
    kill_after_seconds: float | None,
    full_refresh: str | None,
    live_check: Sequence[str],
    gold_mirrors: Mapping[str, str],
    sample_interval: float,
    install: bool = True,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Install the platform from `project`'s own profile, bind its sources, then run and check `product_id`.

    Without `install`, the platform already running is used: it must have
    been applied from the same profile.
    """
    env = os.environ if environ is None else environ
    record.fact(
        "project", {"path": str(project), "git_commit": _output(["git", "-C", str(project), "rev-parse", "HEAD"])}
    )
    for phase in DEPLOY_PHASES[:-1] if install else ():
        with record.timed(f"deploy_{phase}"):
            olf_command(target, ["deploy", "--phase", phase], project_root=project, log_name=phase)
    with session(project) as cfg:
        with record.timed("bind_sources") as detail:
            manifest = binding_manifest(cfg.inventory.sources, env)
            if manifest["items"]:
                kubectl(cfg, ["apply", "-n", cfg.namespace, "-f", "-"], capture=True, input_text=json.dumps(manifest))
            detail["bound"] = [f"{item['kind']}/{item['metadata']['name']}" for item in manifest["items"]]
    with record.timed("deploy_artifacts", project=str(project)):
        olf_command(target, ["deploy", "--phase", "artifacts"], project_root=project, log_name="artifacts")
    nodes = _resources.kind_nodes(target.cluster_name)
    with _resources.Sampler(nodes, interval=sample_interval) as sampler, session(project) as cfg:
        product = _product(cfg, product_id)
        domain = cfg.inventory.domain_for_product(product)
        tables = entities(cfg, domain)
        if not any(entity.incremental for entity in tables):
            raise BenchError(f"{product_id}: no Silver entity of {domain.name} reads an incremental resource")
        probe = _Probe(cfg, tables, product, domain, gold_mirrors)
        for other in live_check:
            _live_check(record, target, cfg, _product(cfg, other))
        slice_ = [rows_per_run] if rows_per_run else []
        state = probe.empty()
        for run in range(1, runs + 1):
            _run(record, target, cfg, sampler, product, f"run_{run}")
            state = probe.check(record, f"run_{run}_tables", state, expected=slice_)
        if kill_after_seconds is not None:
            _killed_run(record, target, cfg, product, kill_after_seconds)
            state = probe.check(record, "killed_run_tables", state, expected=[0])
            _run(record, target, cfg, sampler, product, "retry")
            # A run killed after dlt committed its load leaves that slice in Bronze for the retry to merge too.
            state = probe.check(record, "retry_tables", state, expected=[r * 2 for r in slice_] + slice_)
        if full_refresh:
            _run(
                record, target, cfg, sampler, product, f"full_refresh_{full_refresh}", {FULL_REFRESH_TAG: full_refresh}
            )
            # A Bronze refresh restarts the bounded cursor at the first slice, which Silver already holds.
            expected = [0] if full_refresh == "bronze" and rows_per_run else slice_
            probe.check(record, f"full_refresh_{full_refresh}_tables", state, expected=expected)


class _Probe:
    """Reads the product's Silver and Gold tables after a run and checks them against the previous read."""

    def __init__(
        self, cfg: E2EConfig, tables: list[Entity], product: Product, domain: Domain, mirrors: Mapping[str, str]
    ) -> None:
        self.cfg = cfg
        self.tables = tables
        self.product = product
        self.mirrors = {
            f"{product.gold_namespace}.{gold}": f"{domain.silver_namespace}.{silver}"
            for gold, silver in mirrors.items()
        }

    def empty(self) -> dict[str, int]:
        return {entity.label: 0 for entity in self.tables if entity.incremental}

    def check(
        self, record: BenchRecord, name: str, before: Mapping[str, int], *, expected: Sequence[int]
    ) -> dict[str, int]:
        catalog = stage_catalog_name(self.cfg)
        query = _query(self.cfg)
        with record.timed(name) as detail:
            silver: dict[str, Any] = {}
            for entity in self.tables:
                table = _pipeline.table_facts(query, catalog, entity.namespace, entity.name)
                if entity.incremental and entity.primary_key:
                    table["distinct_keys"] = _pipeline.distinct_rows(
                        query, catalog, entity.namespace, entity.name, entity.primary_key
                    )
                    if table["distinct_keys"] != table["rows"]:
                        raise BenchError(
                            f"{entity.label}: {table['rows']} rows but {table['distinct_keys']} distinct "
                            f"{', '.join(entity.primary_key)}"
                        )
                silver[entity.label] = table
            detail["silver"] = silver
            detail["gold"] = _gold_facts(self.cfg, [self.product.gold_namespace])
            after = {entity.label: silver[entity.label]["rows"] for entity in self.tables if entity.incremental}
            detail["growth"] = check_growth(before, after, expected=expected, label=name)
            check_mirrors(silver, detail["gold"], self.mirrors)
        return after


def _run(
    record: BenchRecord,
    target: Target,
    cfg: E2EConfig,
    sampler: _resources.Sampler,
    product: Product,
    name: str,
    tags: Mapping[str, str] | None = None,
) -> None:
    with record.timed(name, product=product.id) as detail:
        with dagster_client(cfg) as client:
            _pipeline.run_job(client, product.job_name, detail, timeout_seconds=target.job_timeout_seconds, tags=tags)
        for layer in detail.get("layers", {}).values():
            if layer.get("end") is not None:
                layer["cluster_usage"] = sampler.window(layer["start"], layer["end"])


def _killed_run(record: BenchRecord, target: Target, cfg: E2EConfig, product: Product, after_seconds: float) -> None:
    """Start a run, terminate it `after_seconds` into its Bronze step, and wait for Dagster to settle it."""
    with record.timed("killed_run", product=product.id, kill_after_seconds=after_seconds) as detail:
        with dagster_client(cfg) as client:
            run_id = client.launch(product.job_name)
            detail["run_id"] = run_id
            deadline = time.monotonic() + target.job_timeout_seconds
            bronze_start = None
            while bronze_start is None:
                stats = _pipeline.run_stats(client, run_id)
                bronze = stats["layers"].get("bronze", {})
                if bronze.get("end") is not None or stats["status"] in TERMINAL_STATUSES:
                    raise BenchError(f"run {run_id} left Bronze ({stats['status']}) before it could be killed")
                bronze_start = bronze.get("start")
                if time.monotonic() >= deadline:
                    raise BenchError(f"run {run_id} did not start Bronze within {target.job_timeout_seconds}s")
                time.sleep(KILL_POLL_SECONDS)
            time.sleep(max(0.0, bronze_start + after_seconds - time.time()))
            stats = _pipeline.run_stats(client, run_id)
            if stats["layers"].get("bronze", {}).get("end") is not None:
                raise BenchError(f"Bronze of run {run_id} finished before the kill; lower --kill-after-seconds")
            killed_at = time.time()
            _pipeline.terminate(client, run_id)
            while stats["status"] not in TERMINAL_STATUSES:
                if time.monotonic() >= deadline:
                    raise BenchError(f"run {run_id} still {stats['status']} after the kill")
                time.sleep(KILL_POLL_SECONDS)
                stats = _pipeline.run_stats(client, run_id)
            detail.update(stats)
            detail["killed_into_bronze_seconds"] = round(killed_at - bronze_start, 3)
            detail["settled_seconds"] = round(time.time() - killed_at, 3)
            if stats["status"] == "SUCCESS":
                raise BenchError(f"run {run_id} succeeded although it was terminated")


def _live_check(record: BenchRecord, target: Target, cfg: E2EConfig, product: Product) -> None:
    """One run of a product whose source is a live public service: recorded, never fatal, never timed."""
    detail: dict[str, Any] = {"product": product.id}
    try:
        with dagster_client(cfg) as client:
            _pipeline.run_job(client, product.job_name, detail, timeout_seconds=target.job_timeout_seconds)
        detail["gold"] = _gold_facts(cfg, [product.gold_namespace])
        detail["outcome"] = "ok"
    except Exception as exc:  # noqa: BLE001 - a public service being down is a finding, not a failed benchmark
        detail["outcome"] = "failed"
        detail["error"] = str(exc) or type(exc).__name__
    record.fact(f"live_check_{product.id}", detail)


def _product(cfg: E2EConfig, product_id: str) -> Product:
    for product in cfg.inventory.products:
        if product.id == product_id:
            return product
    known = ", ".join(product.id for product in cfg.inventory.products)
    raise BenchError(f"the project has no product {product_id!r}; it has {known}")


def _rows_of(tables: Mapping[str, Any], name: str) -> int:
    if name not in tables:
        raise BenchError(f"{name} was not read; known tables: {', '.join(sorted(tables))}")
    return int(tables[name]["rows"])
