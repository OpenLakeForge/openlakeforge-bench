"""Scenario C: what each table costs, on a generated project of many tables.

`many-tables-project` writes the project (`_many_tables`). This installs the
platform from that project's own DEV-only Slim profile (or reuses the running
one), deploys it, runs every product job `runs` times, and records per run:

- the Dagster run and its Bronze/Silver/Gold split, with cluster CPU/RAM;
- every Floe Kubernetes Job the run started (Floe 0.8 starts one per domain):
  how long its pod waited to start, how long it ran, and, from the Floe run
  events in its log, how long each table took and how many ran at once;
- every Silver table's rows against `expected.json`, its data files and
  snapshots, and how long Trino took to read its rows and metadata;
- each Gold `mart_table_rows` against the row count of every table.
"""

from __future__ import annotations

import json
import statistics
import time
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from olf.infra.e2e._shell import E2EConfig, kubectl
from olf.infra.e2e._trino import stage_catalog_name, trino_query

from olf_bench import _pipeline, _resources
from olf_bench._dagster import dagster_client
from olf_bench._many_tables import generate
from olf_bench._record import BenchError, BenchRecord
from olf_bench._scenarios import DEPLOY_PHASES, Session, Target, _query, olf_command

# Floe's kubernetes_job runner labels every Job and pod it starts with these.
FLOE_APP_LABEL = "app=floe-runner"
FLOE_RUN_LABEL = "dagster/run-id"


def many_tables(
    record: BenchRecord,
    target: Target,
    *,
    session: Session,
    tables: int,
    domains: int,
    rows: int,
    seed: int,
    runs: int,
    sample_interval: float,
    install: bool = True,
) -> None:
    project = target.output_dir / f"project-t{tables}"
    with record.timed("generate_project", tables=tables, domains=domains, rows=rows, seed=seed) as detail:
        expected = generate(project, tables=tables, domains=domains, rows=rows, seed=seed)
        detail["files"] = sum(1 for path in project.rglob("*") if path.is_file())
    for phase in DEPLOY_PHASES[:-1] if install else ():
        with record.timed(f"deploy_{phase}"):
            olf_command(target, ["deploy", "--phase", phase], project_root=project, log_name=phase)
    with record.timed("deploy_artifacts", tables=tables):
        olf_command(target, ["deploy", "--phase", "artifacts"], project_root=project, log_name=f"artifacts-t{tables}")
    nodes = _resources.kind_nodes(target.cluster_name)
    with _resources.Sampler(nodes, interval=sample_interval) as sampler, session(project) as cfg:
        for run in range(1, runs + 1):
            for product in cfg.inventory.products:
                with record.timed(f"run_{run}_{product.id}", product=product.id) as detail:
                    try:
                        with dagster_client(cfg) as client:
                            _pipeline.run_job(
                                client, product.job_name, detail, timeout_seconds=target.job_timeout_seconds
                            )
                    finally:
                        # A failed run is where the Floe Jobs matter most: record them before the error propagates.
                        _record_run(cfg, sampler, detail)
            with record.timed(f"run_{run}_tables", tables=tables) as detail:
                detail.update(_check_tables(cfg, expected))


def _record_run(cfg: E2EConfig, sampler: _resources.Sampler, detail: dict[str, Any]) -> None:
    for layer in detail.get("layers", {}).values():
        if layer.get("end") is not None:
            layer["cluster_usage"] = sampler.window(layer["start"], layer["end"])
    if "run_id" not in detail:
        return
    try:
        pods = _floe_pods(cfg, detail["run_id"])
        detail["floe_jobs"] = floe_jobs(pods)
    except Exception as exc:  # noqa: BLE001 - never hide the run's own error behind this one
        detail["floe_jobs_error"] = str(exc) or type(exc).__name__
        return
    for job, pod in zip(detail["floe_jobs"]["jobs"], _sorted_pods(pods), strict=True):
        try:
            job["entities"] = floe_entities(_pod_log(cfg, pod["metadata"]["name"]))
        except Exception as exc:  # noqa: BLE001 - a missing log must not hide the run's result
            job["entities_error"] = str(exc) or type(exc).__name__


def floe_jobs(pods: Mapping[str, Any]) -> dict[str, Any]:
    """Per Floe Job: seconds from pod creation to container start (cold start) and container run time."""
    jobs = []
    for pod in _sorted_pods(pods):
        labels = pod.get("metadata", {}).get("labels", {})
        created = _time(pod.get("metadata", {}).get("creationTimestamp"))
        statuses: list[dict[str, Any]] = pod.get("status", {}).get("containerStatuses") or [{}]
        state = statuses[0].get("state", {})
        terminated = state.get("terminated") or {}
        started = _time(terminated.get("startedAt") or (state.get("running") or {}).get("startedAt"))
        finished = _time(terminated.get("finishedAt"))
        jobs.append(
            {
                "job": labels.get("floe-job"),
                "phase": pod.get("status", {}).get("phase"),
                "exit_code": terminated.get("exitCode"),
                "start_seconds": _span(created, started),
                "run_seconds": _span(started, finished),
            }
        )
    return {
        "count": len(jobs),
        "succeeded": sum(job["exit_code"] == 0 for job in jobs),
        "start_seconds": _stats(job["start_seconds"] for job in jobs),
        "run_seconds": _stats(job["run_seconds"] for job in jobs),
        "jobs": jobs,
    }


def floe_entities(log: str) -> dict[str, Any]:
    """Each table's seconds from `entity_started` to `entity_finished`, and the most that ran at once.

    Floe writes one JSON event per line with `--log-format json`; other lines are skipped.
    """
    started: dict[str, float] = {}
    spans: list[tuple[float, float]] = []
    run_start = run_end = None
    for line in log.splitlines():
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        kind, at = event.get("event"), event.get("ts_ms")
        if not isinstance(at, int):
            continue
        if kind == "run_started":
            run_start = at / 1000
        elif kind == "run_finished":
            run_end = at / 1000
        elif kind == "entity_started":
            started[event.get("name")] = at / 1000
        elif kind == "entity_finished" and event.get("name") in started:
            spans.append((started.pop(event["name"]), at / 1000))
    first = min((start for start, _ in spans), default=None)
    last = max((end for _, end in spans), default=None)
    return {
        "count": len(spans),
        "unfinished": len(started),
        "seconds": _stats(round(end - start, 3) for start, end in spans),
        "max_concurrent": _max_concurrent(spans),
        "before_first_seconds": _span(run_start, first),
        "entities_seconds": _span(first, last),
        "after_last_seconds": _span(last, run_end),
    }


def _max_concurrent(spans: Sequence[tuple[float, float]]) -> int:
    # An entity that finishes at the instant another starts did not overlap it: ends sort first.
    edges = sorted([(start, 1) for start, _ in spans] + [(end, -1) for _, end in spans], key=lambda e: (e[0], e[1]))
    running = peak = 0
    for _, step in edges:
        running += step
        peak = max(peak, running)
    return peak


def check_gold(rows: Sequence[tuple[str, int]], expected: Mapping[str, Any], *, product: str) -> None:
    """`mart_table_rows` must list every table of the domain once, each with the generated row count."""
    domain_tables = {
        name.split(".", 1)[1] for name, table in expected["tables"].items() if _product_of(table["source"]) == product
    }
    want = {table: expected["rows_per_table"] for table in domain_tables}
    repeated = sorted({table for table, _ in rows if sum(other == table for other, _ in rows) > 1})
    if repeated:
        raise BenchError(f"{product}_gold.mart_table_rows lists {repeated} more than once")
    rows_by_table = dict(rows)
    if rows_by_table != want:
        missing = sorted(set(want) - set(rows_by_table))
        wrong = sorted(t for t in rows_by_table if want.get(t) != rows_by_table[t])
        raise BenchError(f"{product}_gold.mart_table_rows: missing {missing or 'none'}, wrong {wrong or 'none'}")


def _check_tables(cfg: E2EConfig, expected: Mapping[str, Any]) -> dict[str, Any]:
    catalog = stage_catalog_name(cfg)
    query = _query(cfg)
    silver: dict[str, Any] = {}
    wrong = []
    for name, want in sorted(expected["tables"].items()):
        schema, table = name.split(".", 1)
        start = time.monotonic()
        facts = _pipeline.table_facts(query, catalog, schema, table)
        facts["read_seconds"] = round(time.monotonic() - start, 3)
        silver[name] = facts
        if facts["rows"] != want["rows"]:
            wrong.append(f"{name}: {facts['rows']} != {want['rows']}")
    if wrong:
        raise BenchError(f"{len(wrong)} Silver table(s) differ from the generated input: " + "; ".join(wrong[:10]))
    gold = {}
    for name in sorted(expected["gold"]):
        schema, table = name.split(".", 1)
        output = trino_query(cfg, f'SELECT table_name, row_count FROM {catalog}.{schema}."{table}"')
        pairs = [row.split(",", 1) for row in output.splitlines() if row.strip()]
        check_gold([(table, int(count)) for table, count in pairs], expected, product=schema.removesuffix("_gold"))
        gold[name] = {"rows": len(pairs)}
    return {
        "silver_tables": len(silver),
        "silver_rows": sum(facts["rows"] for facts in silver.values()),
        "data_files": sum(facts["data_files"] for facts in silver.values()),
        "snapshots": _stats(facts["snapshots"] for facts in silver.values()),
        "read_seconds": _stats(facts["read_seconds"] for facts in silver.values()),
        "silver": silver,
        "gold": gold,
    }


def _floe_pods(cfg: E2EConfig, run_id: str) -> dict[str, Any]:
    output = kubectl(
        cfg,
        ["get", "pods", "-n", cfg.namespace, "-l", f"{FLOE_APP_LABEL},{FLOE_RUN_LABEL}={run_id}", "-o", "json"],
        capture=True,
    )
    return json.loads(output)


def _pod_log(cfg: E2EConfig, pod: str) -> str:
    return kubectl(cfg, ["logs", "-n", cfg.namespace, pod], capture=True)


def _sorted_pods(pods: Mapping[str, Any]) -> list[dict[str, Any]]:
    def job(pod: Mapping[str, Any]) -> str:
        return pod.get("metadata", {}).get("labels", {}).get("floe-job") or ""

    return sorted(pods.get("items", []), key=job)


def _product_of(source: str) -> str:
    return "profile_" + source.removeprefix("feed_")


def _stats(values: Any) -> dict[str, float | int | None]:
    present = sorted(value for value in values if value is not None)
    if not present:
        return {"count": 0, "min": None, "median": None, "max": None, "total": None}
    return {
        "count": len(present),
        "min": present[0],
        "median": round(statistics.median(present), 3),
        "max": present[-1],
        "total": round(sum(present), 3),
    }


def _time(value: str | None) -> float | None:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() if value else None


def _span(start: float | None, end: float | None) -> float | None:
    return round(end - start, 3) if start is not None and end is not None else None
