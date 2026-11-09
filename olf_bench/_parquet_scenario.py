"""Scenario E: one large Parquet file through Bronze, Silver and Gold.

`scripts/prepare_taxi_parquet.py` builds the file and its manifest; `parquet-project` (`_parquet_project`) writes the
project around it. This installs the platform from that project's own DEV-only Slim profile (or reuses the running
one), deploys it, runs the product job `runs` times, and records per run:

- the Dagster run and its Bronze/Silver/Gold split, with cluster CPU/RAM per layer;
- the Floe Kubernetes Job the run started: its start and run time, and whether it ended or was OOM-killed;
- the Silver table's rows against the file's footer, its data files and bytes, and its snapshots;
- the Gold totals against aggregates computed from the file while it was written.

A failure is a result: the layer that failed and the cause are recorded, and the command exits non-zero.
"""

from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from olf.infra.e2e._shell import E2EConfig
from olf.infra.e2e._trino import stage_catalog_name

from olf_bench import _pipeline, _resources
from olf_bench._dagster import dagster_client
from olf_bench._many_tables_scenario import _record_run
from olf_bench._parquet_project import GOLD_SCHEMA, SILVER_TABLE, generate
from olf_bench._record import BenchError, BenchRecord
from olf_bench._scenarios import DEPLOY_PHASES, Session, Target, _query, olf_command

# Trino sums doubles in parallel and the file's aggregates were summed in row-group order: the totals agree to
# many digits, not to the last bit.
SUM_TOLERANCE = 1e-9


def parquet(
    record: BenchRecord,
    target: Target,
    *,
    session: Session,
    data: Path,
    manifest: Mapping[str, Any],
    label: str,
    runs: int,
    sample_interval: float,
    install: bool = True,
) -> None:
    project = target.output_dir / f"project-{label}"
    aggregates = manifest["aggregates"]
    with record.timed(
        "generate_project",
        file_bytes=manifest["file_bytes"],
        rows=aggregates["rows"],
        row_groups=manifest["row_groups"],
        compression=manifest["compression"],
        months_read=len(manifest["months_read"]),
        distinct_months=manifest["distinct_months"],
        sha256=manifest["sha256"],
    ) as detail:
        expected = generate(project, data=data, manifest=dict(manifest))
        detail["files"] = sum(1 for path in project.rglob("*") if path.is_file())
    for phase in DEPLOY_PHASES[:-1] if install else ():
        with record.timed(f"deploy_{phase}"):
            olf_command(target, ["deploy", "--phase", phase], project_root=project, log_name=phase)
    # The image carries the file, so this phase builds it, loads it into every kind node and starts the code location.
    with record.timed("deploy_artifacts", file_bytes=manifest["file_bytes"]):
        olf_command(target, ["deploy", "--phase", "artifacts"], project_root=project, log_name=f"artifacts-{label}")
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
                        _record_run(cfg, sampler, detail)
            with record.timed(f"run_{run}_tables", rows=aggregates["rows"]) as detail:
                detail.update(_check_tables(cfg, expected))


def _check_tables(cfg: E2EConfig, expected: Mapping[str, Any]) -> dict[str, Any]:
    catalog = stage_catalog_name(cfg)
    query = _query(cfg)
    schema, table = SILVER_TABLE.split(".", 1)
    want = expected["silver"][SILVER_TABLE]["rows"]
    start = time.monotonic()
    count = _scalar(query(f'SELECT count(*) FROM {catalog}.{schema}."{table}"'))
    count_seconds = round(time.monotonic() - start, 3)
    files, file_bytes = _row(
        query(f'SELECT count(*), coalesce(sum(file_size_in_bytes), 0) FROM {catalog}.{schema}."{table}$files"'), 2
    )
    snapshots = _scalar(query(f'SELECT count(*) FROM {catalog}.{schema}."{table}$snapshots"'))
    silver = {
        "rows": int(count),
        "count_seconds": count_seconds,
        "data_files": int(files),
        "data_file_bytes": int(file_bytes),
        "snapshots": int(snapshots),
    }
    if silver["rows"] != want:
        raise BenchError(f"{SILVER_TABLE} holds {silver['rows']:,} rows, the file has {want:,}")

    totals_name = f"{GOLD_SCHEMA}.mart_trip_totals"
    by_payment_name = f"{GOLD_SCHEMA}.mart_trips_by_payment"
    totals = _row(
        query(f'SELECT trips, total_amount_sum, trip_distance_sum FROM {catalog}.{GOLD_SCHEMA}."mart_trip_totals"'), 3
    )
    by_payment = [
        line.split(",", 2)
        for line in query(
            f'SELECT payment_type, trips, total_amount_sum FROM {catalog}.{GOLD_SCHEMA}."mart_trips_by_payment" '
            "ORDER BY payment_type"
        ).splitlines()
        if line.strip()
    ]
    gold = check_gold(totals, by_payment, expected["gold"][totals_name], expected["gold"][by_payment_name])
    return {"silver": {SILVER_TABLE: silver}, "gold": gold}


def check_gold(
    totals: Sequence[str],
    by_payment: Sequence[Sequence[str]],
    want_totals: Mapping[str, Any],
    want_by_payment: Mapping[str, Any],
) -> dict[str, Any]:
    """Gold must reproduce the file's trip counts exactly and its sums to `SUM_TOLERANCE`; every gap is named."""
    problems = []
    trips, total_amount, distance = int(totals[0]), float(totals[1]), float(totals[2])
    if trips != want_totals["trips"]:
        problems.append(f"mart_trip_totals.trips {trips:,} != {want_totals['trips']:,}")
    differences = {
        "total_amount_sum": _relative(total_amount, want_totals["total_amount_sum"]),
        "trip_distance_sum": _relative(distance, want_totals["trip_distance_sum"]),
    }
    got = {key: {"trips": int(count), "total_amount_sum": float(total)} for key, count, total in by_payment}
    want = want_by_payment["by_payment_type"]
    if sorted(got) != sorted(want):
        problems.append(f"mart_trips_by_payment lists {sorted(got)}, the file has {sorted(want)}")
    for key in sorted(set(got) & set(want)):
        if got[key]["trips"] != want[key]["rows"]:
            problems.append(f"payment_type {key}: {got[key]['trips']:,} trips != {want[key]['rows']:,}")
        differences[f"payment_type_{key}_total_amount_sum"] = _relative(
            got[key]["total_amount_sum"], want[key]["total_amount_sum"]
        )
    problems += [
        f"{name} differs by {diff:.2e} (relative)" for name, diff in differences.items() if diff > SUM_TOLERANCE
    ]
    if problems:
        raise BenchError("Gold differs from the file: " + "; ".join(problems))
    return {
        "mart_trip_totals": {"trips": trips, "total_amount_sum": total_amount, "trip_distance_sum": distance},
        "mart_trips_by_payment": {"rows": len(got)},
        "worst_relative_difference": max(differences.values()),
    }


def _relative(got: float, want: float) -> float:
    return abs(got - want) / max(abs(want), 1.0) if not math.isnan(got) else math.inf


def _scalar(output: str) -> str:
    return _row(output, 1)[0]


def _row(output: str, width: int) -> list[str]:
    lines = [line for line in output.splitlines() if line.strip()]
    values = lines[-1].split(",", width - 1) if lines else []
    if len(values) != width:
        raise BenchError(f"expected one row of {width} value(s) from Trino, got {output!r}")
    return [value.strip() for value in values]
