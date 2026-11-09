"""The metadata PostgreSQL counters, derived from recorded `psql` and cgroup v2 output.

`fixtures/postgres` holds what `kubectl exec` printed: `psql -A -t -F '|'` rows of `_postgres_stats.QUERY`
(WAL, database, then three settings) and `cpu.stat` followed by `cpu.pressure` and `io.pressure`. The run
lasted 10 s between the readings.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from olf_bench import _postgres_stats as stats
from olf_bench.cli import app

FIXTURES = Path(__file__).parent / "fixtures" / "postgres"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _snapshot(at: float, query: str, cgroup: str = "cgroup-before.txt") -> dict[str, Any]:
    return {"at": at, **stats.parse_query(_read(query)), "cgroup": stats.parse_cgroup(_read(cgroup))}


def test_wal_timing_off_leaves_the_times_null_and_the_fsync_mean_underived() -> None:
    result = stats.delta(
        _snapshot(100.0, "psql-before-untimed.txt"),
        _snapshot(110.0, "psql-after-untimed.txt", "cgroup-after.txt"),
    )

    assert result["track_wal_io_timing"] is False
    assert result["wal"] == {
        "wal_records": 2440,
        "wal_bytes": 372000,
        "wal_write": 1600,
        "wal_sync": 1400,
        "wal_write_time_ms": None,
        "wal_sync_time_ms": None,
    }
    assert result["database"]["xact_commit"] == 720
    assert result["derived"]["commits_per_second"] == 72.0
    assert result["derived"]["wal_syncs_per_second"] == 140.0
    assert result["derived"]["mean_wal_sync_ms"] is None
    assert result["synchronous_commit"] == "on"
    assert result["error"] is None


def test_wal_timing_on_gives_the_mean_fsync_latency() -> None:
    result = stats.delta(
        _snapshot(100.0, "psql-before-timed.txt"),
        _snapshot(110.0, "psql-after-timed.txt", "cgroup-after.txt"),
    )

    assert result["track_wal_io_timing"] is True
    assert result["wal"]["wal_sync_time_ms"] == 1100.0
    assert result["derived"]["mean_wal_sync_ms"] == pytest.approx(1100 / 1400, abs=1e-3)
    assert result["database"]["blk_write_time_ms"] == 4.0
    assert result["synchronous_commit"] == "off"


def test_the_cgroup_delta_separates_cpu_use_from_cpu_and_io_waiting() -> None:
    result = stats.delta(
        _snapshot(100.0, "psql-before-untimed.txt"),
        _snapshot(110.0, "psql-after-untimed.txt", "cgroup-after.txt"),
    )

    assert result["cgroup"] == {
        "usage_usec": 12_000_000,
        "nr_throttled": 0,
        "throttled_usec": 0,
        "cpu_pressure_some_usec": 6_000_000,
        "io_pressure_some_usec": 7_000_000,
    }
    assert result["derived"]["cpu_cores_used"] == 1.2
    assert result["derived"]["cpu_pressure_share"] == 0.6
    assert result["derived"]["io_pressure_share"] == 0.7
    assert result["derived"]["throttled_share"] == 0.0


def test_a_failed_reading_is_an_error_string_not_an_exception() -> None:
    failed = {"at": 110.0, "error": 'pods "postgresql-0" not found', "cgroup_error": "no such file"}

    result = stats.delta(_snapshot(100.0, "psql-before-untimed.txt"), failed)

    assert result["wal"] is None
    assert result["cgroup"] is None
    assert result["derived"]["commits_per_second"] is None
    assert "not found" in result["error"]
    assert "no such file" in result["error"]


def test_counters_that_restarted_mid_run_are_not_reported_as_negative() -> None:
    result = stats.delta(_snapshot(100.0, "psql-after-untimed.txt"), _snapshot(110.0, "psql-before-untimed.txt"))

    assert result["wal"] is None
    assert "restarted" in result["error"]


def test_unexpected_psql_output_is_rejected() -> None:
    with pytest.raises(ValueError, match="unexpected psql output"):
        stats.parse_query("psql: error: connection to server failed\n")


def test_the_summary_shows_the_postgres_row(tmp_path: Path) -> None:
    detail = {
        "postgres": stats.delta(
            _snapshot(100.0, "psql-before-timed.txt"), _snapshot(110.0, "psql-after-timed.txt", "cgroup-after.txt")
        )
    }
    path = tmp_path / "result.json"
    path.write_text(
        json.dumps(
            {
                "scenario": "many-tables-t100",
                "status": "ok",
                "measurements": [{"name": "run_1_profile_01", "status": "ok", "seconds": 90.0, "detail": detail}],
            }
        )
    )

    output = CliRunner().invoke(app, ["summarize", str(path)]).output

    row = next(line for line in output.splitlines() if line.startswith("| run_1_profile_01 | 72 "))
    assert row == "| run_1_profile_01 | 72 | 140 | 0.786 | 1.2 | 60.0% | 70.0% | 0.0% | off |  |"
