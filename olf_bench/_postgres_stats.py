"""What the metadata PostgreSQL did during a product run.

A Silver step spends most of the time after its Floe Job on Dagster writing events, one transaction per event,
so the Dagster database's commit latency decides it. A slow commit has two different causes: the WAL fsync is
slow (storage), or the server has no CPU to run the transaction (the pod requests 100m and shares its node).
This reads the counters that tell them apart, once before and once after each run, and records the deltas:

- `pg_stat_wal`: WAL records, bytes, writes and fsyncs, and the time spent in the last two. The times are only
  counted with `track_wal_io_timing`, which this does not switch on (turning it on changes the server being
  measured); when it is off they are null and the mean fsync time is not derived;
- `pg_stat_database` of the stage's Dagster database: commits, and block read/write times (null without
  `track_io_timing`);
- the container's cgroup v2 `cpu.stat`, plus the `some` totals of `cpu.pressure` and `io.pressure`. The pod has
  a CPU request but no limit, so `throttled_usec` stays 0 on it: CPU starvation shows up as pressure (time
  runnable tasks waited for a CPU), not as throttling. Both are recorded so a limit added later still reads.

`pg_stat_wal` has the write and fsync columns up to PostgreSQL 17 (the pinned image is 16); on 18 they moved to
`pg_stat_io` and the query fails, which is recorded as an error, not raised.

Everything here is best effort: a missing pod, tool or permission is recorded as an error string and never fails
the benchmark.
"""

from __future__ import annotations

import time
from typing import Any

from olf.infra.contracts import CONTRACT_STAGE_ENV
from olf.infra.e2e._shell import E2EConfig, cfg_environ, kubectl

# modules/storage/postgresql: a StatefulSet named after its `release_name` (default `postgresql`), one container,
# in the shared namespace.
POSTGRES_WORKLOAD = "statefulset/postgresql"
POSTGRES_CONTAINER = "postgresql"
CGROUP_FILES = ("cpu.stat", "cpu.pressure", "io.pressure")

QUERY = """
SELECT w.wal_records, w.wal_bytes, w.wal_write, w.wal_sync, w.wal_write_time, w.wal_sync_time,
       d.xact_commit, d.blk_read_time, d.blk_write_time,
       current_setting('track_wal_io_timing'), current_setting('track_io_timing'),
       current_setting('synchronous_commit')
FROM pg_stat_wal w, pg_stat_database d
WHERE d.datname = current_database()
"""

# Counters in the order the query returns them: the WAL columns, then the database's.
_COUNTERS = ("wal_records", "wal_bytes", "wal_write", "wal_sync", "wal_write_time_ms", "wal_sync_time_ms")
_DATABASE_COUNTERS = ("xact_commit", "blk_read_time_ms", "blk_write_time_ms")
_CGROUP_COUNTERS = ("usage_usec", "nr_throttled", "throttled_usec", "cpu_pressure_some_usec", "io_pressure_some_usec")


def snapshot(cfg: E2EConfig) -> dict[str, Any]:
    """The counters now, or `{"error": ...}`; never raises."""
    taken: dict[str, Any] = {"at": time.time()}
    try:
        psql = ["psql", "-X", "-A", "-t", "-F", "|", "-U", "postgres", "-d", _database(cfg), "-c", QUERY]
        taken.update(parse_query(_exec(cfg, psql)))
    except Exception as exc:  # noqa: BLE001 - never hide the run's own error behind this one
        taken["error"] = str(exc) or type(exc).__name__
    try:
        taken["cgroup"] = parse_cgroup(_exec(cfg, ["sh", "-c", "cd /sys/fs/cgroup && cat " + " ".join(CGROUP_FILES)]))
    except Exception as exc:  # noqa: BLE001
        taken["cgroup_error"] = str(exc) or type(exc).__name__
    return taken


def record_postgres(cfg: E2EConfig, detail: dict[str, Any], before: dict[str, Any]) -> None:
    """Add `postgres` (deltas over the run) to a run's `detail`; never raises."""
    try:
        detail["postgres"] = delta(before, snapshot(cfg))
    except Exception as exc:  # noqa: BLE001
        detail["postgres"] = {"error": str(exc) or type(exc).__name__}


def delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    seconds = after["at"] - before["at"]
    errors = [str(snap[key]) for snap in (before, after) for key in ("error", "cgroup_error") if key in snap]
    database = _delta(before.get("counters"), after.get("counters"), _COUNTERS + _DATABASE_COUNTERS, errors)
    cgroup = _delta(before.get("cgroup"), after.get("cgroup"), _CGROUP_COUNTERS, errors)
    wal_timed = bool(after.get("track_wal_io_timing")) and bool(before.get("track_wal_io_timing"))
    io_timed = bool(after.get("track_io_timing")) and bool(before.get("track_io_timing"))
    if database:
        if not wal_timed:
            database["wal_write_time_ms"] = database["wal_sync_time_ms"] = None
        if not io_timed:
            database["blk_read_time_ms"] = database["blk_write_time_ms"] = None
    syncs = database.get("wal_sync")
    sync_ms = database.get("wal_sync_time_ms")
    commits = database.get("xact_commit")
    return {
        "seconds": round(seconds, 3),
        "track_wal_io_timing": wal_timed if database else None,
        "track_io_timing": io_timed if database else None,
        "synchronous_commit": after.get("synchronous_commit"),
        "wal": {key: database.get(key) for key in _COUNTERS} if database else None,
        "database": {key: database.get(key) for key in _DATABASE_COUNTERS} if database else None,
        "cgroup": cgroup or None,
        "derived": {
            "commits_per_second": _rate(commits, seconds),
            "wal_syncs_per_second": _rate(syncs, seconds),
            "mean_wal_sync_ms": round(sync_ms / syncs, 3) if sync_ms is not None and syncs else None,
            "cpu_cores_used": _share(cgroup.get("usage_usec"), seconds),
            "throttled_share": _share(cgroup.get("throttled_usec"), seconds),
            "cpu_pressure_share": _share(cgroup.get("cpu_pressure_some_usec"), seconds),
            "io_pressure_share": _share(cgroup.get("io_pressure_some_usec"), seconds),
        },
        "error": "; ".join(dict.fromkeys(errors)) or None,
    }


def parse_query(output: str) -> dict[str, Any]:
    """The one row `QUERY` prints with `psql -A -t -F '|'`."""
    row = output.strip().splitlines()[-1].split("|")
    if len(row) != 12:
        raise ValueError(f"unexpected psql output: {output.strip()[:200]!r}")
    numbers = [int(value) if value.isdigit() else float(value) for value in row[:9]]
    return {
        "counters": dict(zip(_COUNTERS + _DATABASE_COUNTERS, numbers, strict=True)),
        "track_wal_io_timing": row[9] == "on",
        "track_io_timing": row[10] == "on",
        "synchronous_commit": row[11],
    }


def parse_cgroup(output: str) -> dict[str, int]:
    """`cpu.stat` lines are `<key> <value>`; a pressure file has `some avg10=.. total=<usec>` and a `full` line."""
    counters: dict[str, int] = {}
    pressure = iter(("cpu_pressure_some_usec", "io_pressure_some_usec"))
    for line in output.splitlines():
        words = line.split()
        if len(words) == 2 and words[0] in ("usage_usec", "nr_throttled", "throttled_usec"):
            counters[words[0]] = int(words[1])
        elif words and words[0] == "some":
            counters[next(pressure)] = int(dict(word.split("=") for word in words[1:])["total"])
    return counters


def _delta(
    before: dict[str, Any] | None, after: dict[str, Any] | None, keys: tuple[str, ...], errors: list[str]
) -> dict[str, Any]:
    if not before or not after:
        return {}
    result = {key: _diff(after[key], before[key]) for key in keys if key in before and key in after}
    if any(value is not None and value < 0 for value in result.values()):
        # The pod restarted between the readings, so the counters started over.
        errors.append("counters went backwards (PostgreSQL restarted during the run)")
        return {}
    return result


def _diff(after: float, before: float) -> float:
    return round(after - before, 3)


def _rate(count: float | None, seconds: float) -> float | None:
    return round(count / seconds, 3) if count is not None and seconds > 0 else None


def _share(usec: float | None, seconds: float) -> float | None:
    return round(usec / 1e6 / seconds, 4) if usec is not None and seconds > 0 else None


def _database(cfg: E2EConfig) -> str:
    return f"dagster_{cfg_environ(cfg).get(CONTRACT_STAGE_ENV, 'dev')}"


def _exec(cfg: E2EConfig, command: list[str]) -> str:
    return kubectl(
        cfg,
        ["exec", "-n", cfg.platform_namespace, POSTGRES_WORKLOAD, "-c", POSTGRES_CONTAINER, "--", *command],
        capture=True,
    )
