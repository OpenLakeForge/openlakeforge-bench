from __future__ import annotations

import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

import olf_bench as bench
from olf_bench import BenchError
from olf_bench._many_tables import generate
from olf_bench._many_tables_scenario import check_gold, floe_entities, floe_jobs
from olf_bench.cli import app


def _pod(job: str, created: str, started: str | None, finished: str | None, exit_code: int | None) -> dict:
    state: dict = {}
    if finished:
        state["terminated"] = {"startedAt": started, "finishedAt": finished, "exitCode": exit_code}
    elif started:
        state["running"] = {"startedAt": started}
    return {
        "metadata": {"name": f"{job}-pod", "labels": {"floe-job": job}, "creationTimestamp": created},
        "status": {"phase": "Succeeded" if exit_code == 0 else "Running", "containerStatuses": [{"state": state}]},
    }


def test_floe_jobs_times_each_pod_from_creation_to_start_and_from_start_to_finish() -> None:
    pods = {
        "items": [
            _pod("floe-synth-02-b", "2026-10-30T10:00:00Z", "2026-10-30T10:00:12Z", "2026-10-30T10:01:12Z", 0),
            _pod("floe-synth-01-a", "2026-10-30T10:00:00Z", "2026-10-30T10:00:04Z", "2026-10-30T10:00:34Z", 0),
            _pod("floe-synth-03-c", "2026-10-30T10:00:00Z", "2026-10-30T10:00:08Z", None, None),
        ]
    }

    summary = floe_jobs(pods)

    assert summary["count"] == 3
    assert summary["succeeded"] == 2
    assert [job["job"] for job in summary["jobs"]] == ["floe-synth-01-a", "floe-synth-02-b", "floe-synth-03-c"]
    assert summary["start_seconds"] == {"count": 3, "min": 4.0, "median": 8.0, "max": 12.0, "total": 24.0}
    # A pod still running has no run time yet, and is left out of the run-time figures.
    assert summary["run_seconds"] == {"count": 2, "min": 30.0, "median": 45.0, "max": 60.0, "total": 90.0}


def _event(kind: str, ms: int, name: str | None = None) -> str:
    return json.dumps({"event": kind, "run_id": "r", "ts_ms": ms, **({"name": name} if name else {})}) + "\n"


def test_floe_entities_times_each_table_and_counts_how_many_ran_at_once() -> None:
    log = (
        "starting floe\n"
        + _event("run_started", 1_000)
        + _event("entity_started", 3_000, "a")
        + _event("entity_started", 3_000, "b")
        + _event("entity_finished", 5_000, "a")
        + _event("entity_started", 5_000, "c")  # starts as a finishes: two at once, not three
        + _event("entity_finished", 9_000, "b")
        + _event("entity_finished", 10_000, "c")
        + "{not json\n"
        + _event("run_finished", 10_500)
    )

    summary = floe_entities(log)

    assert summary["count"] == 3
    assert summary["unfinished"] == 0
    assert summary["seconds"] == {"count": 3, "min": 2.0, "median": 5.0, "max": 6.0, "total": 13.0}
    assert summary["max_concurrent"] == 2
    assert summary["before_first_seconds"] == 2.0
    assert summary["entities_seconds"] == 7.0
    assert summary["after_last_seconds"] == 0.5


def test_floe_entities_one_after_another_never_overlap() -> None:
    log = "".join(_event(kind, ms, name) for kind, ms, name in [
        ("entity_started", 0, "a"), ("entity_finished", 4_000, "a"),
        ("entity_started", 4_000, "b"), ("entity_finished", 9_000, "b"),
    ])  # fmt: skip

    assert floe_entities(log)["max_concurrent"] == 1


def test_floe_jobs_without_pods_reports_nothing() -> None:
    assert floe_jobs({"items": []})["start_seconds"]["count"] == 0


def test_check_gold_wants_every_table_of_the_domain_with_its_row_count(tmp_path: Path) -> None:
    expected = generate(tmp_path, tables=4, domains=2, rows=7, seed=1)
    tables = sorted(name.split(".")[1] for name in expected["tables"] if name.startswith("synth_01_silver."))

    check_gold([(table, 7) for table in tables], expected, product="profile_01")
    with pytest.raises(BenchError, match=f"missing \\['{tables[1]}'\\]"):
        check_gold([(tables[0], 7)], expected, product="profile_01")
    with pytest.raises(BenchError, match=f"wrong \\['{tables[0]}'\\]"):
        check_gold([(tables[0], 6), (tables[1], 7)], expected, product="profile_01")
    # A duplicate cannot hide a wrong count behind a right one.
    with pytest.raises(BenchError, match=f"lists \\['{tables[0]}'\\] more than once"):
        check_gold([(tables[0], 6), (tables[0], 7), (tables[1], 7)], expected, product="profile_01")


def test_the_cli_passes_its_options_and_names_the_scenario_by_table_count(
    external_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict = {}

    def many_tables(record, target, *, session, **options):  # noqa: ANN001, ANN202
        seen.update(options)
        with record.timed("run_1_profile_01"):
            pass

    monkeypatch.setattr(bench, "many_tables", many_tables)

    result = CliRunner().invoke(
        app,
        [
            "many-tables",
            "--tables",
            "300",
            "--domains",
            "6",
            "--runs",
            "1",
            "--existing",
            "--project-root",
            str(external_project),
            "--output",
            str(tmp_path / "out"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert seen == {
        "tables": 300,
        "domains": 6,
        "rows": 1000,
        "seed": 328,
        "runs": 1,
        "sample_interval": 2.0,
        "install": False,
    }
    document = json.loads((tmp_path / "out" / "result.json").read_text())
    assert document["scenario"] == "many-tables-t300"


def test_a_failed_run_still_records_its_floe_jobs(monkeypatch: pytest.MonkeyPatch) -> None:
    from olf_bench import _many_tables_scenario as scenario

    pod = _pod("floe-synth-01-a", "2026-10-30T10:00:00Z", "2026-10-30T10:00:04Z", "2026-10-30T10:00:09Z", 1)
    monkeypatch.setattr(scenario, "_floe_pods", lambda cfg, run_id: {"items": [pod]})
    logs: list[str] = []
    monkeypatch.setattr(scenario, "_pod_log", lambda cfg, name: logs.append(name) or _event("entity_started", 0, "a"))
    detail: dict = {"run_id": "r1", "status": "FAILURE"}

    scenario._record_run(None, None, detail)  # type: ignore[arg-type]

    assert detail["floe_jobs"]["count"] == 1
    assert detail["floe_jobs"]["succeeded"] == 0
    assert logs == ["floe-synth-01-a-pod"]
    assert detail["floe_jobs"]["jobs"][0]["entities"]["unfinished"] == 1


def test_an_unreadable_floe_log_is_recorded_on_its_job(monkeypatch: pytest.MonkeyPatch) -> None:
    from olf_bench import _many_tables_scenario as scenario

    pod = _pod("floe-synth-01-a", "2026-10-30T10:00:00Z", "2026-10-30T10:00:04Z", "2026-10-30T10:00:09Z", 0)
    monkeypatch.setattr(scenario, "_floe_pods", lambda cfg, run_id: {"items": [pod]})

    def broken(cfg, name):  # noqa: ANN001, ANN202
        raise RuntimeError("pod gone")

    monkeypatch.setattr(scenario, "_pod_log", broken)
    detail: dict = {"run_id": "r1"}

    scenario._record_run(None, None, detail)  # type: ignore[arg-type]

    assert detail["floe_jobs"]["jobs"][0]["entities_error"] == "pod gone"


def test_an_unreadable_floe_job_list_is_recorded_not_raised(monkeypatch: pytest.MonkeyPatch) -> None:
    from olf_bench import _many_tables_scenario as scenario

    def broken(cfg, run_id):  # noqa: ANN001, ANN202
        raise RuntimeError("kubectl timed out")

    monkeypatch.setattr(scenario, "_floe_pods", broken)
    detail: dict = {"run_id": "r1"}

    scenario._record_run(None, None, detail)  # type: ignore[arg-type]

    assert detail["floe_jobs_error"] == "kubectl timed out"
