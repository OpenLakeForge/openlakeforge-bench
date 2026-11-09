"""The Silver step breakdown, derived from recorded Kubernetes Job and pod JSON.

`fixtures/k8s` is shaped like `kubectl get jobs|pods -l app=floe-runner,dagster/run-id=<id> -o json`
for one run of two domains: `floe-sales` ran once, `floe-supply-chain` failed once and was retried,
and `floe-gone` has no pod left.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from olf_bench import _silver_breakdown as breakdown
from olf_bench._summary import summarize

FIXTURES = Path(__file__).parent / "fixtures" / "k8s"
T0 = 1794132000.0  # 2026-11-08T10:00:00Z, the fixtures' clock


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _step(key: str, start: float, end: float | None) -> dict[str, Any]:
    return {"step": key, "layer": "silver", "start": T0 + start, "end": None if end is None else T0 + end}


STEPS = [
    {"step": "feed_01_bronze_resources", "layer": "bronze", "start": T0 - 5, "end": T0 - 1},
    _step("floe_proj__sales__default_ab12", 0.4, 20.8),
    _step("floe_proj__supply_chain__default_cd34", 1.2, 30.5),
    {"step": "profile_01_dbt_gold_assets", "layer": "gold", "start": T0 + 31, "end": T0 + 40},
]


def _by_job(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {job["job"]: job for job in result["jobs"]}


def test_a_single_pod_job_is_split_into_the_steps_segments() -> None:
    result = breakdown.silver_breakdown(STEPS, _load("floe-jobs.json"), _load("floe-pods.json"))

    sales = _by_job(result)["floe-sales"]

    assert sales["step"] == "floe_proj__sales__default_ab12"
    assert sales["segments"] == {
        "step_to_job_created": 2.6,  # 10:00:00.4 -> 10:00:03
        "job_to_pod_scheduled": 0.0,
        "scheduled_to_container_started": 6.0,
        "container_run": 5.0,
        "container_finished_to_job_complete": 1.0,
        "job_complete_to_step_end": 5.8,  # 10:00:15 -> 10:00:20.8
    }
    # The segments tile the step exactly.
    assert sum(sales["segments"].values()) == pytest.approx(
        sales["timestamps"]["step_end"] - sales["timestamps"]["step_start"]
    )
    assert sales["timestamps"]["pod_created"] == T0 + 3
    assert result["resolution_seconds"] == 1


def test_a_retried_job_is_measured_on_its_last_pod() -> None:
    supply = _by_job(breakdown.silver_breakdown(STEPS, _load("floe-jobs.json"), _load("floe-pods.json")))[
        "floe-supply-chain"
    ]

    assert supply["pods"] == 2
    assert supply["pod_name"] == "floe-supply-chain-bbbb-r2"
    # Created 10:00:04, rescheduled 10:00:13, started 10:00:19, finished 10:00:26, Job complete 10:00:27.
    assert supply["segments"]["job_to_pod_scheduled"] == 9.0
    assert supply["segments"]["scheduled_to_container_started"] == 6.0
    assert supply["segments"]["container_run"] == 7.0
    assert supply["segments"]["job_complete_to_step_end"] == 3.5


def test_concurrent_steps_are_told_apart_by_the_domain_in_the_step_key() -> None:
    result = breakdown.silver_breakdown(STEPS, _load("floe-jobs.json"), _load("floe-pods.json"))

    # Both steps were running when each Job was created; the names decide, so neither is ambiguous.
    assert [job["step_ambiguous"] for job in result["jobs"] if job["job"] != "floe-gone"] == [False, False]
    assert _by_job(result)["floe-supply-chain"]["step"] == "floe_proj__supply_chain__default_cd34"


def test_concurrent_steps_that_the_names_cannot_separate_are_flagged() -> None:
    steps = [_step("floe_a", 0.0, 20.0), _step("floe_b", 1.0, 30.0)]

    result = breakdown.silver_breakdown(steps, _load("floe-jobs.json"), _load("floe-pods.json"))

    assert _by_job(result)["floe-sales"]["step"] == "floe_b"  # the later start
    assert _by_job(result)["floe-sales"]["step_ambiguous"] is True


def test_a_job_whose_pod_and_completion_are_gone_records_null_not_an_error() -> None:
    gone = _by_job(breakdown.silver_breakdown(STEPS, _load("floe-jobs.json"), _load("floe-pods.json")))["floe-gone"]

    assert gone["pods"] == 0
    assert gone["pod_name"] is None
    assert gone["timestamps"]["job_created"] == T0 + 5
    assert gone["timestamps"]["pod_scheduled"] is None
    assert gone["segments"]["step_to_job_created"] is not None
    assert gone["segments"]["container_run"] is None
    assert gone["segments"]["job_complete_to_step_end"] is None


def test_the_run_level_stats_leave_out_segments_a_job_could_not_measure() -> None:
    result = breakdown.silver_breakdown(STEPS, _load("floe-jobs.json"), _load("floe-pods.json"))

    assert result["segments"]["container_run"] == {"count": 2, "min": 5.0, "median": 6.0, "max": 7.0, "total": 12.0}
    assert result["segments"]["step_to_job_created"]["count"] == 3


def test_a_run_with_no_jobs_or_no_silver_step_still_derives() -> None:
    empty = breakdown.silver_breakdown([], {"items": []}, {"items": []})
    assert empty["jobs"] == []
    assert empty["segments"]["container_run"]["count"] == 0

    stepless = breakdown.silver_breakdown([], _load("floe-jobs.json"), _load("floe-pods.json"))
    assert _by_job(stepless)["floe-sales"]["step"] is None
    assert _by_job(stepless)["floe-sales"]["segments"]["step_to_job_created"] is None
    assert _by_job(stepless)["floe-sales"]["segments"]["container_run"] == 5.0


def test_recording_stores_the_breakdown_or_the_error(monkeypatch: pytest.MonkeyPatch) -> None:
    listings = {"jobs": _load("floe-jobs.json"), "pods": _load("floe-pods.json")}
    monkeypatch.setattr(breakdown, "_list", lambda cfg, kind, run_id: listings[kind])
    detail: dict[str, Any] = {"run_id": "r1", "steps": STEPS}

    breakdown.record_silver_breakdown(None, detail)  # type: ignore[arg-type]

    assert len(detail["silver_breakdown"]["jobs"]) == 3
    assert "silver_breakdown_error" not in detail

    def broken(cfg: Any, kind: str, run_id: str) -> dict[str, Any]:
        raise RuntimeError("kubectl timed out")

    monkeypatch.setattr(breakdown, "_list", broken)
    failed: dict[str, Any] = {"run_id": "r1", "steps": STEPS}
    breakdown.record_silver_breakdown(None, failed)  # type: ignore[arg-type]
    assert failed == {"run_id": "r1", "steps": STEPS, "silver_breakdown_error": "kubectl timed out"}

    never_launched: dict[str, Any] = {}
    breakdown.record_silver_breakdown(None, never_launched)  # type: ignore[arg-type]
    assert never_launched == {}


def test_the_summary_prints_the_breakdown_in_whole_seconds(tmp_path: Path) -> None:
    detail = {"silver_breakdown": breakdown.silver_breakdown(STEPS, _load("floe-jobs.json"), _load("floe-pods.json"))}
    document = {
        "scenario": "etl-x100",
        "status": "ok",
        "measurements": [{"name": "run_1_p", "status": "ok", "detail": detail}],
    }
    path = tmp_path / "result.json"
    path.write_text(json.dumps(document), encoding="utf-8")

    text = summarize([path])

    assert "1 s resolution" in text
    assert "| run_1_p | 3 | 3 s (4 s) | 4 s (9 s) | 6 s (6 s) | 6 s (7 s) | 1 s (1 s) | 5 s (6 s) |" in text
