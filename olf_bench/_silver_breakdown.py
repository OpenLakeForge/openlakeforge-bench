"""Where a Silver step's time goes around its Floe Kubernetes Job.

The Dagster Silver step (`floe_dagster/kubernetes_runner.py`) creates one Job
per domain, polls it every 2 s, reads the pod's logs and, afterwards, each
entity's run report from S3 before it emits its events. Dagster times the whole
step; Kubernetes times the Job. This reads the Job's and its pod's timestamps
from the API and splits the step into:

- `step_to_job_created`: step start to the Job being created (Dagster-side setup);
- `job_to_pod_scheduled`: Job created to the pod being placed on a node;
- `scheduled_to_container_started`: image pull and container start;
- `container_run`: the Floe process itself;
- `container_finished_to_job_complete`: container exit to the Job controller marking it complete;
- `job_complete_to_step_end`: Job complete to step end (the poll interval, log and report
  reading, and writing the Dagster events).

Kubernetes timestamps have a resolution of one second (they are truncated, not
rounded), while Dagster's step times are exact, so every segment carries up to
about 1 s of error and the first can read slightly negative. Read the segments
as whole seconds. A Job is matched to its step by time (the Silver step that
was running when the Job was created); `step_ambiguous` marks a Job that more
than one Silver step could own.

Everything here is best effort: a Job or pod that is gone, or a field it does
not carry, is recorded as null and never fails the benchmark.
"""

from __future__ import annotations

import json
import re
import statistics
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime
from typing import Any

from olf.infra.e2e._shell import E2EConfig, kubectl

# Floe's kubernetes_job runner labels every Job and pod it starts with these.
FLOE_APP_LABEL = "app=floe-runner"
FLOE_RUN_LABEL = "dagster/run-id"

RESOLUTION_SECONDS = 1
SEGMENTS = (
    "step_to_job_created",
    "job_to_pod_scheduled",
    "scheduled_to_container_started",
    "container_run",
    "container_finished_to_job_complete",
    "job_complete_to_step_end",
)
# Each segment is the gap between two of these points, in order.
_POINTS = ("step_start", "job_created", "pod_scheduled", "container_started", "container_finished", "job_completed")


def record_silver_breakdown(cfg: E2EConfig, detail: dict[str, Any]) -> None:
    """Add `silver_breakdown` to a run's `detail`, or `silver_breakdown_error`; never raises."""
    if "run_id" not in detail:
        return
    try:
        jobs = _list(cfg, "jobs", detail["run_id"])
        pods = _list(cfg, "pods", detail["run_id"])
        detail["silver_breakdown"] = silver_breakdown(detail.get("steps") or [], jobs, pods)
    except Exception as exc:  # noqa: BLE001 - never hide the run's own error behind this one
        detail["silver_breakdown_error"] = str(exc) or type(exc).__name__


def silver_breakdown(
    steps: Sequence[Mapping[str, Any]], jobs: Mapping[str, Any], pods: Mapping[str, Any]
) -> dict[str, Any]:
    """Per Floe Job: its Kubernetes timestamps and the step split derived from them.

    `steps` are the run's `summarize_run` steps; `jobs` and `pods` are `kubectl get ... -o json` lists.
    """
    silver = [step for step in steps if step.get("layer") == "silver" and step.get("start") is not None]
    pods_by_job = _pods_by_job(pods)
    rows = [_job_row(job, pods_by_job.get(job["metadata"]["name"], []), silver) for job in _items(jobs)]
    rows.sort(key=lambda row: row["job"] or "")
    return {
        "resolution_seconds": RESOLUTION_SECONDS,
        "jobs": rows,
        "segments": {name: _stats(row["segments"][name] for row in rows) for name in SEGMENTS},
    }


def _job_row(
    job: Mapping[str, Any], pods: Sequence[Mapping[str, Any]], silver: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    metadata = job.get("metadata") or {}
    created = _time(metadata.get("creationTimestamp"))
    # With retries the last pod is the one whose outcome the Job reports.
    pod = max(pods, key=lambda p: (p.get("metadata") or {}).get("creationTimestamp") or "", default=None)
    pod_metadata = (pod or {}).get("metadata") or {}
    status = (pod or {}).get("status") or {}
    container = (status.get("containerStatuses") or [{}])[0]
    state = container.get("state") or {}
    terminated = state.get("terminated") or {}
    scheduled: Mapping[str, Any] = next(
        (c for c in status.get("conditions") or [] if c.get("type") == "PodScheduled" and c.get("status") == "True"), {}
    )
    step, ambiguous = _owning_step(silver, created, metadata.get("name"))
    times: dict[str, float | None] = {
        "step_start": step.get("start") if step else None,
        "job_created": created,
        "pod_scheduled": _time(scheduled.get("lastTransitionTime")),
        "container_started": _time(terminated.get("startedAt") or (state.get("running") or {}).get("startedAt")),
        "container_finished": _time(terminated.get("finishedAt")),
        "job_completed": _time((job.get("status") or {}).get("completionTime")),
    }
    gaps = list(zip(_POINTS, _POINTS[1:], strict=False)) + [("job_completed", "step_end")]
    times["step_end"] = step.get("end") if step else None
    segments = dict(zip(SEGMENTS, (_span(times[a], times[b]) for a, b in gaps), strict=True))
    return {
        # The Job carries no `floe-job` label (only its pod template does); its name is `floe-<entity>-<uuid8>`.
        "job": metadata.get("name"),
        "pod_name": pod_metadata.get("name"),
        "pods": len(pods),
        "step": step.get("step") if step else None,
        "step_ambiguous": ambiguous,
        "timestamps": {
            "job_created": created,
            "pod_created": _time(pod_metadata.get("creationTimestamp")),
            "pod_scheduled": times["pod_scheduled"],
            "container_started": times["container_started"],
            "container_finished": times["container_finished"],
            "job_completed": times["job_completed"],
            "step_start": times["step_start"],
            "step_end": times["step_end"],
        },
        "segments": segments,
    }


def _owning_step(
    silver: Sequence[Mapping[str, Any]], created: float | None, job_name: str | None
) -> tuple[Mapping[str, Any] | None, bool]:
    """The Silver step running when the Job was created; truncation can put the Job up to 1 s before it."""
    if created is None or not silver:
        return None, False
    # Kubernetes truncates to the second: a Job created at 10.9 s is stamped 10.
    running = [
        step
        for step in silver
        if step["start"] - RESOLUTION_SECONDS < created and (step.get("end") is None or created <= step["end"])
    ]
    if len(running) > 1 and job_name:
        # Concurrent steps: a step key names its domain (`floe_<project>__<domain>__...`), as the Job's name does.
        domain = re.sub(r"-[0-9a-f]{8}$", "", job_name.removeprefix("floe-")).replace("-", "_")
        named = [step for step in running if domain in str(step.get("step", "")).replace("-", "_")]
        if len(named) == 1:
            return named[0], False
    if not running:
        return None, False
    # Otherwise the step that started last before the Job is the likeliest owner.
    return max(running, key=lambda step: step["start"]), len(running) > 1


def _pods_by_job(pods: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for pod in _items(pods):
        metadata = pod.get("metadata") or {}
        labels = metadata.get("labels") or {}
        owner = next((o.get("name") for o in metadata.get("ownerReferences") or [] if o.get("kind") == "Job"), None)
        name = owner or labels.get("batch.kubernetes.io/job-name") or labels.get("job-name")
        if name:
            grouped.setdefault(name, []).append(pod)
    return grouped


def _items(listing: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [item for item in listing.get("items") or [] if (item.get("metadata") or {}).get("name")]


def _list(cfg: E2EConfig, kind: str, run_id: str) -> dict[str, Any]:
    output = kubectl(
        cfg,
        ["get", kind, "-n", cfg.namespace, "-l", f"{FLOE_APP_LABEL},{FLOE_RUN_LABEL}={run_id}", "-o", "json"],
        capture=True,
    )
    return json.loads(output)


def _stats(values: Iterable[float | None]) -> dict[str, float | int | None]:
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
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _span(start: float | None, end: float | None) -> float | None:
    return round(end - start, 3) if start is not None and end is not None else None
