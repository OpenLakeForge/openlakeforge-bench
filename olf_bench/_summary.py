"""A Markdown digest of `result.json` files, for a CI job summary or an issue comment."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

LAYERS = ("bronze", "silver", "gold")
_GIB = 1024**3


def summarize(paths: Iterable[Path]) -> str:
    return "\n".join(_one(path, json.loads(path.read_text(encoding="utf-8"))) for path in paths)


def _one(path: Path, document: Mapping[str, Any]) -> str:
    host = document.get("environment", {}).get("host", {})
    params = document.get("environment", {}).get("parameters", {})
    lines = [
        f"### {document.get('scenario', path.parent.name)}: {document.get('status', 'unknown')}",
        "",
        f"`{path}` · commit `{str(document.get('environment', {}).get('git_commit') or '?')[:7]}`"
        f" · {host.get('cpus', '?')} CPUs · {_gib(host.get('memory_bytes'))} · cgroup {host.get('cgroup', '?')}"
        f" · preset {params.get('preset', '?')}",
        "",
        "| Step | Status | Wall | Run | Bronze | Silver | Gold | Peak memory | Note |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for item in document.get("measurements", []):
        detail = item.get("detail", {})
        layers = detail.get("layers", {})
        lines.append(
            "| "
            + " | ".join(
                [
                    item.get("name", ""),
                    item.get("status", ""),
                    _seconds(item.get("seconds")),
                    _seconds(detail.get("seconds")) if layers else "",
                    *(_seconds(layers.get(layer, {}).get("seconds")) if layers else "" for layer in LAYERS),
                    _gib(_peak_memory(detail)),
                    _note(item, detail),
                ]
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def _peak_memory(detail: Mapping[str, Any]) -> int | None:
    usages = [detail.get("cluster_usage", {})]
    usages += [layer.get("cluster_usage", {}) for layer in detail.get("layers", {}).values()]
    peaks = [usage["memory_bytes_max"] for usage in usages if usage.get("memory_bytes_max")]
    return max(peaks) if peaks else None


def _note(item: Mapping[str, Any], detail: Mapping[str, Any]) -> str:
    if item.get("error"):
        return _cell(str(item["error"]).strip().splitlines()[0])
    floe = detail.get("floe_jobs")
    if isinstance(floe, Mapping) and floe.get("count"):
        start = floe["start_seconds"]
        return (
            f"{floe['succeeded']}/{floe['count']} Floe Jobs, pod start {_seconds(start['median'])} median"
            f" / {_seconds(start['max'])} max"
        )
    if "silver_tables" in detail:
        reads = detail["read_seconds"]
        return (
            f"{detail['silver_tables']} tables, {detail['silver_rows']:,} rows, {detail['data_files']} files,"
            f" read {_seconds(reads['median'])} median / {_seconds(reads['max'])} max"
        )
    if "rows" in detail and "bytes" in detail:
        return f"{detail['rows']:,} rows, {detail['bytes'] / 1e6:.1f} MB"
    silver = detail.get("silver")
    if isinstance(silver, Mapping) and silver:
        rows = sum(table.get("rows", 0) for table in silver.values())
        return f"Silver {rows:,} rows"
    return ""


def _cell(text: str, limit: int = 140) -> str:
    text = text.replace("|", "\\|")
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _seconds(value: float | None) -> str:
    return "" if value is None else f"{value:.1f} s"


def _gib(value: int | None) -> str:
    return "" if not value else f"{value / _GIB:.2f} GiB"
