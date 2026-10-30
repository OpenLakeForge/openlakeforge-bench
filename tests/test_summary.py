from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from olf_bench.cli import app

RESULTS = Path(__file__).resolve().parents[1] / "results" / "2026-10-29-cloud-vm"
runner = CliRunner()


def test_a_failed_etl_run_shows_its_layers_and_where_it_stopped() -> None:
    result = runner.invoke(app, ["summarize", str(RESULTS / "etl-x10000.json")])

    assert result.exit_code == 0, result.output
    assert "### etl-x10000: failed" in result.output
    assert "| initial_order_revenue | ok | 142.8 s | 122.7 s | 38.3 s | 26.8 s | 30.5 s |" in result.output
    assert "| repeat_inventory_reliability | failed |" in result.output
    assert "ended with FAILURE" in result.output
    assert "1,380,000 rows, 78.6 MB" in result.output


def test_a_footprint_run_shows_peak_memory_and_the_silver_free_steps(tmp_path: Path) -> None:
    result = runner.invoke(app, ["summarize", str(RESULTS / "footprint-6.json")])

    assert result.exit_code == 0, result.output
    assert "### footprint: ok" in result.output
    assert "| idle_window | ok | 300.0 s |" in result.output
    assert "GiB" in result.output


def test_table_cells_cannot_break_the_markdown_table(tmp_path: Path) -> None:
    path = tmp_path / "result.json"
    path.write_text(
        json.dumps(
            {
                "scenario": "etl-x1",
                "status": "failed",
                "measurements": [{"name": "deploy", "status": "failed", "seconds": 1.0, "error": "a | b\nsecond"}],
            }
        )
    )

    result = runner.invoke(app, ["summarize", str(path)])

    assert "| deploy | failed | 1.0 s |" in result.output
    assert "a \\| b" in result.output
    assert "second" not in result.output
