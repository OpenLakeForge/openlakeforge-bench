"""Scenario E: the project written around a Parquet file, and the Gold check against the file's aggregates."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from olf_bench import BenchError
from olf_bench._parquet_project import GOLD_SCHEMA, SILVER_TABLE, generate
from olf_bench._parquet_scenario import check_gold

MANIFEST: dict[str, Any] = {
    "source": "NYC TLC yellow taxi trip records",
    "file": "trips.parquet",
    "file_bytes": 4,
    "sha256": "0" * 64,
    "row_groups": 2,
    "compression": "snappy",
    "columns": {
        "vendor_id": "int64",
        "pickup_at": "timestamp[us]",
        "passenger_count": "int64",
        "trip_distance": "double",
        "payment_type": "int64",
        "total_amount": "double",
    },
    "aggregates": {
        "rows": 10,
        "total_amount_sum": 123.5,
        "trip_distance_sum": 40.25,
        "by_payment_type": {
            "1": {"rows": 7, "total_amount_sum": 100.0},
            "2": {"rows": 3, "total_amount_sum": 23.5},
        },
    },
}


def _write_project(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    data = tmp_path / "trips.parquet"
    data.write_bytes(b"PAR1")
    project = tmp_path / "project"
    generate(project, data=data, manifest=MANIFEST)
    return project


def test_the_file_ships_in_the_bronze_examples_and_expected_json_names_what_gold_must_hold(tmp_path: Path) -> None:
    project = _write_project(tmp_path)

    shipped = project / "lakehouse_code/bronze/taxi/examples/trips.parquet"
    expected = (project / "expected.json").read_text(encoding="utf-8")

    assert shipped.read_bytes() == b"PAR1"
    assert SILVER_TABLE in expected
    assert f"{GOLD_SCHEMA}.mart_trip_totals" in expected


def test_every_manifest_column_is_a_nullable_contract_column(tmp_path: Path) -> None:
    contract = (_write_project(tmp_path) / "lakehouse_code/silver/mobility/contracts/floe/trips.yml").read_text()

    assert 'name: "pickup_at", type: "datetime", nullable: true' in contract
    assert contract.count("nullable: true") == len(MANIFEST["columns"])


def test_an_unknown_parquet_type_stops_the_generator(tmp_path: Path) -> None:
    data = tmp_path / "trips.parquet"
    data.write_bytes(b"PAR1")

    with pytest.raises(ValueError, match="decimal"):
        generate(tmp_path / "p", data=data, manifest={**MANIFEST, "columns": {"amount": "decimal128(10, 2)"}})


def test_generation_is_deterministic(tmp_path: Path) -> None:
    first, second = _write_project(tmp_path / "a"), _write_project(tmp_path / "b")

    files = sorted(path.relative_to(first) for path in first.rglob("*") if path.is_file())

    assert files == sorted(path.relative_to(second) for path in second.rglob("*") if path.is_file())
    assert all((first / path).read_bytes() == (second / path).read_bytes() for path in files)


@pytest.mark.parametrize(
    "command",
    [
        ["olf", "project", "validate", "--project"],
        ["olf", "check", "contracts", "--repo-root"],
    ],
)
def test_the_platform_accepts_the_generated_project(tmp_path: Path, command: list[str]) -> None:
    project = _write_project(tmp_path)

    result = subprocess.run([*command, str(project)], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + result.stderr


WANT_TOTALS = {"trips": 10, "total_amount_sum": 123.5, "trip_distance_sum": 40.25}
WANT_BY_PAYMENT = {"by_payment_type": MANIFEST["aggregates"]["by_payment_type"]}


def _check(totals: list[str], by_payment: list[list[str]]) -> dict[str, Any]:
    return check_gold(totals, by_payment, WANT_TOTALS, WANT_BY_PAYMENT)


def test_gold_that_reproduces_the_file_passes_with_float_noise() -> None:
    result = _check(["10", "1.235000000001E2", "40.25"], [["1", "7", "100.0"], ["2", "3", "23.5"]])

    assert result["mart_trips_by_payment"] == {"rows": 2}
    assert result["worst_relative_difference"] < 1e-9


@pytest.mark.parametrize(
    ("totals", "by_payment", "message"),
    [
        (["9", "123.5", "40.25"], [["1", "7", "100.0"], ["2", "3", "23.5"]], "trips 9 != 10"),
        (["10", "124.5", "40.25"], [["1", "7", "100.0"], ["2", "3", "23.5"]], "total_amount_sum differs"),
        (["10", "123.5", "40.25"], [["1", "6", "100.0"], ["2", "3", "23.5"]], "payment_type 1: 6 trips != 7"),
        (["10", "123.5", "40.25"], [["1", "7", "100.0"]], r"lists \['1'\]"),
    ],
)
def test_gold_that_differs_from_the_file_names_the_gap(
    totals: list[str], by_payment: list[list[str]], message: str
) -> None:
    with pytest.raises(BenchError, match=message):
        _check(totals, by_payment)
