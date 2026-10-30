from __future__ import annotations

import csv
import hashlib
import json
import subprocess
from pathlib import Path

import pytest
from openlakeforge_domain import load_lakehouse_inventory

from olf_bench._many_tables import generate, plan


def _files(root: Path) -> dict[str, bytes]:
    return {str(path.relative_to(root)): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


def test_the_same_arguments_write_byte_identical_projects(tmp_path: Path) -> None:
    generate(tmp_path / "a", tables=12, domains=3, rows=20, seed=7)
    generate(tmp_path / "b", tables=12, domains=3, rows=20, seed=7)

    assert _files(tmp_path / "a") == _files(tmp_path / "b")


def test_another_seed_changes_the_data(tmp_path: Path) -> None:
    generate(tmp_path / "a", tables=4, domains=1, rows=20, seed=7)
    generate(tmp_path / "b", tables=4, domains=1, rows=20, seed=8)

    assert _files(tmp_path / "a") != _files(tmp_path / "b")


def test_tables_spread_evenly_over_the_domains() -> None:
    tables = plan(tables=10, domains=4, seed=1)

    per_domain = {domain: sum(table.domain == domain for table in tables) for domain in {t.domain for t in tables}}
    assert sorted(per_domain.values()) == [2, 2, 3, 3]
    assert len({table.name for table in tables}) == 10


def test_expected_json_matches_every_generated_file(tmp_path: Path) -> None:
    expected = generate(tmp_path, tables=6, domains=2, rows=50, seed=3)

    assert expected == json.loads((tmp_path / "expected.json").read_text())
    assert len(expected["tables"]) == 6
    for name, table in expected["tables"].items():
        namespace, entity = name.split(".")
        path = tmp_path / "lakehouse_code" / "bronze" / table["source"] / "examples" / f"{entity}.csv"
        assert hashlib.sha256(path.read_bytes()).hexdigest() == table["sha256"]
        with path.open(newline="") as handle:
            rows = list(csv.reader(handle))
        assert len(rows) == 1 + table["rows"]
        assert len(rows[0]) == table["columns"]
        # The primary key is unique, so Silver keeps every row.
        assert len({row[0] for row in rows[1:]}) == table["rows"]
        assert namespace.endswith("_silver")
    assert sum(gold["rows"] for gold in expected["gold"].values()) == 6


def test_the_inventory_sees_every_table_as_a_silver_entity(tmp_path: Path) -> None:
    generate(tmp_path, tables=9, domains=3, rows=5, seed=1)

    inventory = load_lakehouse_inventory(tmp_path / "lakehouse_code")

    assert [product.job_name for product in inventory.products] == [
        "profile_01_pipeline",
        "profile_02_pipeline",
        "profile_03_pipeline",
    ]
    assert sum(len(inventory.floe_entities(domain)) for domain in inventory.domains) == 9


def test_too_few_tables_for_the_domains_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least one table per domain"):
        generate(tmp_path, tables=2, domains=3, rows=5)


@pytest.mark.parametrize(
    "command",
    [
        ["olf", "project", "validate", "--project"],
        ["olf", "check", "contracts", "--repo-root"],
    ],
)
def test_the_platform_accepts_the_generated_project(tmp_path: Path, command: list[str]) -> None:
    generate(tmp_path, tables=8, domains=2, rows=5, seed=1)

    result = subprocess.run([*command, str(tmp_path)], capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + result.stderr


def test_the_cli_writes_the_project_and_refuses_more_domains_than_tables(tmp_path: Path) -> None:
    from typer.testing import CliRunner

    from olf_bench.cli import app

    runner = CliRunner()
    done = runner.invoke(
        app, ["many-tables-project", "--output", str(tmp_path / "p"), "--tables", "4", "--domains", "2"]
    )
    refused = runner.invoke(
        app, ["many-tables-project", "--output", str(tmp_path / "q"), "--tables", "1", "--domains", "2"]
    )

    assert done.exit_code == 0, done.output
    assert len(json.loads((tmp_path / "p" / "expected.json").read_text())["tables"]) == 4
    assert refused.exit_code != 0
    assert not (tmp_path / "q").exists()
