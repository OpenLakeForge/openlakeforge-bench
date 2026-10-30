from __future__ import annotations

import json
from pathlib import Path

import pytest
from openlakeforge_domain.inventory import Source, Table
from typer.testing import CliRunner

import olf_bench as bench
from olf_bench import BenchError
from olf_bench._incremental import binding_manifest, check_growth, check_mirrors
from olf_bench._pipeline import distinct_rows
from olf_bench.cli import app

RNACENTRAL = Source(
    descriptor_path=Path("bronze/rnacentral/source.yaml"),
    name="rnacentral",
    display_name="RNAcentral",
    resources=(Table("sequences"),),
    runtime_config=("host", "port"),
    runtime_secrets=("password",),
)
CSV = Source(descriptor_path=Path("bronze/crm/source.yaml"), name="crm", display_name="CRM", resources=())


def test_binding_manifest_values_every_declared_key_from_the_loader_variable() -> None:
    environ = {
        "SOURCES__RNACENTRAL__HOST": "db.example",
        "SOURCES__RNACENTRAL__PORT": "5432",
        "SOURCES__RNACENTRAL__PASSWORD": "s3cret",
    }

    manifest = binding_manifest([RNACENTRAL, CSV], environ)

    assert manifest["kind"] == "List"
    assert manifest["items"] == [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "olf-source-rnacentral"},
            "data": {"host": "db.example", "port": "5432"},
        },
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "olf-source-rnacentral-credentials"},
            "stringData": {"password": "s3cret"},
        },
    ]


def test_binding_manifest_names_every_missing_variable_and_no_value() -> None:
    with pytest.raises(BenchError) as error:
        binding_manifest([RNACENTRAL], {"SOURCES__RNACENTRAL__PORT": "5432"})

    assert "SOURCES__RNACENTRAL__HOST" in str(error.value)
    assert "SOURCES__RNACENTRAL__PASSWORD" in str(error.value)
    assert "5432" not in str(error.value)


def test_check_growth_accepts_only_the_expected_slices() -> None:
    before = {"rna_silver.sequences": 100}

    assert check_growth(before, {"rna_silver.sequences": 300}, expected=[200, 100], label="retry") == {
        "rna_silver.sequences": 200
    }
    with pytest.raises(BenchError, match="grew by 150, expected 100"):
        check_growth(before, {"rna_silver.sequences": 250}, expected=[100], label="run_2")


def test_check_growth_without_a_bound_only_forbids_shrinking() -> None:
    assert check_growth({"t": 5}, {"t": 9}, expected=[], label="run") == {"t": 4}
    with pytest.raises(BenchError, match="non-negative"):
        check_growth({"t": 5}, {"t": 4}, expected=[], label="run")


def test_check_mirrors_compares_gold_rows_with_their_silver_table() -> None:
    silver = {"rna_silver.sequences": {"rows": 10}}
    check_mirrors(silver, {"g.mart": {"rows": 10}}, {"g.mart": "rna_silver.sequences"})

    with pytest.raises(BenchError, match="g.mart has 9 rows, rna_silver.sequences has 10"):
        check_mirrors(silver, {"g.mart": {"rows": 9}}, {"g.mart": "rna_silver.sequences"})
    with pytest.raises(BenchError, match="g.other was not read"):
        check_mirrors(silver, {"g.mart": {"rows": 10}}, {"g.other": "rna_silver.sequences"})


def test_distinct_rows_counts_distinct_key_tuples() -> None:
    seen: list[str] = []

    def query(sql: str) -> str:
        seen.append(sql)
        return "42\n"

    assert distinct_rows(query, "lakehouse_dev", "rna_silver", "sequences", ["urs", 'a"b']) == 42
    assert seen == ['SELECT count(*) FROM (SELECT DISTINCT "urs", "a""b" FROM lakehouse_dev.rna_silver."sequences")']


runner = CliRunner()


def _invoke(project: Path, tmp_path: Path, *args: str):  # noqa: ANN202
    return runner.invoke(
        app,
        [
            "incremental",
            "--project",
            str(tmp_path / "acme"),
            "--product",
            "rna_catalog",
            *args,
            "--project-root",
            str(project),
            "--output",
            str(tmp_path / "out"),
        ],
    )


def test_incremental_passes_its_options_through(
    external_project: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict = {}

    def incremental(record, target, *, session, **options):  # noqa: ANN001, ANN202
        seen.update(options)
        with record.timed("run_1"):
            pass

    monkeypatch.setattr(bench, "incremental", incremental)

    result = _invoke(
        external_project,
        tmp_path,
        "--runs",
        "3",
        "--kill-after-seconds",
        "-1",
        "--full-refresh",
        "none",
        "--live-check",
        "food_quality",
        "--gold-mirror",
        "mart_sequence_index=sequences",
    )

    assert result.exit_code == 0, result.output
    assert seen["project"] == (tmp_path / "acme").resolve()
    assert seen["product_id"] == "rna_catalog"
    assert seen["runs"] == 3
    assert seen["rows_per_run"] == 100_000
    assert seen["kill_after_seconds"] is None
    assert seen["full_refresh"] is None
    assert seen["live_check"] == ["food_quality"]
    assert seen["gold_mirrors"] == {"mart_sequence_index": "sequences"}
    assert seen["install"] is True
    document = json.loads((tmp_path / "out" / "result.json").read_text())
    assert document["scenario"] == "incremental-rna_catalog"
    assert document["environment"]["parameters"]["runs"] == 3


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--full-refresh", "silver"], "--full-refresh must be one of bronze, gold or none"),
        (["--gold-mirror", "mart_sequence_index"], "--gold-mirror takes GOLD=SILVER"),
    ],
)
def test_incremental_rejects_malformed_options(
    external_project: Path, tmp_path: Path, args: list[str], message: str
) -> None:
    result = _invoke(external_project, tmp_path, *args)

    assert result.exit_code != 0
    assert message in result.output
    assert not (tmp_path / "out" / "result.json").exists()
