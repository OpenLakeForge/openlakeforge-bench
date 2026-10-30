"""Scenario C's project: a deterministic synthetic lakehouse of many tables.

Scenario C of OpenLakeForge/openlakeforge#328 measures what each table costs
the platform (Floe Jobs, catalog calls, memory) independently of any external
API. This writes a complete project to `--output`: CSV sources loaded by the
platform's own CSV loader, one Floe contract per table, one Gold product and
one Dagster job per domain, and `expected.json` with the rows and SHA-256 of
every generated file. The same arguments always write byte-identical files.

    python -m olf_bench many-tables-project --tables 100 --domains 4 --rows 1000 --output .tmp/many-tables
"""

from __future__ import annotations

import hashlib
import json
import random
import shutil
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

DEFAULT_SEED = 328
# The column types the platform's reference Floe contracts use.
COLUMN_TYPES = ("integer", "number", "string", "date", "boolean")
MIN_EXTRA_COLUMNS = 3
MAX_EXTRA_COLUMNS = 10
_EPOCH = date(2026, 1, 1)
_WORDS = ("alpha", "bravo", "delta", "echo", "kilo", "lima", "oscar", "sierra", "tango", "zulu")


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass(frozen=True)
class Table:
    domain: str
    name: str
    columns: tuple[Column, ...]

    @property
    def source(self) -> str:
        return source_name(self.domain)


def domain_name(index: int) -> str:
    return f"synth_{index:02d}"


def source_name(domain: str) -> str:
    return f"feed_{domain.removeprefix('synth_')}"


def product_name(domain: str) -> str:
    return f"profile_{domain.removeprefix('synth_')}"


def plan(*, tables: int, domains: int, seed: int) -> list[Table]:
    """Spread `tables` over `domains` as evenly as possible, each table with seeded columns."""
    if domains < 1 or tables < domains:
        raise ValueError(f"need at least one table per domain: {tables} table(s), {domains} domain(s)")
    result = []
    for index in range(tables):
        domain = domain_name(index % domains + 1)
        rng = random.Random(f"{seed}:columns:{index}")
        extra = rng.randint(MIN_EXTRA_COLUMNS, MAX_EXTRA_COLUMNS)
        columns = [Column("id", "string")]
        columns += [
            Column(f"c{position:02d}_{kind}", kind)
            for position, kind in enumerate((rng.choice(COLUMN_TYPES) for _ in range(extra)), start=1)
        ]
        result.append(Table(domain, f"t{index + 1:03d}", tuple(columns)))
    return sorted(result, key=lambda table: (table.domain, table.name))


def rows_of(table: Table, *, rows: int, seed: int) -> list[list[str]]:
    rng = random.Random(f"{seed}:rows:{table.name}")
    return [[_value(table, column, row, rng) for column in table.columns] for row in range(1, rows + 1)]


def _value(table: Table, column: Column, row: int, rng: random.Random) -> str:
    if column.name == "id":
        return f"{table.name.upper()}-{row:07d}"
    if column.type == "integer":
        return str(rng.randint(0, 1_000_000))
    if column.type == "number":
        return f"{rng.uniform(0, 10_000):.2f}"
    if column.type == "date":
        return (_EPOCH + timedelta(days=rng.randint(0, 730))).isoformat()
    if column.type == "boolean":
        return rng.choice(("true", "false"))
    return f"{rng.choice(_WORDS)}-{rng.randint(0, 9_999):04d}"


def generate(output: Path, *, tables: int, domains: int, rows: int, seed: int = DEFAULT_SEED) -> dict:
    """Write the project to `output`, replacing it, and return what `expected.json` records."""
    if rows < 1:
        raise ValueError("--rows must be at least 1")
    layout = plan(tables=tables, domains=domains, seed=seed)
    if output.exists():
        shutil.rmtree(output)
    code = output / "lakehouse_code"
    by_domain: dict[str, list[Table]] = {}
    for table in layout:
        by_domain.setdefault(table.domain, []).append(table)

    _write(output / "openlakeforge.yaml", _PROFILE)
    _write(code / "governance.yml", _governance(sorted(by_domain)))
    _write(code / "definitions.py", _DEFINITIONS)
    for package in (
        "",
        "bronze",
        "silver",
        "gold",
        "pipelines",
        "pipelines/dagster",
        "dashboards",
        "dashboards/superset",
    ):
        _write(code / package / "__init__.py", "")
    _write(code / "silver" / "floe.yml", _FLOE)
    _write(code / "silver" / "sources.yml", _sources(sorted(by_domain)))

    expected: dict = {"seed": seed, "rows_per_table": rows, "domains": len(by_domain), "tables": {}}
    for domain, members in sorted(by_domain.items()):
        source = source_name(domain)
        product = product_name(domain)
        bronze = code / "bronze" / source
        _write(bronze / "source.yaml", _source(source, members))
        _write(bronze / "dlt" / "__init__.py", "")
        _write(bronze / "dlt" / f"{source}.py", _loader(source, members))
        silver = code / "silver" / domain
        _write(silver / "__init__.py", "")
        _write(silver / "contracts" / "floe" / "_domain.yml", _domain(domain))
        for table in members:
            _write(silver / "contracts" / "floe" / f"{table.name}.yml", _entity(table))
            data = _csv(table, rows=rows, seed=seed)
            _write(bronze / "examples" / f"{table.name}.csv", data)
            expected["tables"][f"{domain}_silver.{table.name}"] = {
                "source": source,
                "rows": rows,
                "columns": len(table.columns),
                "sha256": hashlib.sha256(data.encode()).hexdigest(),
            }
        gold = code / "gold" / product
        _write(gold / "__init__.py", "")
        _write(gold / "dbt" / "dbt_project.yml", _dbt_project(domain, product))
        _write(gold / "dbt" / "packages.yml", "packages:\n  - local: ../../../../libs/dbt/openlakeforge_dbt\n")
        _write(gold / "dbt" / "models" / "sources.yml", _dbt_sources(domain, members))
        _write(gold / "dbt" / "models" / "gold" / "schema.yml", _DBT_SCHEMA)
        _write(gold / "dbt" / "models" / "gold" / "mart_table_rows.sql", _mart(members))
        _write(code / "pipelines" / "dagster" / f"{product}.py", _pipeline(domain, product, members))
        expected.setdefault("gold", {})[f"{product}_gold.mart_table_rows"] = {
            "rows": len(members),
            "row_count_per_table": rows,
        }
    _write(output / "expected.json", json.dumps(expected, indent=2, sort_keys=True) + "\n")
    return expected


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8", newline="\n")


def _csv(table: Table, *, rows: int, seed: int) -> str:
    lines = [",".join(column.name for column in table.columns)]
    lines += [",".join(values) for values in rows_of(table, rows=rows, seed=seed)]
    return "\n".join(lines) + "\n"


_PROFILE = """\
apiVersion: openlakeforge.io/v1alpha1
kind: DeploymentProfile

metadata:
  name: acme-many-tables

# Generated by olf_bench many-tables-project for the many-table benchmark: one
# DEV stage, so the platform measured matches the reference installation.
spec:
  provider:
    type: local
  preset: slim
  stages:
    dev:
      enabled: true
  access:
    issuer: none
"""


def _governance(domains: list[str]) -> str:
    lines = ["apiVersion: openlakeforge.io/v1beta1", "kind: Governance", "domains:"]
    for domain in domains:
        lines += [
            f"  - name: {domain}",
            f"    displayName: Synthetic {domain.removeprefix('synth_')}",
            "    description: Generated domain for the many-table benchmark.",
            "    status: active",
        ]
    lines.append("products:")
    for domain in domains:
        lines += [
            f"  - id: {product_name(domain)}",
            f"    domain: {domain}",
            f"    displayName: Synthetic profile {domain.removeprefix('synth_')}",
            "    description: Row count of every table in the domain.",
            "    status: active",
        ]
    return "\n".join(lines) + "\n"


def _source(source: str, tables: list[Table]) -> str:
    lines = [
        "apiVersion: openlakeforge.io/v1alpha3",
        "kind: Source",
        f"name: {source}",
        f"displayName: Synthetic feed {source.removeprefix('feed_')}",
        "description: Generated CSV tables for the many-table benchmark.",
        "status: active",
        "resources:",
    ]
    for table in tables:
        lines += [f"  - name: {table.name}", "    description: Generated CSV table."]
    return "\n".join(lines) + "\n"


def _loader(source: str, tables: list[Table]) -> str:
    names = "".join(f'    "{table.name}",\n' for table in tables)
    return f'''"""Load the generated `{source}` CSV tables into Bronze with the platform's CSV loader."""

from __future__ import annotations

from pathlib import Path

from libs.bronze_csv import BronzeLoadResult, load_entities_to_bronze

ENTITIES = (
{names})

_RAW_DIR = Path(__file__).resolve().parents[1] / "examples"


def load_{source}_entities_to_bronze(
    entities: tuple[str, ...],
    raw_dir: Path | None = None,
) -> dict[str, BronzeLoadResult]:
    return load_entities_to_bronze(entities=entities, raw_dir=raw_dir or _RAW_DIR, bronze_prefix="{source}")
'''


_FLOE = """\
version: "0.8"

metadata:
  project: "acme-many-tables"
  description: "Generated Bronze to Silver Floe contracts for the many-table benchmark."

report:
  path: "floe/reports"
  storage: "openlakeforge_ops"

include:
  sources: "sources.yml"
  domains:
    - "*/contracts/floe/_domain.yml"
"""


def _sources(domains: list[str]) -> str:
    lines = ["sources:"]
    for domain in domains:
        source = source_name(domain)
        lines += [
            f'  - name: "{source}"',
            '    format: "csv"',
            f'    path: "{source}/{{{{resource}}}}"',
            '    storage: "lakehouse_bronze"',
            '    options: { header: true, separator: ",", encoding: "utf-8", glob: "*.csv" }',
            '    cast_mode: "strict"',
        ]
    return "\n".join(lines) + "\n"


def _domain(domain: str) -> str:
    return f'''name: "{domain}"
defaults:
  incremental_mode: "none"
  sink:
    write_mode: "overwrite"
    accepted:
      format: "iceberg"
      path: "{domain}/{{{{entity.name}}}}"
      storage: "lakehouse_silver"
      iceberg: {{ catalog: "iceberg_catalog", namespace: "{domain}_silver" }}
    rejected: {{ format: "csv", path: "floe/rejected/{domain}/{{{{entity.name}}}}", storage: "lakehouse_silver" }}
  policy: {{ severity: "reject" }}
  schema:
    normalize_columns: {{ enabled: true, strategy: "snake_case" }}
'''


def _entity(table: Table) -> str:
    lines = [
        f'name: "{table.name}"',
        f'source: {{ ref: "{table.source}", resource: "{table.name}" }}',
        "schema:",
        '  primary_key: ["id"]',
        "  columns:",
    ]
    lines += [f'    - {{ name: "{column.name}", type: "{column.type}", nullable: false }}' for column in table.columns]
    return "\n".join(lines) + "\n"


def _dbt_project(domain: str, product: str) -> str:
    return f"""name: {product}
version: "1.0.0"
config-version: 2

profile: {product}

model-paths: ["models"]
macro-paths: []
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  {product}:
    +database: "{{{{ env_var('OPENLAKEFORGE_CATALOG_NAME') }}}}"
    +materialized: table
    +on_table_exists: replace
    +meta:
      dagster:
        group: {product}
    gold:
      +tags: ["{domain}", "{product}", "gold"]
"""


def _dbt_sources(domain: str, tables: list[Table]) -> str:
    lines = [
        "version: 2",
        "",
        "sources:",
        "  - name: silver",
        "    database: \"{{ env_var('OPENLAKEFORGE_CATALOG_NAME') }}\"",
        f"    schema: \"{{{{ env_var('OPENLAKEFORGE_CATALOG_SCHEMA_PREFIX', '') }}}}{domain}_silver\"",
        "    description: Generated Silver tables.",
        "    tables:",
    ]
    lines += [f"      - name: {table.name}" for table in tables]
    return "\n".join(lines) + "\n"


_DBT_SCHEMA = """\
version: 2

models:
  - name: mart_table_rows
    description: Row count of every Silver table in the domain, one row per table.
"""


def _mart(tables: list[Table]) -> str:
    selects = [
        f"select '{table.name}' as table_name, count(*) as row_count from {{{{ source('silver', '{table.name}') }}}}"
        for table in tables
    ]
    return (
        "-- Reads every table of the domain once, so Gold depends on all of them.\n"
        + "\nunion all\n".join(selects)
        + "\n"
    )


def _pipeline(domain: str, product: str, tables: list[Table]) -> str:
    names = "".join(f'    "{table.name}",\n' for table in tables)
    # `olf check project-code` reads the inputs from `<PRODUCT>_SILVER_INPUTS`.
    inputs = f"{product.upper()}_SILVER_INPUTS"
    return f'''from __future__ import annotations

from libs.product_dagster import ProductDefinitionSpec, build_product_definitions

{inputs} = (
{names})

{product.upper()}_GOLD_ASSETS = ("mart_table_rows",)

defs = build_product_definitions(
    ProductDefinitionSpec(
        domain="{domain}",
        product="{product}",
        silver_inputs={inputs},
        bronze_inputs=tuple(("{source_name(domain)}", table) for table in {inputs}),
        gold_assets={product.upper()}_GOLD_ASSETS,
    )
)
'''


_DEFINITIONS = '''"""Aggregate Dagster definitions for the canonical lakehouse code location."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules

from dagster import Definitions
from openlakeforge_domain import LakehouseInventory, load_lakehouse_inventory

import lakehouse_code.pipelines.dagster as dagster_pipelines
from libs.product_dagster import SourceDefinitionSpec, build_silver_definitions, build_source_definitions

_LAKEHOUSE_CODE_ROOT = Path(__file__).resolve().parent


def _product_definitions() -> list[Definitions]:
    """Load Definitions exposed by product pipeline modules in a stable order."""
    product_modules = sorted(
        (
            module
            for module in iter_modules(dagster_pipelines.__path__, "lakehouse_code.pipelines.dagster.")
            if not module.ispkg
        ),
        key=lambda module: module.name,
    )
    return [import_module(module.name).defs for module in product_modules]


def _source_definitions(inventory: LakehouseInventory) -> list[Definitions]:
    definitions: list[Definitions] = []
    for source in inventory.sources:
        module = import_module(f"lakehouse_code.bronze.{source.name}.dlt.{source.name}")
        loader = getattr(module, f"load_{source.name}_entities_to_bronze")
        definitions.append(
            build_source_definitions(
                SourceDefinitionSpec(
                    source=source.name,
                    resources=tuple(resource.name for resource in source.resources),
                    bronze_loader=loader,
                )
            )
        )
    return definitions


_inventory = load_lakehouse_inventory(_LAKEHOUSE_CODE_ROOT)
defs = Definitions.merge(*_source_definitions(_inventory), build_silver_definitions(), *_product_definitions())
'''
