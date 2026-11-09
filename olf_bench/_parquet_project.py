"""Scenario E's project: one large Parquet file through Bronze, Silver and Gold.

Scenario E of OpenLakeForge/openlakeforge-bench#22 measures what a single multi-gigabyte Parquet file costs each
layer. `scripts/prepare_taxi_parquet.py` builds the file from NYC TLC yellow-taxi records and writes a manifest of
its row count and aggregates. This writes the project around it: one Parquet source, one Floe contract, one Gold
product, and the file shipped in the project-code image the way scenario B ships its CSV fixtures.

    python -m olf_bench parquet-project --data .tmp/taxi/trips.parquet --output .tmp/parquet-project
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from olf_bench._many_tables import _DEFINITIONS, _FLOE, _PROFILE, _write

DOMAIN = "mobility"
SOURCE = "taxi"
ENTITY = "trips"
PRODUCT = "trip_summary"
SILVER_TABLE = f"{DOMAIN}_silver.{ENTITY}"
GOLD_SCHEMA = f"{PRODUCT}_gold"

# manifest column type -> Floe contract type
_CONTRACT_TYPES = {"int64": "integer", "double": "number", "timestamp[us]": "datetime", "string": "string"}


def generate(output: Path, *, data: Path, manifest: dict) -> dict:
    """Write the project to `output`, replacing it, and return what `expected.json` records."""
    if not data.is_file():
        raise ValueError(f"{data} is not a file")
    columns = manifest["columns"]
    unknown = sorted({kind for kind in columns.values() if kind not in _CONTRACT_TYPES})
    if unknown:
        raise ValueError(f"no contract type for Parquet type(s): {', '.join(unknown)}")
    if output.exists():
        shutil.rmtree(output)
    code = output / "lakehouse_code"

    _write(output / "openlakeforge.yaml", _PROFILE.replace("acme-many-tables", "acme-large-parquet"))
    _write(code / "governance.yml", _GOVERNANCE)
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
    _write(
        code / "silver" / "floe.yml",
        _FLOE.replace("acme-many-tables", "acme-large-parquet").replace("many-table", "large-Parquet"),
    )
    _write(code / "silver" / "sources.yml", _SOURCES)

    bronze = code / "bronze" / SOURCE
    _write(bronze / "source.yaml", _SOURCE_DESCRIPTOR)
    _write(bronze / "dlt" / "__init__.py", "")
    _write(bronze / "dlt" / f"{SOURCE}.py", _LOADER)
    examples = bronze / "examples"
    examples.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(data, examples / f"{ENTITY}.parquet")

    silver = code / "silver" / DOMAIN
    _write(silver / "__init__.py", "")
    _write(silver / "contracts" / "floe" / "_domain.yml", _domain())
    _write(silver / "contracts" / "floe" / f"{ENTITY}.yml", _entity(columns))

    gold = code / "gold" / PRODUCT
    _write(gold / "__init__.py", "")
    _write(gold / "dbt" / "dbt_project.yml", _DBT_PROJECT)
    _write(
        gold / "dbt" / "packages.yml",
        "packages:\n  - local: \"{{ env_var('OPENLAKEFORGE_DBT_PACKAGES_DIR') }}/openlakeforge_dbt\"\n",
    )
    _write(gold / "dbt" / "models" / "sources.yml", _DBT_SOURCES)
    _write(gold / "dbt" / "models" / "gold" / "schema.yml", _DBT_SCHEMA)
    _write(gold / "dbt" / "models" / "gold" / "mart_trip_totals.sql", _MART_TOTALS)
    _write(gold / "dbt" / "models" / "gold" / "mart_trips_by_payment.sql", _MART_BY_PAYMENT)
    _write(code / "pipelines" / "dagster" / f"{PRODUCT}.py", _PIPELINE)

    aggregates = manifest["aggregates"]
    expected = {
        "source": manifest["source"],
        "file": manifest["file"],
        "file_bytes": manifest["file_bytes"],
        "sha256": manifest["sha256"],
        "row_groups": manifest["row_groups"],
        "compression": manifest["compression"],
        "silver": {SILVER_TABLE: {"rows": aggregates["rows"]}},
        "gold": {
            f"{GOLD_SCHEMA}.mart_trip_totals": {
                "rows": 1,
                "trips": aggregates["rows"],
                "total_amount_sum": aggregates["total_amount_sum"],
                "trip_distance_sum": aggregates["trip_distance_sum"],
            },
            f"{GOLD_SCHEMA}.mart_trips_by_payment": {
                "rows": len(aggregates["by_payment_type"]),
                "by_payment_type": aggregates["by_payment_type"],
            },
        },
    }
    _write(output / "expected.json", json.dumps(expected, indent=2, sort_keys=True) + "\n")
    return expected


_GOVERNANCE = f"""\
apiVersion: openlakeforge.io/v1beta2
kind: Governance
domains:
  - name: {DOMAIN}
    displayName: Mobility
    description: Generated domain for the large-Parquet benchmark.
    status: active
    products:
      - id: {PRODUCT}
        displayName: Trip summary
        description: Trip count and totals of the Silver trips table, overall and per payment type.
        status: active
"""

_SOURCES = f"""\
sources:
  - name: "{SOURCE}"
    format: "parquet"
    path: "{SOURCE}/{{{{resource}}}}"
    storage: "lakehouse_bronze"
    options: {{ glob: "*.parquet" }}
"""

_SOURCE_DESCRIPTOR = f"""\
apiVersion: openlakeforge.io/v1beta1
kind: Source
name: {SOURCE}
displayName: NYC taxi trips
description: One large Parquet file of NYC TLC yellow-taxi trip records for the large-Parquet benchmark.
status: active
resources:
  - name: {ENTITY}
    description: Taxi trips.
"""

_LOADER = f'''\
"""Bronze loader for the large-Parquet benchmark: upload the file as it is.

Parquet needs no parsing to land: the object store takes the file whole, and the row count is in its footer. The
upload streams in multipart chunks, so memory stays flat whatever the file size.
"""

from __future__ import annotations

import os
from pathlib import Path

import boto3
import pyarrow.parquet as pq
from botocore.client import Config
from openlakeforge_runtime.bronze_csv import BronzeLoadResult

ENTITIES = ("{ENTITY}",)

_RAW_DIR = Path(__file__).resolve().parents[1] / "examples"


def load_{SOURCE}_entities_to_bronze(
    entities: tuple[str, ...],
    raw_dir: Path | None = None,
) -> dict[str, BronzeLoadResult]:
    raw_dir = raw_dir or _RAW_DIR
    bucket = os.environ["OPENLAKEFORGE_STORAGE_BRONZE_BUCKET"]
    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3"),
        region_name=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "us-east-1",
        config=Config(s3={{"addressing_style": "path"}}),
    )
    results = {{}}
    for entity in entities:
        if entity not in ENTITIES:
            raise ValueError(f"unsupported entity for Bronze load: {{entity}}")
        source_file = raw_dir / f"{{entity}}.parquet"
        rows = pq.ParquetFile(source_file).metadata.num_rows
        key = f"{SOURCE}/{{entity}}/{{entity}}.parquet"
        client.upload_file(str(source_file), bucket, key)
        results[entity] = BronzeLoadResult(
            entity=entity, rows=rows, uri=f"s3://{{bucket}}/{{key}}", source_file=str(source_file)
        )
    return results
'''


def _domain() -> str:
    return f'''name: "{DOMAIN}"
defaults:
  incremental_mode: "none"
  sink:
    write_mode: "overwrite"
    accepted:
      format: "iceberg"
      path: "{DOMAIN}/{{{{entity.name}}}}"
      storage: "lakehouse_silver"
      iceberg: {{ catalog: "iceberg_catalog", namespace: "{DOMAIN}_silver" }}
    rejected: {{ format: "csv", path: "floe/rejected/{DOMAIN}/{{{{entity.name}}}}", storage: "lakehouse_silver" }}
  policy: {{ severity: "reject" }}
  schema:
    normalize_columns: {{ enabled: true, strategy: "snake_case" }}
'''


def _entity(columns: dict[str, str]) -> str:
    lines = [
        f'name: "{ENTITY}"',
        f'source: {{ ref: "{SOURCE}", resource: "{ENTITY}" }}',
        "schema:",
        "  columns:",
    ]
    # Every column is nullable: the TLC data has nulls and out-of-range values, and a rejected row would make
    # Silver hold fewer rows than the file, which the benchmark must report as a finding, not hide.
    lines += [
        f'    - {{ name: "{name}", type: "{_CONTRACT_TYPES[kind]}", nullable: true }}' for name, kind in columns.items()
    ]
    return "\n".join(lines) + "\n"


_DBT_PROJECT = f"""\
name: {PRODUCT}
version: "1.0.0"
config-version: 2

profile: {PRODUCT}

model-paths: ["models"]
macro-paths: []
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  {PRODUCT}:
    +database: "{{{{ env_var('OPENLAKEFORGE_CATALOG_NAME') }}}}"
    +materialized: table
    +on_table_exists: replace
    +meta:
      dagster:
        group: {PRODUCT}
    gold:
      +tags: ["{DOMAIN}", "{PRODUCT}", "gold"]
"""

_DBT_SOURCES = f"""\
version: 2

sources:
  - name: silver
    database: "{{{{ env_var('OPENLAKEFORGE_CATALOG_NAME') }}}}"
    schema: "{{{{ env_var('OPENLAKEFORGE_CATALOG_SCHEMA_PREFIX', '') }}}}{DOMAIN}_silver"
    description: Silver taxi trips.
    tables:
      - name: {ENTITY}
"""

_DBT_SCHEMA = """\
version: 2

models:
  - name: mart_trip_totals
    description: Trip count and totals over the whole Silver trips table, one row.
  - name: mart_trips_by_payment
    description: Trip count and total amount per payment type.
"""

_MART_TOTALS = f"""\
select
    count(*) as trips,
    sum(total_amount) as total_amount_sum,
    sum(trip_distance) as trip_distance_sum
from {{{{ source('silver', '{ENTITY}') }}}}
"""

_MART_BY_PAYMENT = f"""\
select
    coalesce(cast(payment_type as varchar), 'null') as payment_type,
    count(*) as trips,
    sum(total_amount) as total_amount_sum
from {{{{ source('silver', '{ENTITY}') }}}}
group by 1
"""

_PIPELINE = f'''\
from __future__ import annotations

from openlakeforge_runtime.product_dagster import ProductDefinitionSpec, build_product_definitions

{PRODUCT.upper()}_SILVER_INPUTS = ("{ENTITY}",)

{PRODUCT.upper()}_GOLD_ASSETS = ("mart_trip_totals", "mart_trips_by_payment")

defs = build_product_definitions(
    ProductDefinitionSpec(
        domain="{DOMAIN}",
        product="{PRODUCT}",
        silver_inputs={PRODUCT.upper()}_SILVER_INPUTS,
        bronze_inputs=tuple(("{SOURCE}", table) for table in {PRODUCT.upper()}_SILVER_INPUTS),
        gold_assets={PRODUCT.upper()}_GOLD_ASSETS,
    )
)
'''
