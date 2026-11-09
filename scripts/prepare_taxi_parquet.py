# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow==26.0.0"]
# ///
"""Build the large Parquet input of scenario E from the NYC TLC yellow-taxi trip records.

The TLC publishes one Parquet file per month (about 50 MB, 3 million trips) on a public CDN that needs no account.
This downloads months until their rows reach `--size-gb`, and writes them as ONE Parquet file: a single large file
is what a source hands over, and it is what stresses a loader's per-file memory. Columns are renamed to snake_case,
cast to one fixed type each and reduced to the twelve the benchmark's contract uses, so every month and every
repeat has the same schema. Past 24 months of data the months repeat; the rows are real, but the file then
contains each trip more than once (the Silver contract has no primary key).

It also writes `<output>.json`: the row count, size, SHA-256 and the aggregates the Gold layer must reproduce,
computed here from the file while it is written, never from the platform.

    uv run scripts/prepare_taxi_parquet.py --size-gb 1 --output .tmp/taxi/trips.parquet

Run it outside the OpenLakeForge environment: that one has no pyarrow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
MONTHS = [f"2024-{m:02d}" for m in range(1, 13)] + [f"2023-{m:02d}" for m in range(1, 13)]

# source column -> (name in the benchmark's file, type)
COLUMNS: dict[str, tuple[str, pa.DataType]] = {
    "VendorID": ("vendor_id", pa.int64()),
    "tpep_pickup_datetime": ("pickup_at", pa.timestamp("us")),
    "tpep_dropoff_datetime": ("dropoff_at", pa.timestamp("us")),
    "passenger_count": ("passenger_count", pa.int64()),
    "trip_distance": ("trip_distance", pa.float64()),
    "PULocationID": ("pickup_location_id", pa.int64()),
    "DOLocationID": ("dropoff_location_id", pa.int64()),
    "payment_type": ("payment_type", pa.int64()),
    "fare_amount": ("fare_amount", pa.float64()),
    "tip_amount": ("tip_amount", pa.float64()),
    "tolls_amount": ("tolls_amount", pa.float64()),
    "total_amount": ("total_amount", pa.float64()),
}
SCHEMA = pa.schema([pa.field(name, kind) for name, kind in COLUMNS.values()])


def download(month: str, cache: Path) -> Path:
    target = cache / f"yellow_tripdata_{month}.parquet"
    if target.exists() and target.stat().st_size > 0:
        return target
    cache.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".part")
    for attempt in range(1, 5):
        try:
            with urllib.request.urlopen(URL.format(month=month), timeout=120) as response, partial.open("wb") as out:
                while chunk := response.read(1 << 20):
                    out.write(chunk)
            partial.rename(target)
            return target
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            print(f"  {month}: attempt {attempt} failed: {exc}", file=sys.stderr)
            time.sleep(2**attempt)
    raise SystemExit(f"could not download {URL.format(month=month)}")


def conform(table: pa.Table) -> pa.Table:
    """The twelve columns, renamed and cast; a month that lacks one or cannot be cast stops the run."""
    arrays = []
    for source, (_, kind) in COLUMNS.items():
        column = table[source]
        arrays.append(column if column.type == kind else pc.cast(column, kind, safe=False))
    return pa.Table.from_arrays(arrays, schema=SCHEMA)


class Aggregates:
    """What the Gold layer must reproduce: totals overall and per payment type."""

    def __init__(self) -> None:
        self.rows = 0
        self.total_amount = 0.0
        self.trip_distance = 0.0
        self.by_payment: dict[str, list[float]] = {}
        self.first_pickup: object = None
        self.last_pickup: object = None

    def add(self, table: pa.Table) -> None:
        self.rows += table.num_rows
        self.total_amount += pc.sum(table["total_amount"]).as_py() or 0.0
        self.trip_distance += pc.sum(table["trip_distance"]).as_py() or 0.0
        grouped = table.group_by("payment_type").aggregate(
            [("total_amount", "sum"), ("total_amount", "count", pc.CountOptions(mode="all"))]
        )
        for key, total, count in zip(
            grouped["payment_type"].to_pylist(),
            grouped["total_amount_sum"].to_pylist(),
            grouped["total_amount_count"].to_pylist(),
            strict=True,
        ):
            entry = self.by_payment.setdefault("null" if key is None else str(key), [0, 0.0])
            entry[0] += count
            entry[1] += total or 0.0
        low, high = pc.min_max(table["pickup_at"]).values()
        low, high = low.as_py(), high.as_py()
        if low is not None and (self.first_pickup is None or low < self.first_pickup):
            self.first_pickup = low
        if high is not None and (self.last_pickup is None or high > self.last_pickup):
            self.last_pickup = high

    def as_dict(self) -> dict:
        return {
            "rows": self.rows,
            "total_amount_sum": self.total_amount,
            "trip_distance_sum": self.trip_distance,
            "by_payment_type": {
                key: {"rows": int(rows), "total_amount_sum": total}
                for key, (rows, total) in sorted(self.by_payment.items())
            },
            "first_pickup": str(self.first_pickup),
            "last_pickup": str(self.last_pickup),
        }


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1 << 24):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--size-gb", type=float, required=True, help="Stop at the first row group past this many GB.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, default=Path(".tmp/taxi-cache"))
    parser.add_argument("--row-group-rows", type=int, default=1_000_000)
    parser.add_argument("--compression", default="snappy", choices=("snappy", "zstd", "gzip", "none"))
    args = parser.parse_args()
    if args.size_gb <= 0:
        raise SystemExit("--size-gb must be positive")

    target_bytes = int(args.size_gb * 1e9)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    totals = Aggregates()
    read: list[str] = []
    passes = 0
    started = time.monotonic()
    with args.output.open("wb") as sink:
        writer = pq.ParquetWriter(sink, SCHEMA, compression=args.compression)
        try:
            while sink.tell() < target_bytes:
                for month in MONTHS:
                    source = pq.ParquetFile(download(month, args.cache_dir))
                    read.append(month)
                    for index in range(source.num_row_groups):
                        table = conform(source.read_row_group(index, columns=list(COLUMNS)))
                        totals.add(table)
                        writer.write_table(table, row_group_size=args.row_group_rows)
                        if sink.tell() >= target_bytes:
                            break
                    print(f"  {month}: {totals.rows:,} rows, {sink.tell() / 1e9:.2f} GB", flush=True)
                    if sink.tell() >= target_bytes:
                        break
                passes += 1
        finally:
            writer.close()

    footer = pq.ParquetFile(args.output).metadata
    if footer.num_rows != totals.rows:
        raise SystemExit(f"footer says {footer.num_rows} rows, the aggregates saw {totals.rows}")
    record = {
        "source": "NYC TLC yellow taxi trip records",
        "url_pattern": URL,
        "target_gb": args.size_gb,
        "months_read": read,
        "distinct_months": len(set(read)),
        "full_passes_over_the_24_months": passes,
        "file": args.output.name,
        "file_bytes": args.output.stat().st_size,
        "sha256": sha256_of(args.output),
        "row_groups": footer.num_row_groups,
        "row_group_rows": args.row_group_rows,
        "compression": args.compression,
        "columns": {name: str(kind) for name, kind in COLUMNS.values()},
        "aggregates": totals.as_dict(),
        "prepared_seconds": round(time.monotonic() - started, 1),
    }
    manifest = args.output.with_suffix(".json")
    manifest.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    size_gb = record["file_bytes"] / 1e9
    print(f"{totals.rows:,} rows, {size_gb:.2f} GB, {footer.num_row_groups} row groups -> {args.output}")


if __name__ == "__main__":
    main()
