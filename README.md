# openlakeforge-bench

Reproducible measurements of what a local OpenLakeForge deployment costs and
how its data path behaves as volume grows
([OpenLakeForge/openlakeforge#328](https://github.com/OpenLakeForge/openlakeforge/issues/328)).
It reports measurements, failures and ceilings, not targets;
[#329](https://github.com/OpenLakeForge/openlakeforge/issues/329) draws the
conclusions. The harness lives here rather than in the platform repository
because it is occasional tooling, not something every platform change has to
carry.

## Scenarios

| Command | Scenario | Measures |
| --- | --- | --- |
| `footprint [--clean\|--existing]` | A: install and idle | Each `olf infra deploy` phase (skipped with `--existing`, which measures the running deployment), time to the first Trino query, the first product pipeline and its Gold rows, pods per namespace, node storage, and CPU/RAM installed versus after `--settle-seconds` and over `--idle-seconds` |
| `etl --scale N [--delta D]` | B: end-to-end ETL | The CSV fixtures replicated N times through every product job, a no-change rerun, then a reload at N+D replicas: wall clock per run and per layer, cluster CPU/RAM per layer, rows, checksums, Iceberg data files, bytes and snapshots |
| `many-tables --tables N` | C: many tables | A generated project of N tables over `--domains` product jobs, run twice: Floe Jobs started and their pod start and run times, per-layer times, cluster CPU/RAM, every table's rows, files and snapshots against `expected.json` |
| `incremental --project P --product X` | D: incremental loads | An external project installed from its own profile, one product run `--runs` times, then a run killed `--kill-after-seconds` into Bronze and its retry, then a `--full-refresh`: per-run layer times as Silver and Gold grow, rows added per run, key uniqueness, Gold-to-Silver row parity |
| `parquet --data F` | E: large Parquet | One Parquet file of NYC taxi trips, 1 GB and up, through Bronze, Silver and Gold: per-layer time and cluster CPU/RAM, the Floe Job's start, run time and exit code (an OOM kill is a result), the Silver table's rows against the file's footer, and Gold's totals against aggregates computed from the file |

Every product run also records `silver_breakdown`: for each Floe Kubernetes Job
of the run (read from the Kubernetes API right after the run, before the 3600 s
TTL deletes it), the Job, pod and container timestamps and the Silver step split
into step start to Job created, Job created to pod scheduled, scheduled to
container started, container run, container finished to Job complete, and Job
complete to step end (Dagster polling, log and report reading, event writing).
Kubernetes timestamps have 1 s resolution, so each segment is good to about
+/-1 s; a Job or pod that is gone is recorded as null. `summary` prints it.

Every product run also records `postgres`: the deltas, over the run, of the
metadata PostgreSQL's `pg_stat_wal`, the stage's Dagster database in
`pg_stat_database` and the container's cgroup v2 `cpu.stat`, `cpu.pressure` and
`io.pressure`, read with `kubectl exec` into `statefulset/postgresql` of the
platform namespace, and derived commits/s, WAL fsyncs/s, mean fsync time, CPU
cores used and the share of the run spent in CPU pressure, I/O pressure and
throttling. WAL and block times are null unless the server has
`track_wal_io_timing` and `track_io_timing` on; the harness does not switch
them on. A reading that fails is recorded as `postgres.error`, never as a
failed run. `summary` prints it.

Each run writes `result.json`, plus one log per `olf` subprocess, under
`--output` (default `<project-root>/.tmp/bench/<scenario>-<UTC time>`). A step
that fails is recorded with its elapsed time and error, the result's `status`
becomes `failed`, and the command exits 1: a stopped run never reads as a
fast one.

## Run

The harness measures the OpenLakeForge checkout it runs in. Run it on that
checkout's own `olf` environment, so the code measured is exactly the code
deployed:

```bash
cd ~/src/openlakeforge
PYTHONPATH=~/src/openlakeforge-bench uv run --project tools/olf --locked python -m olf_bench footprint --clean
```

```bash
PYTHONPATH=~/src/openlakeforge-bench uv run --project tools/olf --locked python -m olf_bench etl --scale 100 --delta 10
```

```bash
PYTHONPATH=~/src/openlakeforge-bench uv run --project tools/olf --locked python -m olf_bench etl --scale 10000 --delta 1000
```

The harness needs nothing beyond what `olf` already installs, which is why it
runs from `PYTHONPATH` instead of `uv run --with`: an overlay would install a
second `olf` over the checkout's. Hardware: the
[local installation sizing](https://github.com/OpenLakeForge/openlakeforge/blob/main/docs/guides/local-installation.md#hardware)
(4 CPUs, 16 GB of memory for Docker); the Slim preset on three kind nodes held
about 20 GB of images, so leave at least 40 GB of disk free.

Scenario D measures a project outside this checkout, such as
[acme-lakehouse](https://github.com/OpenLakeForge/acme-lakehouse), on a
cluster of its own: it installs the platform from that project's
`openlakeforge.yaml`, then binds the stage's sources before the artifacts
phase activates them. Each binding value comes from the variable the loader
itself reads, `SOURCES__<SOURCE>__<KEY>`; the result names the ConfigMaps and
Secrets it applied, never their values.

```bash
PYTHONPATH=~/src/openlakeforge-bench uv run --project tools/olf --locked python -m olf_bench incremental \
  --project ~/src/acme-lakehouse --product rna_catalog --runs 5 --rows-per-run 100000 \
  --gold-mirror mart_sequence_index=sequences --live-check food_quality
```

Every run must add exactly `--rows-per-run` rows to each Silver entity that
reads an incremental resource, and keep its primary key unique. Each
`--gold-mirror` Gold table must hold as many rows as its Silver table. The killed run
must add nothing. Its retry may add one slice or two, because a kill after dlt
committed its load leaves that slice in Bronze. A Bronze full refresh carries
both run tags `olf infra product run --full-refresh bronze` sets, so Floe
rebuilds Silver from the new extract: with a bounded cursor, Silver then holds
the first slice alone, and Gold still mirrors it. A `--live-check` product
runs once against its live source, untimed, and its outcome is recorded
without failing the run. Run it on a stage that has never run the product.

Scenario C measures what each table costs (Floe Jobs, catalog calls,
memory) on a project that no external service can slow down or break.
`many-tables-project` writes one:

```bash
uv run --locked python -m olf_bench many-tables-project --tables 100 --domains 4 --rows 1000 --output .tmp/many-tables
```

| Generated | Per domain |
| --- | --- |
| A CSV source `feed_NN`, loaded by the platform's CSV loader (`libs.bronze_csv`) | one |
| Silver tables `tNNN`: an `id` key and 3 to 10 seeded columns of the reference contracts' types | `--tables / --domains` |
| A Gold product `profile_NN` whose `mart_table_rows` counts every Silver table of the domain, and its Dagster job | one |

The project has a DEV-only Slim profile, like the reference installation.
`expected.json` records each table's rows, columns and CSV SHA-256, and each
Gold table's row count. The same arguments always write byte-identical files;
`--seed` (default 328) changes the data.

```bash
PYTHONPATH=~/src/openlakeforge-bench uv run --project tools/olf --locked python -m olf_bench many-tables --tables 100
```

`many-tables` generates that project under `--output`, installs the platform
from its profile (`--existing` deploys onto a running DEV-only Slim platform
instead), and runs every product job `--runs` times (default 2: one cold run,
one repeat). Each run records the Dagster run and its layer split, and every
Floe Kubernetes Job it started: the seconds from pod creation to container
start, and the container's run time. After each run, every Silver table must
hold the generated rows and every `mart_table_rows` must list its domain's
tables with their row counts; the time Trino took to read each table's rows
and metadata is recorded with its data files and snapshots.

`python -m olf_bench summarize <result.json>...` prints a Markdown table of one
or more results: every step's status and time, the Dagster run time and
per-layer split of each product job, peak cluster memory, and the first line of
any error.

## Scenario E: a large Parquet file

The reference products load CSV. Real sources often hand over one large Parquet file, which has no text to parse and
stresses other paths: a loader's per-file memory, Floe's read of a file bigger than its chunk, row groups and
compression. `scripts/prepare_taxi_parquet.py` builds the input from the
[NYC TLC yellow-taxi trip records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), which are public and
need no account: it downloads months until their rows reach `--size-gb` and writes them as ONE Parquet file, with
twelve columns renamed and cast to fixed types. A month is about 50 MB and 3 million trips, so 1 GB is 16 months and
about 54 million rows. Past 24 months of data (about 1.2 GB) the months repeat: the rows are real, but the file then
holds each trip more than once, and the manifest says how many passes it took. Run it outside the OpenLakeForge
environment, which has no pyarrow:

```bash
uv run --no-project scripts/prepare_taxi_parquet.py --size-gb 1 --output .tmp/taxi/trips.parquet
```

It writes `trips.json` beside the file: the row count, size, SHA-256, row groups and the aggregates (total amount and
distance, and trips and amount per payment type) that Gold must reproduce. They are computed from the file while it
is written, never from the platform.

```bash
PYTHONPATH=~/src/openlakeforge-bench uv run --project tools/olf --locked python -m olf_bench parquet --data .tmp/taxi/trips.parquet
```

`parquet-project` writes the project without running it. It has one Parquet source whose loader uploads the file as it
is (the row count comes from the footer), one Floe contract with every column nullable so that no row is rejected, and
one Gold product with two marts. The file ships in the project-code image, as scenario B ships its CSV fixtures. After
each run, Silver must hold exactly the footer's rows, Gold's trip counts must match exactly, and its sums must match to
a relative 1e-9 (Trino and the script add doubles in different orders). A gap is a failure, never a tolerance. Silver
has no content checksum here: at tens of millions of rows the sums are the content check.

## Run on GitHub

The [benchmark workflow](.github/workflows/benchmark.yml) runs scenarios A and
B on one GitHub-hosted runner (4 vCPU, 16 GB, cgroup v2), the host the
platform's local sizing is written for, and scenarios C, D and E on one
runner each. Start it from the Actions tab, or:

```bash
gh workflow run benchmark.yml -R OpenLakeForge/openlakeforge-bench -f openlakeforge_ref=main -f etl_scales="100 10000"
```

| Input | Default | Meaning |
| --- | --- | --- |
| `openlakeforge_ref` | `main` | OpenLakeForge branch, tag or commit to measure |
| `openlakeforge_patch` | empty | A file in `patches/` applied to the OpenLakeForge checkout before it is measured, to measure a candidate fix without a branch in OpenLakeForge; pin `openlakeforge_ref` to the commit the patch was made against |
| `etl_scales` | `100 10000` | Fixture replica counts for scenario B, run in order; each reloads at +10%. Empty skips scenario B |
| `idle_seconds` | `300` | Scenario A's steady-state window |
| `footprint_and_etl` | `true` | Run scenarios A and B |
| `many_tables` | `100` | Table counts for scenario C, run in order on one cluster; `none` skips scenario C (the API turns an empty value into the default) |
| `many_tables_domains` | `4` | Domains, and product jobs, each scenario C count is spread over. Keep a domain's share at about 50 tables or fewer: every product's `mart_table_rows` reads all of its domain's tables in one query, and at 75 tables Trino refuses it (`QUERY_HAS_TOO_MANY_STAGES`, 152 stages against its default maximum of 150) |
| `parquet_sizes` | empty | Sizes in GB of the Parquet file scenario E loads, run in order on one cluster, such as `"1 2.5"`; empty skips scenario E. Each size is built on the runner from the NYC taxi records |
| `parquet_compression` | `snappy` | Compression of that file: `snappy`, `zstd`, `gzip` or `none` |
| `incremental_project` | empty | Project scenario D installs and loads, such as `OpenLakeForge/acme-lakehouse`; empty skips scenario D |
| `incremental_project_ref` | `main` | Its branch, tag or commit |
| `incremental_runs` | `5` | Scenario D's incremental runs of `rna_catalog` |

Scenario D binds acme's sources: RNAcentral's public database, and the Open
Food Facts staging API for the `food_quality` live check. RNAcentral publishes
its read-only login, but the workflow still reads it from the repository
secrets `RNACENTRAL_USERNAME` and `RNACENTRAL_PASSWORD`, like any credential.

The job frees disk, installs Slim through scenario A, then runs scenario B at
each scale on the same cluster. A failed scale is recorded and the next still
runs. The run page shows the `summarize` table, and the `benchmark-<run id>`
artifact keeps every `result.json` and `olf` log for 90 days, plus kind
diagnostics when a step failed. One run takes one to three hours of runner
time, so the workflow only starts by hand.

## How the numbers are taken

| Quantity | Source |
| --- | --- |
| Phase times | `olf infra deploy --phase foundation\|prefetch\|platform\|artifacts`, each a timed subprocess |
| CPU and memory | `docker stats` of the kind node containers, summed and sampled every `--sample-interval` seconds; this includes the kubelet, containerd and control plane, as the host pays them |
| Storage | `du` of `/var/lib/containerd` and `/var/local-path-provisioner` inside every node; kind keeps a copy of each image per node |
| Layer times | Dagster step stats: `<source>_bronze_resources` is Bronze, `<product>_dbt_gold_assets` is Gold, and the Floe domain steps are Silver. A layer's time is its wall-clock span |
| Validity | Silver rows must equal the generated rows per entity, every Gold table must have rows, and a no-change rerun must reproduce every table's checksum |

The ETL dataset is deterministic. Replica 0 is the fixture itself. Replica `r`
suffixes every primary-key value, and every column named after a primary key
(keys come from the Floe contracts), with `-R<r>`. Each replica is therefore a
disjoint copy whose references resolve. The same scale always produces
byte-identical files, and their SHA-256 is recorded.

`result.json` records the commit, the files modified in the checkout, the
component-catalog SHA-256, host CPUs, memory, kernel and cgroup version, the
Docker version, and any kind-config or image override in effect.

## Results

| Date | Host | Summary |
| --- | --- | --- |
| 2026-11-08 | GitHub-hosted runner, 4 vCPU, 15.6 GiB, cgroup v2: A, B at ×100, ×10,000 and ×30,000, and C at 100 tables, after the Trino and dlt changes | [results/2026-11-08-main](results/2026-11-08-main/README.md) |
| 2026-10-30 | GitHub-hosted runner: scenario C, 100 and 300 tables | [results/2026-10-30-many-tables](results/2026-10-30-many-tables/README.md) |
| 2026-10-30 | GitHub-hosted runner, 4 vCPU, 15.6 GiB, cgroup v2 (reference) | [results/2026-10-30-github-runner](results/2026-10-30-github-runner/README.md) |
| 2026-10-29 | Cloud VM, 4 vCPU, 15.7 GiB, cgroup v1, with workarounds | [results/2026-10-29-cloud-vm](results/2026-10-29-cloud-vm/README.md) |

## Known limits of the method

- Every Silver entity in the reference project uses `incremental_mode: none`
  with `overwrite`, so the delta step is a full reload at a larger size, not
  an incremental merge.
- Scenario E's data is the NYC TLC trip records, not a seeded fixture. Past 24 months of data (about 1.2 GB) the months repeat, so a larger file holds each trip more than once. Its Silver contract has no primary key, and Silver has no content checksum, only row counts and Gold's sums.
- CPU and memory are cluster totals per window, not per pod: a layer's peak
  includes everything else the cluster did at the same time.
- Scenario D covers one kill-and-retry case, a run terminated during Bronze.
  Platform restarts and the other failure cases (OpenLakeForge/openlakeforge#343,
  #533 and #53) are not covered yet.
- Scenario D reads RNAcentral's shared public database, so its Bronze times
  include that service's latency and load.

## Development

```bash
uv sync --locked --python 3.12
uv run --locked ruff check .
uv run --locked mypy
uv run --locked pytest
```

CI runs these against the OpenLakeForge commit `pyproject.toml` pins. The
harness imports `olf` internals (deploy contexts, the e2e Trino and Dagster
helpers), so moving that pin is the way to notice when a platform change
breaks the harness. `tests/fixtures/project` is the descriptor, Floe-contract
and CSV subset of the reference project at that same commit.
