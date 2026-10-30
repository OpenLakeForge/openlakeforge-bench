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
| `footprint [--clean\|--existing]` | A: install and idle | Each `olf deploy` phase (skipped with `--existing`, which measures the running deployment), time to the first Trino query, the first product pipeline and its Gold rows, pods per namespace, node storage, and CPU/RAM installed versus after `--settle-seconds` and over `--idle-seconds` |
| `etl --scale N [--delta D]` | B: end-to-end ETL | The CSV fixtures replicated N times through every product job, a no-change rerun, then a reload at N+D replicas: wall clock per run and per layer, cluster CPU/RAM per layer, rows, checksums, Iceberg data files, bytes and snapshots |
| `many-tables --tables N` | C: many tables | A generated project of N tables over `--domains` product jobs, run twice: Floe Jobs started and their pod start and run times, per-layer times, cluster CPU/RAM, every table's rows, files and snapshots against `expected.json` |
| `incremental --project P --product X` | D: incremental loads | An external project installed from its own profile, one product run `--runs` times, then a run killed `--kill-after-seconds` into Bronze and its retry, then a `--full-refresh`: per-run layer times as Silver and Gold grow, rows added per run, key uniqueness, Gold-to-Silver row parity |

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
`--gold-mirror` Gold table must hold as many rows as its Silver table. The
killed run must add nothing. Its retry may add one slice or two, because a
kill after dlt committed its load leaves that slice in Bronze. A Bronze full
refresh restarts a bounded cursor at the first slice, which Silver already
holds, so it adds nothing either. A `--live-check` product runs once against
its live source, untimed, and its outcome is recorded without failing the run.
Run it on a stage that has never run the product.

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

## Run on GitHub

The [benchmark workflow](.github/workflows/benchmark.yml) runs scenarios A and
B on one GitHub-hosted runner (4 vCPU, 16 GB, cgroup v2), the host the
platform's local sizing is written for, and scenarios C and D on one
runner each. Start it from the Actions tab, or:

```bash
gh workflow run benchmark.yml -R OpenLakeForge/openlakeforge-bench -f openlakeforge_ref=main -f etl_scales="100 10000"
```

| Input | Default | Meaning |
| --- | --- | --- |
| `openlakeforge_ref` | `main` | OpenLakeForge branch, tag or commit to measure |
| `etl_scales` | `100 10000` | Fixture replica counts for scenario B, run in order; each reloads at +10%. Empty skips scenario B |
| `idle_seconds` | `300` | Scenario A's steady-state window |
| `footprint_and_etl` | `true` | Run scenarios A and B |
| `many_tables` | `100` | Table counts for scenario C, run in order on one cluster; empty skips scenario C |
| `incremental_project` | `OpenLakeForge/acme-lakehouse` | Project scenario D installs and loads; empty skips scenario D |
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
| Phase times | `olf deploy --phase foundation\|prefetch\|platform\|artifacts`, each a timed subprocess |
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
| 2026-10-30 | GitHub-hosted runner, 4 vCPU, 15.6 GiB, cgroup v2 (reference) | [results/2026-10-30-github-runner](results/2026-10-30-github-runner/README.md) |
| 2026-10-29 | Cloud VM, 4 vCPU, 15.7 GiB, cgroup v1, with workarounds | [results/2026-10-29-cloud-vm](results/2026-10-29-cloud-vm/README.md) |

## Known limits of the method

- Every Silver entity in the reference project uses `incremental_mode: none`
  with `overwrite`, so the delta step is a full reload at a larger size, not
  an incremental merge.
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
