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

`python -m olf_bench summarize <result.json>...` prints a Markdown table of one
or more results: every step's status and time, the Dagster run time and
per-layer split of each product job, peak cluster memory, and the first line of
any error.

## Run on GitHub

The [benchmark workflow](.github/workflows/benchmark.yml) runs both scenarios
on a GitHub-hosted runner (4 vCPU, 16 GB, cgroup v2), the host the platform's
local sizing is written for. Start it from the Actions tab, or:

```bash
gh workflow run benchmark.yml -R OpenLakeForge/openlakeforge-bench -f openlakeforge_ref=main -f etl_scales="100 10000"
```

| Input | Default | Meaning |
| --- | --- | --- |
| `openlakeforge_ref` | `main` | OpenLakeForge branch, tag or commit to measure |
| `etl_scales` | `100 10000` | Fixture replica counts for scenario B, run in order; each reloads at +10%. Empty skips scenario B |
| `idle_seconds` | `300` | Scenario A's steady-state window |

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
- Restart, failure and retry cases (OpenLakeForge/openlakeforge#343, #533 and
  #53) and the ~100-table scenario C are not covered yet.

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
