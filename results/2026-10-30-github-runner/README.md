# 2026-10-30: Slim on a GitHub-hosted runner (reference)

These results come from OpenLakeForge commit `061f5d1` (SeaweedFS 4.48 and #629's S3 read-buffer fix), with the Slim preset and one DEV stage. They ran on an `ubuntu-latest` runner (4 vCPU, 15.6 GiB, cgroup v2, kernel 7.0.0-azure) in [benchmark run 41028068120](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41028068120). Nothing on the host was overridden: no kind-config or image override, and no modified files. This is the reference run; the [2026-10-29 cloud-VM run](../2026-10-29-cloud-vm/README.md) needed workarounds.

Every number below is read from the JSON files in this directory. `python -m olf_bench summarize *.json` renders them as tables.

## Findings

| | Finding |
| --- | --- |
| Holds | All three scales passed every check, including the ×10,000 rerun that OOM-killed the SeaweedFS S3 gateway on 4.23, and the new ×30,000 (4.14M rows) and its ×33,000 reload (4.55M rows): 18 product runs at ×10,000 and above, with no failure. Silver rows equalled the generated input at every scale, and every no-change rerun reproduced every table's checksum. |
| Ceiling not reached | Peak cluster memory stayed at 6.2–6.6 GiB on a 15.6 GiB host up to 4.55M rows. Silver and Gold times stayed flat from 14k to 4.55M rows. |
| Bottleneck | Bronze grows linearly with volume and is the slowest layer from ×30,000: 1.1–2.1 s at ×100, 22–37 s at ×10,000, 65–105 s at ×30,000 (about half of each run). The CSV loader holds every row in memory and uploads one object per entity. |
| Cost | Silver (Floe) takes 34–52 s on this runner at every scale, about twice the 15–27 s it took on the cloud VM. It is the slowest layer until Bronze overtakes it. |
| Cost | A clean install takes 598 s (10 min), against about 16 min on the cloud VM; prefetch fell from 435–471 s to 134 s. |
| Cost | At idle the cluster uses 0.42 cores on average (p95 0.93) and 4.16 GiB, with 28 containers running in 42 pods. |
| Cost | Every overwrite run adds snapshots, and nothing expires them: by the last run some tables held 19. |

## Scenario A: install and idle

| Phase | Time | Peak CPU | Peak memory |
| --- | ---: | ---: | ---: |
| foundation (kind cluster) | 122.0 s | – | – |
| prefetch | 134.3 s | 3.8 cores | 1.5 GiB |
| platform | 149.6 s | 3.7 cores | 3.1 GiB |
| artifacts | 192.1 s | 3.6 cores | 4.0 GiB |
| install total | 598.1 s | | |

| After install | Value |
| --- | ---: |
| Time to first Trino query | 3.5 s |
| First pipeline (`order_revenue`, fixtures), Dagster run | 97.1 s |
| Bronze / Silver / Gold | 1.6 / 46.4 / 28.5 s |
| Pods: running / completed jobs / total | 28 / 14 / 42 |
| Node image stores, 3 nodes | 21.0 GB |
| Persistent volumes | 76 MB |
| Memory, installed / steady state | 4.18 / 4.16 GiB |
| CPU at steady state, mean / p95 / max | 0.42 / 0.93 / 1.50 cores |

## Scenario B: end-to-end ETL

Each scale ran every product job once, then a no-change rerun, then a reload at +10%. The table shows the initial run.

| Scale | Rows | CSV | Run per product | Bronze | Silver | Gold | Peak cluster memory | Silver Iceberg |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| ×100 | 13,800 | 0.7 MB | 78–92 s | 1.2–1.6 s | 34–46 s | 22–24 s | 5.1–5.2 GiB | 0.1 MB |
| ×10,000 | 1,380,000 | 78.6 MB | 100–126 s | 22–33 s | 34–46 s | 23–27 s | 5.9–6.2 GiB | 6.9 MB |
| ×30,000 | 4,140,000 | 242.9 MB | 142–187 s | 65–95 s | 34–46 s | 23–25 s | 6.2 GiB | 15.4 MB |
| ×33,000 (reload) | 4,554,000 | 267.6 MB | 154–196 s | 72–105 s | 39–52 s | 23–25 s | 6.5–6.6 GiB | 16.1 MB |

Gold held 1,551,035 rows (5.95 MB) at ×33,000. Silver stayed in 15 data files at every scale.

## Not measured

- DuckDB Slim (OpenLakeForge/openlakeforge#547).
- The Full preset.
- Scenario C, and the restart, failure and retry cases.
- Anything above ×33,000. The ceiling lies beyond 4.55M rows on this host.
- S3 gateway restarts were not counted directly; no run failed, where on 4.23 each restart failed the run in flight.
