# 2026-11-01: the Full preset (scenarios A and B)

These runs use OpenLakeForge `824820b` (Floe 0.8.9) with the **Full** preset, on an `ubuntu-latest` runner (4 vCPU, 15.6 GiB, cgroup v2), in [run 41318362081](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41318362081).

The comparison column is Slim from [2026-11-01-main](../2026-11-01-main/README.md) (A, and B at ×10,000) and [2026-11-01-floe-089](../2026-11-01-floe-089/README.md) (A).

| File | Scenario |
| --- | --- |
| [footprint.json](footprint.json) | A: install, first pipeline, 300 s idle |
| [etl-x100.json](etl-x100.json), [etl-x10000.json](etl-x10000.json) | B at ×100 and ×10,000: initial, repeat and +10% delta |

**Holds:** every step passed every check.

## Scenario A

| | Full | Slim (floe-089) |
| --- | ---: | ---: |
| foundation / prefetch / platform / artifacts | 122 / 424 / 336 / 258 s | 122 / 137 / 186 / 200 s |
| Install, total | 1140 s | 646 s |
| First pipeline (Dagster run; Bronze / Silver / Gold) | 66 s (1.9 / 16.2 / 27.4 s) | 66 s (2.0 / 16.8 / 28.0 s) |
| Time to first query | 2.2 s | 3.0 s |
| Idle, 300 s: CPU mean / memory mean | 0.56 cores / 7.1 GiB | 0.38 cores / 4.1 GiB |
| Pods (running / completed) | 53 (33 / 20) | 42 (28 / 14) |
| Node image stores | 44.1 GB | 21.6 GB |

## Scenario B (min–max over 9 product runs)

| Scale | Preset | Dagster run | Bronze | Silver | Gold | Peak memory |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| ×100 | Full | 54–63 s | 1.2–1.8 s | 10.2–14.5 s | 21.5–26.4 s | 7.8–8.7 GiB |
| ×10,000 | Full | 80–98 s | 22.7–37.6 s | 12.0–15.1 s | 22.9–28.7 s | 8.6–9.9 GiB |
| ×10,000 | Slim (main) | 81–101 s | 17–28 s | 9.5–13.2 s | 21.2–25.4 s | 6.1–6.6 GiB |

## What it shows

**Pipelines aren't slower on Full.** Its extra components (OpenMetadata, Superset and the rest) add no work to a product run.

**Full costs on the platform side:**
- **Memory:** about 3 GiB more at idle and under load.
- **Install time:** 494 s more, mostly prefetch and platform, which pull twice the images.
- **Disk:** twice the node image storage.

**Headroom on this runner:** at ×10,000, Full peaks at 9.9 GiB of 15.6 GiB. Scenario B at ×100,000 and above was not run on Full; on Slim it already needs 7.4 GiB.
