# 2026-11-08: scenarios A, B and C on main after the Trino and dlt optimisations

Scenarios A and B ran on OpenLakeForge main `b7a4003` in [run 42372012921](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/42372012921). Scenario C ran on main `82e6a1e` in [run 42483540034](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/42483540034), because it needed harness fixes that landed the same day (below) and main had moved on in between.

Both ran on `ubuntu-latest` runners (4 vCPU, 15.6 GiB, cgroup v2) with the Slim preset. They are compared with the [1 Nov run](../2026-11-01-main/) on `69977b5`, which used the same runner type and fixtures. Each ETL scale ran its three products through three steps: initial, repeat and +10% delta.

| File | Scenario |
| --- | --- |
| [footprint.json](footprint.json) | A: install, first pipeline, 300 s idle |
| [etl-x100.json](etl-x100.json), [etl-x10000.json](etl-x10000.json), [etl-x30000.json](etl-x30000.json) | B: end-to-end ETL at ×100, ×10,000 and ×30,000 |
| [many-tables-t100.json](many-tables-t100.json) | C: 100 tables over 4 domains |

## Findings

| | Finding |
| --- | --- |
| Faster | **Bronze no longer grows with volume.** At ×30,000 it takes 1.9–3.5 s instead of 50–81 s; at ×10,000, 1.2–2.1 s instead of 17–28 s; at ×100, 0.6–1.4 s. It is now the smallest layer at every scale. |
| Faster | **Product runs at ×30,000 are 36–50% shorter:** 61–100 s instead of 121–156 s. At ×10,000 the range is 61–100 s against 81–101 s, and at ×100 61–90 s against 61–104 s. |
| Shift | **Gold is now the slowest layer at every scale** (18.0–24.9 s), then Silver (8.9–13.5 s). Silver is unchanged; Gold is 0.5–2.5 s lower. |
| Faster | **Scenario C: Silver drops from 36–39 s to 21–25 s and the Dagster run from 95–104 s to 59–71 s.** A 25-table Floe Job takes 3–6 s (it was 5–8 s), the median entity 0.21–0.34 s (it was 0.34–0.62 s), and Trino reads a table in 5.3–5.4 s (it was 8.0–8.1 s). |
| Holds | **Every scale and both runs passed every check.** Trino answered the first query on its first attempt at 3.0 s. |
| Cost | **Idle memory is higher, B's peak is not clearly different.** Idle mean is 4.69 GiB (4.3 GiB on 1 Nov). B's peak is within about 0.3 GiB of 1 Nov at every scale: higher at ×100 and at the top of ×30,000, lower at ×10,000. Idle CPU is lower: 0.31 cores against 0.37. |
| Method | **Wall times cluster at about 61, 71, 91 and 100 s, which looks like Dagster's poll interval.** Compare layers and the Dagster run time; a wall time is only good to about 10 s. In scenario C the step also includes opening a Dagster port-forward and reading the Floe pods' logs, so it stays at 110–141 s although the Dagster run fell by about 40 s. |

## Scenario B per layer (min–max over the 9 product runs)

Product run is the harness's wall time for the step.

| Scale | Floe | Product run | Bronze | Silver | Gold | Peak memory |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| ×100 | 1 Nov | 61–104 s | 1.0–1.6 s | 9.6–13.7 s | 20.4–23.0 s | 5.2–5.7 GiB |
| ×100 | now | 61–90 s | 0.6–1.4 s | 9.2–13.5 s | 18.0–21.8 s | 5.49–5.84 GiB |
| ×10,000 | 1 Nov | 81–101 s | 17–28 s | 9.5–13.2 s | 21.2–25.4 s | 6.1–6.6 GiB |
| ×10,000 | now | 61–100 s | 1.2–2.1 s | 9.2–13.2 s | 20.0–24.9 s | 5.99–6.31 GiB |
| ×30,000 | 1 Nov | 121–156 s | 50–81 s | 11.6–18.5 s | 21.9–24.0 s | 6.4–6.9 GiB |
| ×30,000 | now | 61–100 s | 1.9–3.5 s | 8.9–13.0 s | 19.6–23.3 s | 6.38–7.11 GiB |

## Scenario A

| | 1 Nov | now |
| --- | ---: | ---: |
| foundation / prefetch / platform / artifacts | 144 / 191 / 162 / 204 s | 141 / 171 / 183 / 208 s |
| First pipeline (Bronze / Silver / Gold) | 84 s (1.3 / 13.6 / 26.8) | 71 s (1.1 / 12.9 / 24.7) |
| Time to first query | 3.2 s | 3.0 s |
| Idle, 300 s: CPU mean / memory mean | 0.37 cores / 4.3 GiB | 0.31 cores / 4.69 GiB |

Install took 703 s against 701 s: the phases moved against each other (prefetch −20 s, platform +21 s) and the total did not.

## Scenario C

100 tables over 4 domains, two runs of each of the 4 product jobs (8 runs, 8 Floe Jobs). Every table's rows and checksum matched `expected.json`, each table held 1 data file, and every `mart_table_rows` listed its 25 tables.

| | 1 Nov (`69977b5`) | now (`82e6a1e`) |
| --- | ---: | ---: |
| Dagster run | 95–104 s | 59–71 s |
| Bronze | 7.7–8.6 s | 3.3–3.8 s |
| Silver | 36.2–38.9 s | 21.2–25.1 s |
| Gold | 22.0–30.5 s | 16.8–23.8 s |
| Floe Job run time | 5.0–8.0 s | 3.0–6.0 s |
| Median entity in a Floe Job | 0.34–0.62 s | 0.21–0.34 s |
| Trino read of one table, median | 8.0–8.1 s | 5.3–5.4 s |
| Peak memory | 4.82–5.70 GiB | 5.02–5.92 GiB |

Four entities ran at once in every Job, as before. The Floe Job is still only 3–6 s of a Silver step that takes 21–25 s, so most of Silver is outside the Job itself. This run does not show where.

## Harness changes this run needed

Scenario C failed four times on 7–8 Nov before it passed, each time because main had changed under the generated project. [PR #24](https://github.com/OpenLakeForge/openlakeforge-bench/pull/24) fixed them:

1. The Gold products' `packages.yml` named `libs/dbt/openlakeforge_dbt`, which the project-code image no longer has, so `dbt deps` failed in the image build.
2. `governance.yml` is now v1beta2 (products nested under domains), `source.yaml` v1beta1, and the profile's `spec.access.issuer: none` is `tls: none`, so `olf` rejected the project.
3. The generated code imported `libs.product_dagster` and `libs.bronze_csv`, now in `openlakeforge_runtime`, so the Dagster code location crash-looped until Helm's atomic release timed out after 20 minutes.

The first of these was hidden because `olf` keeps only the first 2,000 characters of a failed command's stderr.

Scenarios A and B failed once on 7 Nov with `Trino answered HTTP 401` for the whole first-query window; that was a bug in main, fixed before this run.

## Limits

- A and B were measured on `b7a4003` and C on `82e6a1e`. The runs are not on one commit.
- One run per scenario, on shared runners. The 1 Nov write-up recorded a 20–25% Bronze difference between two runs that it could not explain, so small differences here (Gold's 0.5–2.5 s, the idle memory increase) may be runner variance. The Bronze change at volume is far larger than that.
- Scenarios D and the 300-table, 12-domain and Full-preset runs were not repeated.
