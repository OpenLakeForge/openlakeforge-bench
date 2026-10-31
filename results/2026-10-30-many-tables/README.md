# 2026-10-30: scenario C, many tables, on a GitHub-hosted runner

These results come from OpenLakeForge commit `5a6496d` (Floe 0.8.6 with
dagster-floe 0.4.3, Floe Job polling every 5 s), with the Slim preset and one
DEV stage. They ran on `ubuntu-latest` runners (4 vCPU, 15.6 GiB, cgroup v2)
with nothing overridden. Each project comes from `many-tables-project` with
seed 328 and 1,000 rows per table, and every product job ran twice: a cold run
and a repeat.

| File | Tables | Domains (tables each) | Run | Status |
| --- | ---: | ---: | --- | --- |
| [many-tables-t100.json](many-tables-t100.json) | 100 | 4 (25) | [41091645839](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41091645839) | ok |
| [many-tables-t300-12-domains.json](many-tables-t300-12-domains.json) | 300 | 12 (25) | [41109766146](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41109766146) | ok |
| [many-tables-t300-4-domains.json](many-tables-t300-4-domains.json) | 300 | 4 (75) | [41104937608](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41104937608) | failed at Gold |

Every number below is read from these files. `python -m olf_bench summarize *.json`
renders them as tables.

## Findings

| | Finding |
| --- | --- |
| Holds | 100 and 300 tables loaded with every check passing on both runs: each Silver table held its 1,000 generated rows, and each Gold `mart_table_rows` listed its domain's tables with those counts. Every Floe Job exited 0. |
| Ceiling | A Gold query over about 75 tables fails in Trino. `mart_table_rows` over a 75-table domain needed 152 stages against Trino's default maximum of 150 (`QUERY_HAS_TOO_MANY_STAGES`). Silver for that domain was fine. A product whose Gold model reads that many Silver tables in one query needs splitting, or `query.max-stage-count` raised. |
| Cost | Floe 0.8 starts one Job per domain, and its pod starts within 1 s (the image is already on the node). Inside the Job, tables are written one after another at about 5 s each: 120–130 s for 25 tables and 371 s for 75. Silver averages 0.6–0.8 of 4 cores, so the time is not CPU. |
| Cost | The Silver step spends time outside its Floe Job that grows with the domain and with the total: 17–20 s at 25 tables of 100, 27–36 s at 25 tables of 300, 81 s at 75 tables. |
| Cost | Per-table costs rise with the total table count. At 25 tables a domain, 300 tables against 100 make a product run take 241–271 s instead of 176–188 s, Silver 152–163 s instead of 137–142 s, and reading one table's rows, files and snapshots through Trino 7.6–7.8 s instead of 4.7–4.9 s (median). Deploying the artifacts takes 275 s instead of 167 s. |
| Cost | Peak cluster memory stays at 4.9–6.4 GiB of 15.6 GiB at 300 tables. Product jobs run one at a time, so 300 tables take about 50 minutes per pass. |
| Cost | The repeat run was no faster than the cold run: each overwrite rewrites every table and adds a snapshot. |

## Product runs (per domain)

| Tables | Domains | Dagster run | Bronze | Silver | Floe Job | Gold | Peak memory |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 100 | 4 × 25 | 176–188 s | 4.0–12.3 s | 137–142 s | 120–123 s | 16–22 s | 4.9–5.6 GiB |
| 300 | 12 × 25 | 241–271 s | 6.8–13.7 s | 152–163 s | 124–130 s | 20–31 s | 5.9–6.4 GiB |
| 300 | 4 × 75 | 540 s (failed) | 22 s | 452 s | 371 s | failed after 27 s | 5.1 GiB |

## Install and table checks

| | 100 tables | 300 tables, 12 domains |
| --- | ---: | ---: |
| foundation / prefetch / platform | 127 / 147 / 132 s | 121 / 138 / 186 s |
| artifacts (the generated project) | 167 s | 275 s |
| Reading every table back, per pass | 477–501 s | 2,329–2,391 s |
| Trino read per table, median / max | 4.7–4.9 / 5.6 s | 7.6–7.8 / 9.0 s |

## Not measured

- The per-entity execution that Floe 0.8 replaced: no baseline exists on this host.
- Domains over 75 tables, and tables wider than 11 columns or longer than 1,000 rows.
- Catalog calls are not counted directly; Trino's read time per table stands in for catalog latency.
