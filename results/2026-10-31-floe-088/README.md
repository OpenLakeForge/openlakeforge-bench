# 2026-10-31: scenario C on Floe 0.8.8 with parallel entities

OpenLakeForge#705 moved the platform to Floe 0.8.8 and made each domain's Floe
Job run up to 4 entities at once (`orchestration: parallel`,
`max_concurrent_entities: 4`). These runs measure that change against the
Floe 0.8.6 baseline in [2026-10-30-many-tables](../2026-10-30-many-tables/README.md).
They also check whether Trino still refuses a Gold query over 75 tables (OpenLakeForge#706).

Same host and project as the baseline:
- **Runners:** `ubuntu-latest`, 4 vCPU, 15.6 GiB, cgroup v2.
- **Platform:** the Slim preset with one DEV stage.
- **Project:** `many-tables-project` with seed 328 and 1,000 rows per table.
- **Passes:** every product job ran twice.

| File | OpenLakeForge | Tables | Domains (tables each) | Run | Status |
| --- | --- | ---: | ---: | --- | --- |
| [many-tables-t100-7d60fb8.json](many-tables-t100-7d60fb8.json) | `7d60fb8` (#705) | 100 | 4 (25) | [41191045907](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41191045907) | ok |
| [many-tables-t100-cacde6c.json](many-tables-t100-cacde6c.json) | `cacde6c` (#705, #708) | 100 | 4 (25) | [41197726355](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41197726355) | ok |
| [many-tables-t300-4-domains-cacde6c.json](many-tables-t300-4-domains-cacde6c.json) | `cacde6c` (#705, #708) | 300 | 4 (75) | [41197334219](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41197334219) | failed at Gold |
| [many-tables-t300-4-domains-967dd16.json](many-tables-t300-4-domains-967dd16.json) | `967dd16` (#705, #708, #715) | 300 | 4 (75) | [41201832698](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41201832698) | ok |

`many-tables-t100-cacde6c.json` and `many-tables-t300-4-domains-967dd16.json` record each entity's time
and the most that ran at once (`floe_jobs.jobs[].entities`, bench #13).
For the failed 75-table run, those figures are read from the Floe pod log kept in its diagnostics.

## Findings

| | Finding |
| --- | --- |
| Holds | 100 tables passed every check on Floe 0.8.8, on both commits: rows, Gold `mart_table_rows` and every Floe Job exiting 0. |
| Parallel is on | Every 25-table Job ran 4 entities at once (`max_concurrent` 4 in all 8 Jobs). The 75-table Job did too. |
| Small gain | Each entity is about 4 times slower when 4 run together. Median entity time is 16.2–16.4 s, against about 4.8 s when entities ran one after another (120 s / 25). So a 25-table Job went from 120–123 s to 107–109 s (−10%), Silver from 137–142 s to 123–128 s, and the 75-table Job from 371 s to 313 s (−16%). |
| Not CPU | Silver still averages 0.4–0.6 of 4 cores, and Floe Jobs have no CPU limit. Something the four entities share serializes their work. Reading a 1,000-row CSV takes about 20 ms of an entity's 16 s; the rest is the Iceberg write and commit. A likely cause is that PyIceberg's commit runs in Python (Avro manifests and metadata) under one interpreter lock, so threads take turns. This is not yet confirmed. |
| Ceiling lifted | On `cacde6c` the 75-table Gold query still failed with `QUERY_HAS_TOO_MANY_STAGES` (152 > 150). #708 rendered `query.max-stage-count` into `coordinator.additionalConfigProperties`, which the pinned Trino chart (1.42.2) does not read, so the server kept its default. On `967dd16`, #715's dbt session property `query_max_stage_count: 300` alone let Gold pass, with the server still at 150. The session value is not capped by the server's, despite #715's note. 300 tables over 4 domains then passed every check on both runs. OpenLakeForge#721 sets the server value as well. |
| Host noise | Run 41191045907 landed on a slower runner: Trino reads took 7.8–8.1 s per table instead of 4.5–4.9 s, and Bronze 7–8 s instead of 4–5 s. Its Floe Job times (109–112 s) agree with the other run's; its read and Gold times are not comparable. |

## Product runs (per domain)

| Floe | OpenLakeForge | Tables | Dagster run | Bronze | Silver | Floe Job | Silver outside the Job | Gold | Silver CPU (mean) | Peak memory |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0.8.6 | `5a6496d` | 4 × 25 | 196–234 s | 4.0–12.3 s | 137–142 s | 120–123 s | 17–20 s | 16–22 s | 0.5–0.7 | 4.9–5.6 GiB |
| 0.8.8 | `7d60fb8` | 4 × 25 | 221–301 s | 7.3–8.4 s | 137–142 s | 109–112 s | 27–31 s | 21–29 s | 0.6–0.9 | 4.8–5.7 GiB |
| 0.8.8 | `cacde6c` | 4 × 25 | 176–221 s | 3.6–5.2 s | 123–128 s | 107–109 s | 16–20 s | 16–21 s | 0.4–0.6 | 4.9–5.7 GiB |
| 0.8.6 | `5a6496d` | 4 × 75 | 540 s (failed) | 22 s | 452 s | 371 s | 81 s | failed after 27 s | | 5.1 GiB |
| 0.8.8 | `cacde6c` | 4 × 75 | 514 s (failed) | 15 s | 364 s | 313 s | 52 s | failed after 22 s | 0.6 | 5.0 GiB |
| 0.8.8 | `967dd16` | 4 × 75 | 500–622 s | 19–20 s | 379–384 s | 313–318 s | 65–66 s | 22–35 s | 0.6–0.7 | 5.5–6.4 GiB |

## Floe entities (0.8.8)

| Per Job | 25 tables (`cacde6c`) | 75 tables (`967dd16`) |
| --- | ---: | ---: |
| Entities | 25 | 75 |
| Most at once | 4 | 4 |
| Entity time, median | 16.2–16.4 s | 16.3–16.4 s |
| First start to last finish | 105.6–107.7 s | 310.1–314.5 s |
| Job start to first entity | 0.23–0.34 s | 0.43–0.46 s |

Entity time does not grow with the domain: a Job takes about 4.2 s per table at 25 or 75.

## Install and table checks

| | 0.8.6 `5a6496d` | 0.8.8 `cacde6c` |
| --- | ---: | ---: |
| foundation / prefetch / platform | 127 / 147 / 132 s | 130 / 151 / 178 s |
| artifacts (100 tables) | 167 s | 170 s |
| Reading every table back, per pass | 477–501 s | 474–480 s |
| Trino read per table, median | 4.7–4.9 s | 4.5–4.7 s |

At 300 tables over 4 domains (`967dd16`), the artifacts deploy took 215 s, reading all 300 tables back 2,149–2,151 s per pass, and one Trino read 7.1 s (median), against 7.6–7.8 s at 300 over 12 domains on 0.8.6.

## Not measured

- Where an entity's 16 s go inside the write and commit. Floe's events mark only the file read and the entity's start and end.
- `max_concurrent_entities` other than 4, and Floe Jobs with more than one process.
