# 2026-11-01: scenario D (incremental RNAcentral) and scenario C with the Floe Job fix

## Setup

Both runs used `ubuntu-latest` runners (4 vCPU, 15.6 GiB, cgroup v2) and the Slim preset with one DEV stage.

| File | OpenLakeForge | Scenario | Run | Status |
| --- | --- | --- | --- | --- |
| [incremental-rna_catalog.json](incremental-rna_catalog.json) | `a1e34db`: main `6f2f1c5` with #747 | D: acme-lakehouse `rna_catalog` | [41302958292](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41302958292) | ok |
| [many-tables-t100-ee35c88.json](many-tables-t100-ee35c88.json) | `ee35c88`: #743 | C: 100 tables over 4 domains | [41291324739](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41291324739) | ok |

**Scenario D's settings:**
- **Project:** acme-lakehouse `5c495f5`.
- **Source:** RNAcentral's public Postgres, loaded by dlt into Bronze. Each run adds 100,000 sequences.
- **Pipeline:** Floe loads `rna_silver.sequences` incrementally. dbt maintains `rna_catalog_gold.mart_sequence_index` as an incremental model, which must mirror Silver row for row.
- **Sequence:** five runs, then a run killed 5 s into Bronze, a retry, and a full refresh from Bronze.

## What it took to get here

Scenario D found three OpenLakeForge bugs, each now fixed:

| Run | Failure | Cause | Fix |
| --- | --- | --- | --- |
| [41289803619](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41289803619) | Run 1, Bronze: `OPENLAKEFORGE_STAGE is not set` | The Dagster module never exported the stage. Only project activation did, and dlt names its pipelines by stage. | OpenLakeForge#746 |
| [41294567178](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41294567178), [41300543632](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41300543632) | Run 2, Gold: `Failed to drop view 'mart_sequence_index__dbt_tmp'` | Trino drops Iceberg views as a purge. Polaris answered 403 `Unable to purge entity` because `DROP_WITH_PURGE_ENABLED` was off. Every dbt incremental model therefore failed from its second run. | OpenLakeForge#747 |
| (scenario C) | Each Floe entity took about 16 s with 4 at once, CPU idle | The Floe Job pod had no AWS region, so the AWS SDK's EC2 metadata lookup timed out on kind | OpenLakeForge#743 |

## Findings

| | Finding |
| --- | --- |
| Holds | **Rows:** every run added exactly 100,000 rows to `rna_silver.sequences`, with as many distinct keys as rows, up to 500,000 after run 5. `mart_sequence_index` matched Silver's row count every time. |
| Holds | **Killed run:** stopped 5 s into Bronze, it left every table's checksum, row count and snapshot count unchanged. The retry then added its 100,000 rows. |
| Holds | **Full refresh from Bronze:** brought Silver back to 100,000 rows. Silver's and Gold's checksums were identical to run 1's, so a refresh reproduces the first load exactly. |
| Cost | **Run time:** an incremental product run takes 71 s; Bronze 9–11 s, Silver 9–11 s and Gold 15–18 s, the rest is Dagster. The first run took 81 s. |
| Cost | **Table checks:** reading back every table's rows, distinct keys, files and snapshots takes 37 s per run. |
| Cost | **Deploy:** about 10.5 minutes (foundation 119 s, prefetch 132 s, platform 187 s, artifacts 190 s). |
| Cost | **Gold files:** `mart_sequence_index` gains one data file and one snapshot per run (5 of each after run 5). Nothing compacts or expires them. |
| Outside | **Live check:** the `food_quality` product failed in Bronze because the Open Food Facts API returned `502 Bad Gateway`. That's an outage on their side. The benchmark records it and goes on. |

## Product runs

| Step | Product run | Bronze | Silver | Gold | Silver rows after |
| --- | ---: | ---: | ---: | ---: | ---: |
| run 1 | 81 s | 10.8 s | 12.5 s | 21.7 s | 100,000 |
| run 2 | 71 s | 9.4 s | 8.8 s | 17.6 s | 200,000 |
| run 3 | 71 s | 9.5 s | 8.8 s | 16.0 s | 300,000 |
| run 4 | 71 s | 9.3 s | 10.7 s | 16.0 s | 400,000 |
| run 5 | 71 s | 9.0 s | 10.7 s | 15.0 s | 500,000 |
| killed 5 s into Bronze | 12.6 s (cancelled) | 6.0 s | – | – | 500,000 |
| retry | 71 s | 9.1 s | 8.9 s | 16.5 s | 600,000 |
| full refresh from Bronze | 71 s | 8.9 s | 9.0 s | 14.0 s | 100,000 |

## Scenario C with #743

Same project and host as [2026-10-31-floe-088](../2026-10-31-floe-088/README.md):

| Per 25-table domain | main `cacde6c` | #743 `ee35c88` |
| --- | ---: | ---: |
| Entity time, median (4 at once) | 16.3 s | 0.30–0.47 s |
| Floe Job | 107–109 s | 4–8 s |
| Silver step | 123–128 s | 25–40 s |
| Dagster run | 176–221 s | 96–121 s |
| Silver CPU, mean | 0.4–0.6 cores | 1.2–1.7 cores |

A domain's Floe Job is now about 20× faster. Silver's remaining 20–30 s is spent outside the Job, in the Dagster step's launch and polling.
