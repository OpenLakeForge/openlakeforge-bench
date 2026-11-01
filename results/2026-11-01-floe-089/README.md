# 2026-11-01: Floe 0.8.9 (scenarios A, B at GB scale, C and D)

These runs use OpenLakeForge `824820b`, which is main plus the bump to Floe 0.8.9 (OpenLakeForge#749). Floe 0.8.9 fixes the per-entity costs reported in floe#1129.

They ran on `ubuntu-latest` runners (4 vCPU, 15.6 GiB) with the Slim preset, in [run 41318305780](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41318305780).

| File | Scenario |
| --- | --- |
| [many-tables-t100.json](many-tables-t100.json) | C: 100 tables over 4 domains |
| [many-tables-t300.json](many-tables-t300.json) | C: 300 tables over 4 domains |
| [many-tables-t300-12-domains.json](many-tables-t300-12-domains.json) | C: 300 tables over 12 domains, from [run 41318362081](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41318362081) |
| [incremental-rna_catalog.json](incremental-rna_catalog.json) | D: acme-lakehouse `rna_catalog` |
| [footprint.json](footprint.json) | A: install, first pipeline, 300 s idle |
| [etl-x100000.json](etl-x100000.json) | B at ×100,000: 13.8M rows, 0.82 GB of CSV; the delta step adds 10% (0.90 GB) |
| [etl-x300000.json](etl-x300000.json) | B at ×300,000: 41.4M rows, 2.5 GB of CSV. **failed** on the repeat step |

## Scenario C, per 25- or 75-table domain

| | 100 tables | 300 tables |
| --- | ---: | ---: |
| Floe Job | 4–7 s | 7–9 s |
| Entity time, median (4 at once) | 0.26–0.43 s | 0.23–0.31 s |
| Dagster run | 68–77 s | 121–124 s |
| Bronze / Silver / Gold step | 5.0–5.4 / 24–28 / 18–25 s | 14.7–15.4 / 57–59 / 19.4–20.6 s |
| Peak memory | 4.7–5.6 GiB | 5.7–6.1 GiB |
| Table checks (every table read back) | — | 1810–1813 s |

**Holds:** every check is exact on both runs, at both sizes.

**300 tables over 12 domains (25 per domain, Slim):** passed on both runs.
- Per domain: Dagster run 76–87 s; Bronze 3.9–9.9 s, Silver 18.5–24.7 s, Gold 15.8–22.1 s.
- Floe: Job 3–8 s, median entity 0.20–0.30 s.
- Peak memory: 5.1–6.4 GiB.
- Twelve domains cost no more per domain than four, because the domains run one after another.

**Silver scaling:** three times the tables cost about twice the Silver step. The Floe Job itself is under 10 s; the rest of the Silver step is Dagster's step launch and the dagster-floe poll.

## Scenario D

**Result:** same behaviour as on [2026-11-01-incremental](../2026-11-01-incremental/README.md):
- Each of the 5 runs added exactly 100,000 rows.
- The run killed in Bronze changed nothing.
- The full refresh reproduced run 1's Silver (`B1C73F01E0F7DAF4`) and Gold (`C7DFF4B1D12DA3AA`) checksums.

| Step | Dagster run | Bronze | Silver | Gold |
| --- | ---: | ---: | ---: | ---: |
| run 1 | 65.6 s | 10.2 s | 13.1 s | 21.3 s |
| runs 2–5, retry | 54–61 s | 9.8–12.2 s | 8.8–11.4 s | 15.1–17.9 s |
| full refresh from Bronze | 56.2 s | 11.9 s | 9.0 s | 14.7 s |

## Scenario B at GB scale

**×100,000:** every step passed: initial, repeat and +10% delta.

**×300,000:**
- **Initial:** passed for all three products.
- **Repeat:** `order_revenue`'s Gold step failed. The Trino coordinator was OOM-killed at its 3Gi limit while it ran `mart_order_revenue_by_channel`, and dbt got `404 Query not found`. Filed as OpenLakeForge#755.
- **Not run:** the rest of the scale was skipped after the failure.

| Scale | Step | Product run | Bronze | Silver | Gold | Peak memory |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| ×100,000 | initial, repeat | 286–404 s | 219–322 s | 16–24 s | 28–40 s | 5.7–7.4 GiB |
| ×100,000 | +10% delta | 308–428 s | 237–352 s | 19–27 s | 30–33 s | 6.0–7.0 GiB |
| ×300,000 | initial | 761–1104 s | 654–970 s | 41–65 s | 42–51 s | 6.8–9.3 GiB |
| ×300,000 | repeat `order_revenue` | failed | 953 s | 61 s | failed at 79 s | — |

**What it shows:**
- **Bronze is 75–88% of every run.** The reference CSV loader moves about 1 MB/s on about one core (1.5 cores in use across the cluster). It holds every row in memory, which is why peak memory reaches 9.3 GiB at ×300,000. Filed as OpenLakeForge#756.
- **Silver scales.** Floe 0.8.9 loads 2.5 GB in 41–65 s per product, using 3.1–3.3 cores, about 15 MB/s.
- **Gold fits until the coordinator runs out of room.** 42–51 s at ×300,000, but the Slim coordinator's 1Gi of non-heap headroom over its 2G heap isn't enough for a rebuild over 3.6M orders and 6.3M order lines.

## Scenario A

| | |
| ---: | --- |
| foundation / prefetch / platform / artifacts | 122 / 137 / 186 / 200 s (646 s in total) |
| First pipeline (Dagster run; Bronze / Silver / Gold) | 66 s (2.0 / 16.8 / 28.0 s) |
| Time to first query | 3.0 s |
| Idle, 300 s: CPU mean / memory mean | 0.38 cores / 4.1 GiB |
