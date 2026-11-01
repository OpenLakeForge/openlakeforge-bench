# 2026-11-01: Floe 0.8.9 (scenarios C and D)

These runs use OpenLakeForge `824820b`, which is main plus the bump to Floe 0.8.9 (OpenLakeForge#749). Floe 0.8.9 fixes the per-entity costs reported in floe#1129.

They ran on `ubuntu-latest` runners (4 vCPU, 15.6 GiB) with the Slim preset, in [run 41318305780](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41318305780).

Scenarios A and B at ×100,000 and ×300,000 from the same run follow in a separate results folder.

| File | Scenario |
| --- | --- |
| [many-tables-t100.json](many-tables-t100.json) | C: 100 tables over 4 domains |
| [many-tables-t300.json](many-tables-t300.json) | C: 300 tables over 4 domains |
| [incremental-rna_catalog.json](incremental-rna_catalog.json) | D: acme-lakehouse `rna_catalog` |

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
