# 2026-11-01: scenarios A, B and C on main after the Floe Job fix

These runs use OpenLakeForge main `69977b5`, which includes three fixes: #743 (the Floe Job's AWS region, which skips the EC2 metadata lookup), #746 and #747.

They ran on `ubuntu-latest` runners (4 vCPU, 15.6 GiB, cgroup v2) with the Slim preset, in [run 41306555197](https://github.com/OpenLakeForge/openlakeforge-bench/actions/runs/41306555197).

They are compared with the [30 Oct reference](../2026-10-30-github-runner/) on `061f5d1`, which used the same runner type and fixtures. Each ETL scale ran its three products through three steps: initial, repeat and +10% delta.

| File | Scenario |
| --- | --- |
| [footprint.json](footprint.json) | A: install, first pipeline, 300 s idle |
| [etl-x100.json](etl-x100.json), [etl-x10000.json](etl-x10000.json), [etl-x30000.json](etl-x30000.json) | B: end-to-end ETL at ×100, ×10,000 and ×30,000 |
| [many-tables-t100.json](many-tables-t100.json) | C: 100 tables over 4 domains |

## Findings

| | Finding |
| --- | --- |
| Faster | **Silver is no longer a fixed 34–52 s:** it now takes 9.5–18.5 s at every scale. Most of the old cost was the Floe Job pod waiting on the EC2 metadata service once per entity. |
| Faster | **Product runs are 30–35% shorter.** At ×100 they take 61–71 s instead of 91–116 s; one delta took 104 s. At ×10,000, 81–101 s instead of 111–148 s. At ×30,000, 121–156 s instead of 151–244 s. |
| Shift | **Gold is now the slowest layer at ×100** (20.4–23.0 s against Silver's 9.6–13.7 s). At ×10,000 Gold and Bronze take about as long (21–25 s and 17–28 s). Gold's time is flat across scales; Bronze overtakes it at ×30,000. |
| Bottleneck | **Bronze still grows with volume:** 1.0–1.6 s at ×100, 17–28 s at ×10,000 and 50–81 s at ×30,000. At ×30,000 that's about half of each run. It also came out 20–25% faster than on 30 Oct; that difference isn't explained and may be runner variance. |
| Cost | **About 30 s of each product run is outside the three layers:** Dagster's run launch, step launch and the dagster-floe poll. In scenario C, a 25-table Floe Job takes 5–8 s, but its Silver step takes 36–39 s. |
| Holds | **Every scale passed every check.** The idle footprint is unchanged: 0.37 cores and 4.3 GiB mean, against 0.42 cores and 4.2 GiB. |

## Scenario B per layer (min–max over the 9 product runs)

| Scale | Floe | Product run | Bronze | Silver | Gold | Peak memory |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| ×100 | 30 Oct | 91–116 s | 1.1–2.1 s | 34–46 s | 21–24 s | 5.1–5.5 GiB |
| ×100 | main | 61–104 s | 1.0–1.6 s | 9.6–13.7 s | 20.4–23.0 s | 5.2–5.7 GiB |
| ×10,000 | 30 Oct | 111–148 s | 22–37 s | 34–47 s | 22–28 s | 5.9–6.7 GiB |
| ×10,000 | main | 81–101 s | 17–28 s | 9.5–13.2 s | 21.2–25.4 s | 6.1–6.6 GiB |
| ×30,000 | 30 Oct | 151–244 s | 65–105 s | 34–52 s | 23–25 s | 6.2–6.6 GiB |
| ×30,000 | main | 121–156 s | 50–81 s | 11.6–18.5 s | 21.9–24.0 s | 6.4–6.9 GiB |

## Scenario A

| | 30 Oct | main |
| --- | ---: | ---: |
| foundation / prefetch / platform / artifacts | 122 / 134 / 150 / 192 s | 144 / 191 / 162 / 204 s |
| First pipeline (Bronze / Silver / Gold) | 112 s (1.6 / 46.4 / 28.5) | 84 s (1.3 / 13.6 / 26.8) |
| Time to first query | 3.5 s | 3.2 s |
| Idle, 300 s: CPU mean / memory mean | 0.42 cores / 4.2 GiB | 0.37 cores / 4.3 GiB |

Install took 103 s longer on main, mostly prefetch (+57 s) and foundation (+22 s). #743 doesn't touch those phases. This run doesn't show whether the difference is the runner or the newer platform.

## Scenario C

100 tables over 4 domains: the Floe Job takes 5–8 s (median entity 0.34–0.62 s), the Silver step 36–39 s, and a product run 117–141 s, with every check exact on both runs. That matches the #743 measurement in [2026-11-01-incremental](../2026-11-01-incremental/README.md).
