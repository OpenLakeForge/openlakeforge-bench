# 2026-10-29: Slim on a 4-vCPU cloud VM

These are the first results for scenarios A and B. They come from OpenLakeForge
commit `f091809` (olf 0.5.0a1, Slim preset, one DEV stage), run on a
firecracker VM with 4 vCPU, 15.7 GiB, cgroup v1, and kind with three nodes
on Kubernetes 1.36.1. The host needed the workarounds under
[How far to trust these numbers](#how-far-to-trust-these-numbers): treat them
as development measurements, not the published baseline. Every number below is
read from the JSON files in this directory.

## Findings

| | Finding |
| --- | --- |
| Ceiling | At 10,000 fixture replicas (1.38M rows, 79 MB of CSV), the SeaweedFS S3 gateway (`limits.memory: 256Mi`) was OOM-killed on the 4th or 5th consecutive product run in **both** attempts. Each kill failed a run: a Bronze upload in attempt 1 and a Floe Silver step in attempt 2. Cluster memory rose from 7.6 to 8.4 GiB run over run. At 100 replicas the same jobs ran nine times without a restart. |
| Cost | A clean Slim install takes about 16 minutes, mostly prefetch (435–471 s) and platform (212–241 s). The node image stores held 19.9 GB, because kind keeps a copy of every image in each of its three nodes; persistent volumes held 77 MB. |
| Cost | At idle the cluster uses 0.68 cores on average (p95 1.15) and 5.76 GiB, with 28 containers running in 44 pods. |
| Cost | At 14 thousand rows a product job takes 59–70 s. About 25 s of that is run-pod start-up and step hand-offs; Silver and Gold take about 20 s each, Bronze about 1.5 s. |
| Cost | At 1.4M rows Bronze becomes the slowest layer (21–38 s). The CSV loader holds every row in memory and uploads one object per entity. Silver barely moves (19–27 s); 79 MB of CSV becomes 6.9 MB of Iceberg in 15 files. |
| Cost | Every overwrite run adds two Iceberg snapshots per table, and nothing expires them: some tables held 14 snapshots after eight runs. |
| Holds | Silver rows equalled the generated input at every scale (13,800, 15,180 and 1,380,000), and a no-change rerun reproduced every table's checksum. |

## Scenario A: install and idle

| Phase | Run 3 | Run 4 | Peak CPU | Peak memory |
| --- | ---: | ---: | ---: | ---: |
| foundation (kind cluster) | 38.5 s | 36.7 s | – | – |
| prefetch (images into 3 nodes) | 471.3 s | 434.8 s | 2.8 cores | 1.5 GiB |
| platform (Terraform and Helm) | 212.5 s | 240.8 s | 3.3 cores | 3.6 GiB |
| artifacts (project image, Dagster) | 260.7 s | 263.0 s | – | – |
| install total | 983 s | 975 s | | |

The artifacts phase came from two standalone runs with a cold build cache
(`artifacts-1.json`, `artifacts-2.json`): the in-harness attempts had failed
on host problems that were fixed afterwards.

| After install (`footprint-6.json`) | Value |
| --- | ---: |
| Time to first Trino query | 2.5 s |
| First pipeline (`order_revenue`, fixtures), Dagster run | 73.6 s |
| Bronze / Silver / Gold | 1.6 / 21.5 / 24.4 s |
| Pods: running / completed jobs / total | 28 / 15 / 44 |
| Node image stores, 3 nodes | 19.9 GB |
| Persistent volumes | 77 MB |
| Memory, installed / steady state | 5.79 / 5.76 GiB |
| CPU at steady state, mean / p95 / max | 0.68 / 1.15 / 1.57 cores |

## Scenario B: end-to-end ETL

| Scale | Run | Runs ok | Run time per product | Peak cluster memory | Validity |
| --- | --- | ---: | ---: | ---: | --- |
| ×100 | initial | 3/3 | 59–70 s | 6.7–7.0 GiB | Silver 13,800 = 13,800 |
| ×100 | no-change rerun | 3/3 | 65–67 s | 7.1 GiB | Every checksum identical |
| ×110 | delta reload | 3/3 | 64–66 s | 7.2–7.3 GiB | Silver 15,180 = 15,180 |
| ×10,000 | initial, attempt 1 | 3/3 | 91–123 s | 7.6–7.8 GiB | Silver 1,380,000 = 1,380,000 |
| ×10,000 | rerun, attempt 1 | 2/3 | 84–100 s | 8.0–8.2 GiB | Bronze upload failed: S3 gateway OOM |
| ×10,000 | initial, attempt 2 | 3/3 | 83–101 s | 8.1–8.3 GiB | Silver 1,380,000 = 1,380,000 |
| ×10,000 | rerun, attempt 2 | 1/3 | 98 s | 8.4 GiB | Floe Silver failed: S3 gateway OOM |
| ×11,000 | delta reload | – | – | – | Not reached |

## Not measured

- DuckDB Slim (OpenLakeForge/openlakeforge#547): not validated on this host.
- Full preset: no disk room, since Slim used about 36 of this session's
  roughly 39 GB.
- Scenario C and the restart, failure and retry cases.
- An incremental merge: every Silver entity is configured
  `incremental_mode: none`.

## How far to trust these numbers

The local stack would not start on this host unchanged. Every workaround is
recorded in the results: the kind config hash, image overrides and modified
files.

- **cgroup v1 host.** The kubelet in `kindest/node:v1.36.1` refuses cgroup v1.
  `kind-cluster-bench-vm.yaml` sets `failCgroupV1: false`.
- **No CAP_SYS_RESOURCE.** Without it no pod sandbox could start, so containerd
  runs with `restrict_oom_score_adj = true`.
- **No CONFIG_NFT_QUEUE in the kernel.** kindnet cannot enforce NetworkPolicy,
  and the ADR 0017 canary blocks the deploy. For runs 3 and 4 the canary was
  skipped by an uncommitted local edit, so policies existed but were not
  enforced.
- **A TLS-intercepting egress proxy.** The workarounds were a forwarder on the
  kind bridge, the proxy CA mounted into the nodes, `docker.io` mirrored through
  `mirror.gcr.io` (anonymous Docker Hub pulls returned 429), and a project-code
  base image built from the pinned `python:3.12-slim` digest plus the proxy CA,
  used for build-time pip only.
- **A disk allowance of about 39 GB.** Images no container used were pruned
  from the nodes between ETL cycles, as kubelet's image GC does under disk
  pressure.

Next: the same three commands on a GitHub-hosted runner, where the platform's
`Deploy slim kind smoke` job already installs Slim in about 14 minutes.
