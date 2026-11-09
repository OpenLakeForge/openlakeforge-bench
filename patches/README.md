# Patches

A patch here can be applied to the OpenLakeForge checkout before a benchmark run, to measure a candidate fix on the
same runner without a branch in OpenLakeForge. Start the workflow with `-f openlakeforge_patch=<file name>`, and pin
`openlakeforge_ref` to the commit the patch was made against.

| File | Made against | What it changes |
| --- | --- | --- |
| `in-process-executor.patch` | `0639f85` | Runs a product job's steps in the run's own process instead of one new Python process per step, so the code location is imported once per run, not once per step. |
