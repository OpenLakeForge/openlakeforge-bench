"""The `olf-bench` command: install footprint and end-to-end ETL of a local deployment."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import typer
from olf.common import log
from olf.common.log import fail
from olf.infra.commands._shared import applied_runtime_environment, deployment_context

app = typer.Typer(help="Reproducible benchmarks of a local deployment; each run writes a JSON result.")

_OUTPUT_HELP = "Result directory; defaults to <project-root>/.tmp/bench/<scenario>-<UTC time>."


@app.command("footprint")
def footprint_command(
    clean: bool = typer.Option(False, "--clean", help="Destroy any existing local deployment first."),
    existing: bool = typer.Option(
        False, "--existing", help="Measure the running deployment: no install, start at the first query."
    ),
    settle_seconds: int = typer.Option(120, "--settle-seconds", help="Wait after the first pipeline before idling."),
    idle_seconds: int = typer.Option(300, "--idle-seconds", help="Length of the steady-state sample window."),
    sample_interval: float = typer.Option(5.0, "--sample-interval", help="Seconds between node usage samples."),
    output: str = typer.Option("", "--output", help=_OUTPUT_HELP),
    project_root: str = typer.Option("", "--project-root", help="Project root; defaults to the current directory."),
    cluster_name: str = typer.Option("openlakeforge-local", "--cluster-name", help="Local kind cluster name."),
) -> None:
    """Scenario A: phase-by-phase install, first query, first pipeline, installed and idle footprint."""
    import olf_bench as bench

    if clean and existing:
        raise typer.Exit(code=fail("--clean and --existing are mutually exclusive."))
    _run(
        "footprint",
        project_root,
        cluster_name,
        output,
        lambda record, target, session: bench.footprint(
            record,
            target,
            session=session,
            clean=clean,
            install=not existing,
            settle_seconds=settle_seconds,
            idle_seconds=idle_seconds,
            sample_interval=sample_interval,
        ),
        clean=clean,
        existing=existing,
    )


@app.command("etl")
def etl_command(
    scale: int = typer.Option(100, "--scale", help="Replicas of the CSV fixtures to load."),
    delta: int = typer.Option(10, "--delta", help="Replicas added for the delta reload; 0 skips it."),
    sample_interval: float = typer.Option(2.0, "--sample-interval", help="Seconds between node usage samples."),
    job_timeout_seconds: int = typer.Option(3600, "--job-timeout-seconds", help="Limit for one product job."),
    output: str = typer.Option("", "--output", help=_OUTPUT_HELP),
    project_root: str = typer.Option("", "--project-root", help="Project root; defaults to the current directory."),
    cluster_name: str = typer.Option("openlakeforge-local", "--cluster-name", help="Local kind cluster name."),
) -> None:
    """Scenario B: a scaled dataset through every product job, a no-change rerun and a delta reload."""
    import olf_bench as bench

    _run(
        f"etl-x{scale}",
        project_root,
        cluster_name,
        output,
        lambda record, target, session: bench.etl(
            record, target, session=session, scale=scale, delta=delta, sample_interval=sample_interval
        ),
        job_timeout_seconds=job_timeout_seconds,
        scale=scale,
        delta=delta,
    )


@app.command("incremental")
def incremental_command(
    project: Path = typer.Option(  # noqa: B008
        ..., "--project", help="The project to deploy and load: its root holds lakehouse_code/."
    ),
    product: str = typer.Option(..., "--product", help="The product whose job is run again and again."),
    runs: int = typer.Option(5, "--runs", min=1, help="Incremental runs before the kill and the full refresh."),
    rows_per_run: int = typer.Option(
        100_000, "--rows-per-run", min=0, help="Rows each run adds to an incremental entity; 0 when unbounded."
    ),
    kill_after_seconds: float = typer.Option(
        5.0, "--kill-after-seconds", help="Kill a run this far into its Bronze step, then retry; negative skips it."
    ),
    full_refresh: str = typer.Option("bronze", "--full-refresh", help="Last run's full-refresh scope, or none."),
    live_check: list[str] = typer.Option(  # noqa: B008
        [], "--live-check", help="A product run once, untimed and non-fatal, against a live public source."
    ),
    gold_mirror: list[str] = typer.Option(  # noqa: B008
        [], "--gold-mirror", help="GOLD=SILVER: a Gold table that must hold as many rows as a Silver table."
    ),
    existing: bool = typer.Option(
        False, "--existing", help="Use the running platform, applied from the same profile, instead of installing."
    ),
    sample_interval: float = typer.Option(2.0, "--sample-interval", help="Seconds between node usage samples."),
    job_timeout_seconds: int = typer.Option(3600, "--job-timeout-seconds", help="Limit for one product job."),
    output: str = typer.Option("", "--output", help=_OUTPUT_HELP),
    project_root: str = typer.Option("", "--project-root", help="Project root; defaults to the current directory."),
    cluster_name: str = typer.Option("openlakeforge-local", "--cluster-name", help="Local kind cluster name."),
) -> None:
    """Scenario D: an external project's incremental product, run after run, killed, retried and refreshed."""
    from openlakeforge_domain import FULL_REFRESH_SCOPES

    import olf_bench as bench

    if full_refresh not in (*FULL_REFRESH_SCOPES, "none"):
        raise typer.Exit(code=fail(f"--full-refresh must be one of {', '.join(FULL_REFRESH_SCOPES)} or none."))
    mirrors = dict(pair.split("=", 1) for pair in gold_mirror if "=" in pair)
    if len(mirrors) != len(gold_mirror):
        raise typer.Exit(code=fail("--gold-mirror takes GOLD=SILVER."))
    _run(
        f"incremental-{product}",
        project_root,
        cluster_name,
        output,
        lambda record, target, session: bench.incremental(
            record,
            target,
            session=session,
            project=project.resolve(),
            product_id=product,
            runs=runs,
            rows_per_run=rows_per_run,
            kill_after_seconds=kill_after_seconds if kill_after_seconds >= 0 else None,
            full_refresh=None if full_refresh == "none" else full_refresh,
            live_check=live_check,
            gold_mirrors=mirrors,
            sample_interval=sample_interval,
            install=not existing,
        ),
        job_timeout_seconds=job_timeout_seconds,
        project=str(project),
        product=product,
        runs=runs,
        rows_per_run=rows_per_run,
        kill_after_seconds=kill_after_seconds,
        full_refresh=full_refresh,
        live_check=live_check,
        gold_mirrors=mirrors,
        existing=existing,
    )


@app.command("many-tables")
def many_tables_command(
    tables: int = typer.Option(100, "--tables", min=1, help="Silver tables to generate and load."),
    domains: int = typer.Option(4, "--domains", min=1, help="Domains, and product jobs, to spread them over."),
    rows: int = typer.Option(1000, "--rows", min=1, help="Rows per table."),
    seed: int = typer.Option(328, "--seed", help="Seed of the generated project."),
    runs: int = typer.Option(2, "--runs", min=1, help="Runs of every product job: the first is cold."),
    existing: bool = typer.Option(
        False, "--existing", help="Deploy onto the running DEV-only Slim platform instead of installing one."
    ),
    sample_interval: float = typer.Option(2.0, "--sample-interval", help="Seconds between node usage samples."),
    job_timeout_seconds: int = typer.Option(3600, "--job-timeout-seconds", help="Limit for one product job."),
    output: str = typer.Option("", "--output", help=_OUTPUT_HELP),
    project_root: str = typer.Option("", "--project-root", help="Project root; defaults to the current directory."),
    cluster_name: str = typer.Option("openlakeforge-local", "--cluster-name", help="Local kind cluster name."),
) -> None:
    """Scenario C: a generated project of many tables, deployed and run; per-table and per-Floe-Job costs."""
    import olf_bench as bench

    if tables < domains:
        raise typer.Exit(code=fail("--tables must be at least --domains: every domain needs a table."))
    _run(
        f"many-tables-t{tables}",
        project_root,
        cluster_name,
        output,
        lambda record, target, session: bench.many_tables(
            record,
            target,
            session=session,
            tables=tables,
            domains=domains,
            rows=rows,
            seed=seed,
            runs=runs,
            sample_interval=sample_interval,
            install=not existing,
        ),
        job_timeout_seconds=job_timeout_seconds,
        tables=tables,
        domains=domains,
        rows=rows,
        seed=seed,
        runs=runs,
        existing=existing,
    )


@app.command("many-tables-project")
def many_tables_project_command(
    output: Path = typer.Option(..., "--output", help="Project root to write; replaced if it exists."),  # noqa: B008
    tables: int = typer.Option(100, "--tables", min=1, help="Silver tables to generate."),
    domains: int = typer.Option(4, "--domains", min=1, help="Domains, and products, to spread them over."),
    rows: int = typer.Option(1000, "--rows", min=1, help="Rows per table."),
    seed: int = typer.Option(328, "--seed", help="Changes the generated data; the same seed writes the same files."),
) -> None:
    """Write scenario C's synthetic project of many tables, with expected.json to validate it against."""
    from olf_bench._many_tables import generate

    if tables < domains:
        raise typer.Exit(code=fail("--tables must be at least --domains: every domain needs a table."))
    expected = generate(output, tables=tables, domains=domains, rows=rows, seed=seed)
    log.info(f"{len(expected['tables'])} tables in {expected['domains']} domains written to {output}")


@app.command("summarize")
def summarize_command(
    results: list[Path] = typer.Argument(..., help="result.json files, in the order to show them."),  # noqa: B008
) -> None:
    """Print a Markdown digest of benchmark results, for a CI job summary or an issue comment."""
    from olf_bench._summary import summarize

    typer.echo(summarize(results))


def _run(
    scenario: str,
    project_root: str,
    cluster_name: str,
    output: str,
    body: Callable[..., None],
    *,
    job_timeout_seconds: int = 3600,
    **parameters: object,
) -> None:
    import olf_bench as bench

    context = deployment_context(
        "local", profile="", namespace="", cluster_name=cluster_name, project_root=project_root
    )
    root = context.paths.repo_root
    output_dir = (
        Path(output)
        if output
        else root / ".tmp" / "bench" / f"{scenario}-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
    )
    target = bench.Target(
        project_root=root,
        cluster_name=cluster_name,
        kubeconfig_path=context.paths.kubeconfig_path,
        output_dir=output_dir.resolve(),
        job_timeout_seconds=job_timeout_seconds,
    )
    environment = bench.environment(context.paths.distribution_root)
    environment["parameters"] = {"cluster_name": cluster_name, "preset": context.topology.preset.value, **parameters}
    record = bench.BenchRecord(scenario, environment=environment)
    try:
        body(record, target, _session(cluster_name, context.paths.kubeconfig_path))
    except Exception as exc:  # noqa: BLE001 - every failure is recorded before the run exits non-zero
        log.error(f"{scenario}: {exc}")
        # A check outside any timed step (an unknown product, say) would otherwise leave the run "ok".
        if record.status == "ok":
            record.fail(scenario, str(exc) or type(exc).__name__)
    finally:
        path = record.write(target.output_dir / "result.json")
        for item in record.measurements:
            seconds = "" if item.seconds is None else f"{item.seconds:>9.1f}s"
            log.info(f"  {item.status:<7} {seconds:>10}  {item.name}")
        log.info(f"Result ({record.status}): {path}")
    if record.status != "ok":
        raise typer.Exit(code=fail(f"benchmark {scenario} did not complete; the result records where it stopped."))


def _session(cluster_name: str, kubeconfig_path: Path):  # noqa: ANN202
    from olf.infra.e2e._runner import prepare_config
    from olf.infra.e2e._shell import E2EConfig

    @contextmanager
    def session(project_root: Path) -> Iterator[E2EConfig]:
        context = deployment_context(
            "local",
            profile="",
            namespace="",
            cluster_name=cluster_name,
            kubeconfig_path=str(kubeconfig_path),
            project_root=str(project_root),
        )
        with applied_runtime_environment(context, env="local") as kube_context:
            yield prepare_config(
                "local",
                suite="smoke",
                namespace=context.namespace,
                shared_namespace=context.shared_namespace,
                kube_context=kube_context,
                repo_root=context.paths.repo_root,
                distribution_root=context.paths.distribution_root,
            )

    return session


def main() -> None:
    app()
