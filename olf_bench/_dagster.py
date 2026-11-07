"""A Dagster GraphQL client for the stage's webserver, reached the way `olf e2e` reaches it."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from olf.infra import access, k8s
from olf.infra.contracts import CONTRACT_STAGE_ENV
from olf.infra.e2e._dagster import DAGSTER_WEBSERVER_SERVICE_NAME, _graphql_over, expected_repository_location_names
from olf.infra.e2e._shell import E2EConfig, E2EError, cfg_environ
from openlakeforge_services.dagster import DagsterClient


@contextmanager
def dagster_client(cfg: E2EConfig) -> Iterator[DagsterClient]:
    """A client for the stage's Dagster webserver, reachable for the duration of the block."""
    assert cfg.dagster_local_port is not None
    with access.service_route(
        f"stage/{cfg_environ(cfg).get(CONTRACT_STAGE_ENV, 'dev')}/orchestration",
        service=DAGSTER_WEBSERVER_SERVICE_NAME,
        remote_port=80,
        namespace=cfg.namespace,
        local_port=cfg.dagster_local_port,
        log_path=f"/tmp/openlakeforge-{cfg.env}-bench-dagster-port-forward.log",
        shared_namespace=cfg.platform_namespace,
        kube_context=cfg.kube_context,
        environ=cfg_environ(cfg),
    ) as route:
        if not k8s.http_wait(f"{route.url}/server_info", attempts=90, delay=2, session=route.session()):
            raise E2EError("Dagster endpoint did not become reachable.")
        yield DagsterClient(
            f"{route.url}/graphql",
            expected_location_names=expected_repository_location_names(cfg),
            request_json=_graphql_over(route),
        )
