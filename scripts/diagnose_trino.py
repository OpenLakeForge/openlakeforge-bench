"""Why does Trino answer 401 to the harness? Run after `olf infra deploy`, from the OpenLakeForge checkout.

Prints, without any secret value: the contract's Trino settings, the Trino credential Secrets that exist (name,
namespace, keys), whether the Secret `olf` would read exists and is non-empty, and the outcome of one
`SELECT 1` through the harness's session. Exits 0 either way: the output is the result.
"""

from __future__ import annotations

import json
import subprocess
import sys

from olf.infra import trino
from olf.infra.commands._shared import deployment_context
from olf.infra.e2e._shell import E2EError, cfg_environ
from olf.infra.e2e._trino import stage_catalog_name, trino_query

from olf_bench.cli import _session

CLUSTER = "openlakeforge-local"
SETTINGS = (
    "OPENLAKEFORGE_QUERY_TRINO_CATALOG",
    "OPENLAKEFORGE_QUERY_TRINO_CREDENTIAL_SECRET",
    "OPENLAKEFORGE_DBT_TRINO_USER",
    "OPENLAKEFORGE_KUBE_NAMESPACE",
)


def say(title: str, body: object = "") -> None:
    print(f"\n## {title}\n{body}", flush=True)


def kubectl(cfg: object, *args: str) -> str:
    done = subprocess.run(
        ["kubectl", "--context", cfg.kube_context, *args],  # type: ignore[attr-defined]
        capture_output=True,
        text=True,
        check=False,
    )
    return (done.stdout + done.stderr).strip()


def main() -> None:
    context = deployment_context("local", profile="", namespace="", cluster_name=CLUSTER, project_root="")
    with _session(CLUSTER, context.paths.kubeconfig_path)(context.paths.repo_root) as cfg:
        environ = cfg_environ(cfg)
        say(
            "cfg",
            json.dumps(
                {
                    "env": cfg.env,
                    "namespace": cfg.namespace,
                    "platform_namespace": cfg.platform_namespace,
                    "kube_context": cfg.kube_context,
                    "environ_is_contract": cfg.environ is not None,
                },
                indent=1,
            ),
        )
        say("contract settings (names, never passwords)", json.dumps({k: environ.get(k) for k in SETTINGS}, indent=1))
        catalog = stage_catalog_name(cfg)
        target = trino.TrinoTarget(
            provider=cfg.env,
            kube_context=cfg.kube_context,
            namespace=cfg.platform_namespace,
            catalog=catalog,
            stage=trino.stage_from_catalog(catalog),
        )
        wanted = trino.password_secret(target)
        say("Secret olf reads", f"{wanted} in {target.namespace}")
        secrets = kubectl(cfg, "get", "secret", "-A", "-o", "json")
        try:
            items = json.loads(secrets)["items"]
        except (ValueError, KeyError):
            items = []
            say("kubectl get secret -A failed", secrets[:500])
        rows = [
            {
                "namespace": i["metadata"]["namespace"],
                "name": i["metadata"]["name"],
                "keys": {k: len(v) for k, v in (i.get("data") or {}).items()},
            }
            for i in items
            if "trino" in i["metadata"]["name"]
        ]
        say("Trino Secrets (key -> base64 length)", json.dumps(rows, indent=1))
        say("Secret olf reads exists", any(r["name"] == wanted and r["namespace"] == target.namespace for r in rows))
        say("trino_query(cfg, 'SELECT 1')")
        try:
            print(trino_query(cfg, "SELECT 1"))
        except E2EError as exc:
            print(f"FAILED: {exc}")


if __name__ == "__main__":
    sys.exit(main())
