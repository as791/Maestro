"""Cohestra MCP server — exposes the control plane as MCP tools."""

from __future__ import annotations

import os
import sys

# Strip the project root from sys.path so the local mcp/ directory does not
# shadow the installed 'mcp' package (Python adds '' = CWD to sys.path when
# running a script, and CWD is usually the project root which contains mcp/).
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path = [p for p in sys.path if os.path.abspath(p or ".") != _project_root]

# Allow running from repo root without installing cohestra-sdk
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "sdk", "python"))

import uuid
from typing import Any

from mcp.server.fastmcp import FastMCP
from cohestra_sdk import CohestraClient
from cohestra_sdk.client import DeploymentSpec, ResourceShape, StateCompatibility

_base_url = os.environ.get("COHESTRA_BASE_URL", "http://localhost:8080")
_token = os.environ.get("COHESTRA_TOKEN")

mcp = FastMCP("cohestra")
_client = CohestraClient(base_url=_base_url, token=_token)


def _key(prefix: str = "mcp") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# ── read operations ────────────────────────────────────────────────────────────

@mcp.tool()
def list_deployments(environment: str = "", namespace: str = "", limit: int = 100) -> dict:
    """List active Flink deployment actors, optionally filtered by environment and namespace."""
    return _client.list_deployments(environment=environment, namespace=namespace, limit=limit)


@mcp.tool()
def deployment_summary() -> list[dict]:
    """Return a health-enriched summary of every active deployment (status, parallelism, health, errors)."""
    return _client.summary()


@mcp.tool()
def describe_deployment(environment: str, namespace: str, name: str) -> dict:
    """Query the full actor state for one deployment."""
    return _client.deployment(environment, namespace, name).status()


@mcp.tool()
def list_deployment_versions(environment: str, namespace: str, name: str) -> list[dict]:
    """List recorded deployment versions (spec, savepoint, health) for one deployment."""
    return _client.deployment(environment, namespace, name).versions()


@mcp.tool()
def describe_cluster(environment: str, namespace: str) -> dict:
    """Query the cluster actor state for an environment/namespace pair (freeze status, etc.)."""
    return _client.cluster(environment, namespace)


# ── deployment lifecycle ───────────────────────────────────────────────────────

@mcp.tool()
def register_deployment(
    environment: str,
    namespace: str,
    name: str,
    owner: str = "",
    service_account: str = "flink",
    node_pool: str = "default",
    flink_dashboard_url: str = "",
) -> dict:
    """Start a deployment actor (idempotent). Call before sending any commands."""
    return _client.register(
        environment, namespace, name,
        owner=owner,
        service_account=service_account,
        node_pool=node_pool,
        flink_dashboard_url=flink_dashboard_url,
    )


@mcp.tool()
def deploy(
    environment: str,
    namespace: str,
    name: str,
    image_digest: str,
    parallelism: int,
    max_parallelism: int,
    flink_version: str = "2.2",
    job_args: dict[str, str] | None = None,
    flink_config: dict[str, str] | None = None,
    task_manager_cpu: float = 1.0,
    task_manager_memory_mib: int = 2048,
    task_manager_count: int = 1,
    slots_per_manager: int = 4,
    autoscaler_enabled: bool = False,
    approved: bool = False,
    incident: bool = False,
    reason: str = "",
    requester: str = "mcp",
    idempotency_key: str = "",
) -> dict:
    """Submit a controlled rollout to a new image/spec. Returns operationId for tracking."""
    spec = DeploymentSpec(
        image_digest=image_digest,
        parallelism=parallelism,
        max_parallelism=max_parallelism,
        flink_version=flink_version,
        resources=ResourceShape(
            task_manager_cpu=task_manager_cpu,
            task_manager_memory_mib=task_manager_memory_mib,
            task_manager_count=task_manager_count,
            slots_per_manager=slots_per_manager,
        ),
        job_args=job_args or {},
        flink_config=flink_config or {},
        autoscaler_enabled=autoscaler_enabled,
    )
    return _client.deployment(environment, namespace, name).deploy(
        spec,
        requester=requester,
        approved=approved,
        incident=incident,
        reason=reason,
        idempotency_key=idempotency_key or _key("deploy"),
    )


@mcp.tool()
def savepoint(
    environment: str,
    namespace: str,
    name: str,
    requester: str = "mcp",
    idempotency_key: str = "",
) -> dict:
    """Request a savepoint for a running deployment."""
    return _client.deployment(environment, namespace, name).savepoint(
        requester=requester,
        idempotency_key=idempotency_key or _key("sp"),
    )


@mcp.tool()
def suspend(
    environment: str,
    namespace: str,
    name: str,
    reason: str = "",
    requester: str = "mcp",
    idempotency_key: str = "",
) -> dict:
    """Suspend a running deployment (takes a savepoint first)."""
    return _client.deployment(environment, namespace, name).suspend(
        requester=requester,
        reason=reason,
        idempotency_key=idempotency_key or _key("suspend"),
    )


@mcp.tool()
def resume(
    environment: str,
    namespace: str,
    name: str,
    requester: str = "mcp",
    idempotency_key: str = "",
) -> dict:
    """Resume a suspended deployment from its last savepoint."""
    return _client.deployment(environment, namespace, name).resume(
        requester=requester,
        idempotency_key=idempotency_key or _key("resume"),
    )


@mcp.tool()
def rollback(
    environment: str,
    namespace: str,
    name: str,
    target_version: int = 0,
    reason: str = "",
    approved: bool = True,
    requester: str = "mcp",
    idempotency_key: str = "",
) -> dict:
    """Roll back to a recorded version. target_version=0 rolls back to the previous version."""
    return _client.deployment(environment, namespace, name).rollback(
        target_version=target_version,
        requester=requester,
        approved=approved,
        reason=reason,
        idempotency_key=idempotency_key or _key("rollback"),
    )


@mcp.tool()
def scale(
    environment: str,
    namespace: str,
    name: str,
    parallelism: int,
    reason: str = "",
    approved: bool = False,
    requester: str = "mcp",
    idempotency_key: str = "",
) -> dict:
    """Scale a deployment to a new parallelism."""
    return _client.deployment(environment, namespace, name).scale(
        parallelism=parallelism,
        requester=requester,
        approved=approved,
        reason=reason,
        idempotency_key=idempotency_key or _key("scale"),
    )


# ── cluster operations ─────────────────────────────────────────────────────────

@mcp.tool()
def freeze_cluster(
    environment: str,
    namespace: str,
    reason: str = "",
    requester: str = "mcp",
) -> dict:
    """Freeze all runtime mutations in a namespace (blocks deploy/scale/resume)."""
    return _client.freeze_cluster(environment, namespace, requester=requester, reason=reason)


@mcp.tool()
def unfreeze_cluster(
    environment: str,
    namespace: str,
    reason: str = "",
    requester: str = "mcp",
) -> dict:
    """Remove a namespace freeze, re-enabling runtime operations."""
    return _client.unfreeze_cluster(environment, namespace, requester=requester, reason=reason)


if __name__ == "__main__":
    mcp.run()
