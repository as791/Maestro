"""Core Maestro client — thin wrapper over the REST API."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Any, Optional
from urllib.parse import urljoin

import requests


@dataclass
class ResourceShape:
    task_manager_cpu: float = 1.0
    task_manager_memory_mib: int = 2048
    task_manager_count: int = 1
    slots_per_manager: int = 4

    def to_dict(self) -> dict:
        return {
            "taskManagerCpu": self.task_manager_cpu,
            "taskManagerMemoryMiB": self.task_manager_memory_mib,
            "taskManagerCount": self.task_manager_count,
            "slotsPerManager": self.slots_per_manager,
        }


@dataclass
class StateCompatibility:
    job_graph_compatible: bool = True
    operator_uids_stable: bool = True
    allow_non_restored: bool = False
    fresh_start_approved: bool = False

    def to_dict(self) -> dict:
        return {
            "jobGraphCompatible": self.job_graph_compatible,
            "operatorUidsStable": self.operator_uids_stable,
            "allowNonRestored": self.allow_non_restored,
            "freshStartApproved": self.fresh_start_approved,
        }


@dataclass
class DeploymentSpec:
    image_digest: str
    parallelism: int
    max_parallelism: int
    flink_version: str = "2.2"
    resources: ResourceShape = field(default_factory=ResourceShape)
    state_compatibility: StateCompatibility = field(default_factory=StateCompatibility)
    job_args: dict[str, str] = field(default_factory=dict)
    flink_config: dict[str, str] = field(default_factory=dict)
    autoscaler_enabled: bool = False

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "imageDigest": self.image_digest,
            "flinkVersion": self.flink_version,
            "parallelism": self.parallelism,
            "maxParallelism": self.max_parallelism,
            "resources": self.resources.to_dict(),
            "stateCompatibility": self.state_compatibility.to_dict(),
            "autoscalerEnabled": self.autoscaler_enabled,
        }
        if self.job_args:
            d["jobArgs"] = self.job_args
        if self.flink_config:
            d["flinkConfig"] = self.flink_config
        return d


def _idempotency_key(prefix: str = "sdk") -> str:
    ts = time.strftime("%Y%m%d%H%M%S")
    return f"{prefix}-{ts}-{uuid.uuid4().hex[:8]}"


class Deployment:
    """Handle for one Maestro-managed deployment."""

    def __init__(self, client: MaestroClient, env: str, namespace: str, name: str):
        self._c = client
        self._env = env
        self._ns = namespace
        self._name = name

    @property
    def _base(self) -> str:
        return f"/api/v1/deployments/{self._env}/{self._ns}/{self._name}"

    # — queries —

    def status(self) -> dict:
        return self._c._get(f"{self._base}/actor")

    def versions(self) -> list[dict]:
        return self._c._get(f"{self._base}/versions")

    # — commands —

    def deploy(
        self,
        spec: DeploymentSpec,
        *,
        requester: str = "sdk",
        approved: bool = False,
        incident: bool = False,
        reason: str = "",
        idempotency_key: str | None = None,
    ) -> dict:
        return self._c._post(
            f"{self._base}/deploy",
            body={
                "requester": requester,
                "approved": approved,
                "incident": incident,
                "reason": reason,
                "spec": spec.to_dict(),
            },
            key=idempotency_key or _idempotency_key("deploy"),
        )

    def scale(
        self,
        parallelism: int,
        *,
        requester: str = "sdk",
        approved: bool = False,
        reason: str = "",
        idempotency_key: str | None = None,
    ) -> dict:
        return self._c._post(
            f"{self._base}/scale",
            body={"requester": requester, "parallelism": parallelism, "approved": approved, "reason": reason},
            key=idempotency_key or _idempotency_key("scale"),
        )

    def savepoint(self, *, requester: str = "sdk", idempotency_key: str | None = None) -> dict:
        return self._c._post(
            f"{self._base}/savepoint",
            body={"requester": requester},
            key=idempotency_key or _idempotency_key("sp"),
        )

    def suspend(self, *, requester: str = "sdk", reason: str = "", idempotency_key: str | None = None) -> dict:
        return self._c._post(
            f"{self._base}/suspend",
            body={"requester": requester, "reason": reason},
            key=idempotency_key or _idempotency_key("suspend"),
        )

    def resume(self, *, requester: str = "sdk", idempotency_key: str | None = None) -> dict:
        return self._c._post(
            f"{self._base}/resume",
            body={"requester": requester},
            key=idempotency_key or _idempotency_key("resume"),
        )

    def rollback(
        self,
        target_version: int = 0,
        *,
        requester: str = "sdk",
        approved: bool = True,
        reason: str = "",
        idempotency_key: str | None = None,
    ) -> dict:
        return self._c._post(
            f"{self._base}/rollback",
            body={"requester": requester, "targetVersion": target_version, "approved": approved, "reason": reason},
            key=idempotency_key or _idempotency_key("rollback"),
        )

    def enable_autoscaler(self, *, requester: str = "sdk", idempotency_key: str | None = None) -> dict:
        return self._c._post(
            f"{self._base}/autoscaler/enable",
            body={"requester": requester},
            key=idempotency_key or _idempotency_key("autoscaler-enable"),
        )

    def freeze_autoscaler(self, *, requester: str = "sdk", idempotency_key: str | None = None) -> dict:
        return self._c._post(
            f"{self._base}/autoscaler/freeze",
            body={"requester": requester},
            key=idempotency_key or _idempotency_key("autoscaler-freeze"),
        )

    # — polling helpers —

    def wait_healthy(self, timeout: float = 300, poll: float = 5) -> dict:
        """Poll until the deployment reports healthy or timeout."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            view = self.status()
            health = (view.get("currentVersion") or {}).get("healthSummary", {})
            if health.get("healthy"):
                return view
            if view.get("status") == "FAILED":
                raise RuntimeError(f"deployment failed: {view.get('lastError')}")
            time.sleep(poll)
        raise TimeoutError(f"deployment not healthy after {timeout}s")


class MaestroClient:
    """Maestro control plane client."""

    def __init__(self, base_url: str = "http://localhost:8080", token: str | None = None, timeout: float = 30):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        if token:
            self._session.headers["Authorization"] = f"Bearer {token}"
        self._session.headers["Content-Type"] = "application/json"

    # — public API —

    def healthz(self) -> dict:
        return self._get("/healthz")

    def list_deployments(self, *, environment: str = "", namespace: str = "", limit: int = 100) -> dict:
        params: dict[str, Any] = {"limit": limit}
        if environment:
            params["environment"] = environment
        if namespace:
            params["namespace"] = namespace
        return self._get("/api/v1/deployments", params=params)

    def summary(self) -> list[dict]:
        return self._get("/api/v1/deployments/summary")

    def deployment(self, env: str, namespace: str, name: str) -> Deployment:
        return Deployment(self, env, namespace, name)

    def register(
        self,
        env: str,
        namespace: str,
        name: str,
        *,
        owner: str = "",
        service_account: str = "flink",
        node_pool: str = "default",
        flink_dashboard_url: str = "",
    ) -> dict:
        body: dict[str, Any] = {
            "owner": owner,
            "serviceAccount": service_account,
            "nodePool": node_pool,
        }
        if flink_dashboard_url:
            body["flinkDashboardUrl"] = flink_dashboard_url
        return self._put(f"/api/v1/deployments/{env}/{namespace}/{name}", body)

    def cluster(self, env: str, namespace: str) -> dict:
        return self._get(f"/api/v1/clusters/{env}/{namespace}/actor")

    def freeze_cluster(self, env: str, namespace: str, *, requester: str = "sdk", reason: str = "") -> dict:
        return self._post(
            f"/api/v1/clusters/{env}/{namespace}/freeze",
            body={"requester": requester, "reason": reason},
        )

    def unfreeze_cluster(self, env: str, namespace: str, *, requester: str = "sdk", reason: str = "") -> dict:
        return self._post(
            f"/api/v1/clusters/{env}/{namespace}/unfreeze",
            body={"requester": requester, "reason": reason},
        )

    # — internals —

    def _get(self, path: str, params: dict | None = None) -> Any:
        r = self._session.get(urljoin(self.base_url, path), params=params, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _put(self, path: str, body: dict) -> Any:
        r = self._session.put(urljoin(self.base_url, path), json=body, timeout=self.timeout)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, body: dict, key: str | None = None) -> Any:
        headers = {}
        if key:
            headers["Idempotency-Key"] = key
        r = self._session.post(urljoin(self.base_url, path), json=body, headers=headers, timeout=self.timeout)
        r.raise_for_status()
        return r.json()
