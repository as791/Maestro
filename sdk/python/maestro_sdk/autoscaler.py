"""Base class for building custom Maestro autoscalers."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass

from maestro_sdk.client import MaestroClient, Deployment

logger = logging.getLogger("maestro.autoscaler")


@dataclass
class ScaleDecision:
    target_parallelism: int
    reason: str
    cooldown_seconds: float = 300


class AutoscalerBase(ABC):
    """Subclass this to build a custom autoscaler (Lambda, CronJob, etc)."""

    def __init__(self, client: MaestroClient, env: str, namespace: str, name: str):
        self.client = client
        self.deployment = client.deployment(env, namespace, name)
        self.env = env
        self.namespace = namespace
        self.name = name

    @abstractmethod
    def evaluate(self, status: dict) -> ScaleDecision | None:
        """Return a ScaleDecision if scaling is needed, None to hold."""
        ...

    def execute(self) -> dict | None:
        """Run one evaluation cycle. Returns the scale response or None."""
        status = self.deployment.status()

        if status.get("status") not in ("IDLE", "RUNNING"):
            logger.info("deployment status=%s, skipping", status.get("status"))
            return None

        current = status.get("currentVersion")
        if not current:
            logger.info("no current version, skipping")
            return None

        health = current.get("healthSummary", {})
        if not health.get("healthy"):
            logger.info("deployment not healthy, skipping")
            return None

        decision = self.evaluate(status)
        if decision is None:
            logger.info("no scaling needed")
            return None

        current_parallelism = current["spec"]["parallelism"]
        if decision.target_parallelism == current_parallelism:
            logger.info("already at target parallelism=%d", current_parallelism)
            return None

        logger.info(
            "scaling %s/%s/%s: %d → %d (%s)",
            self.env, self.namespace, self.name,
            current_parallelism, decision.target_parallelism, decision.reason,
        )
        return self.deployment.scale(
            decision.target_parallelism,
            requester="autoscaler",
            approved=True,
            reason=decision.reason,
        )

    def run_loop(self, interval: float = 60):
        """Run the autoscaler in a loop (for long-running processes)."""
        logger.info("starting autoscaler loop for %s/%s/%s", self.env, self.namespace, self.name)
        while True:
            try:
                self.execute()
            except Exception:
                logger.exception("autoscaler cycle failed")
            time.sleep(interval)
