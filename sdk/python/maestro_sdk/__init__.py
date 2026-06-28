"""Maestro SDK — Python client for the Flink Control Plane."""

from maestro_sdk.client import MaestroClient, Deployment, DeploymentSpec, ResourceShape, StateCompatibility
from maestro_sdk.autoscaler import AutoscalerBase

__version__ = "0.1.0"
__all__ = [
    "MaestroClient",
    "Deployment",
    "DeploymentSpec",
    "ResourceShape",
    "StateCompatibility",
    "AutoscalerBase",
]
