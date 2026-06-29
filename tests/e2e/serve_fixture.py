#!/usr/bin/env python3
"""Serve the Operations Console with deterministic API data for visual checks."""

import json
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[2]
WEB = ROOT / "internal" / "api" / "web"
IDENTITY = {"environment": "prod", "namespace": "streaming", "name": "orders"}
HEALTH = {
    "healthy": True,
    "message": "All health gates passed",
    "running": True,
    "checkpointCompleted": True,
    "sinkHealthy": True,
    "restartCount": 0,
    "backpressureRatio": 0.08,
    "kafkaLag": 124,
}
SPEC = {
    "imageDigest": "registry.example.com/orders@sha256:8be2a97f42b6c89e",
    "flinkVersion": "2.2",
    "parallelism": 8,
    "maxParallelism": 64,
    "resources": {"taskManagerCount": 2, "slotsPerManager": 4},
}


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB, **kwargs)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            self.send_response(302)
            self.send_header("Location", "/ui/")
            self.end_headers()
            return
        if path.startswith("/api/v1/"):
            self._api(path)
            return
        if path.startswith("/ui/"):
            self.path = path.removeprefix("/ui") or "/"
        super().do_GET()

    def _api(self, path):
        if path == "/api/v1/deployments":
            body = {"deployments": [{"identity": IDENTITY, "startedAt": "2026-06-29T07:30:00Z"}]}
        elif path == "/api/v1/deployments/summary":
            body = [{
                "identity": IDENTITY,
                "status": "IDLE",
                "healthy": True,
                "version": 12,
                "parallelism": 8,
                "imageDigest": SPEC["imageDigest"],
                "pendingOperations": 0,
            }]
        elif path.endswith("/actor") and path.startswith("/api/v1/deployments/"):
            body = {
                "identity": IDENTITY,
                "status": "IDLE",
                "currentVersion": {"versionId": 12, "spec": SPEC, "healthSummary": HEALTH},
                "pendingOperations": 0,
                "autoscalerEnabled": True,
                "autoscalerFrozen": False,
                "recentOperations": [{
                    "operationId": "deploy-12",
                    "commandType": "DEPLOY",
                    "status": "SUCCEEDED",
                    "result": "Version 12 deployed",
                    "completedAt": "2026-06-29T07:32:00Z",
                }],
            }
        elif path.startswith("/api/v1/clusters/") and path.endswith("/actor"):
            body = {"frozen": False}
        else:
            self.send_error(404)
            return
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 4173), Handler).serve_forever()
