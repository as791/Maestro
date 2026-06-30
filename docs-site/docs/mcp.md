---
sidebar_position: 5
title: MCP Server
---

# MCP Server

Cohestra ships an [MCP (Model Context Protocol)](https://modelcontextprotocol.io) server so AI coding assistants — Claude, Cursor, VS Code Copilot, or any MCP-compatible client — can query and operate Flink deployments directly.

## Install

```bash
pip install "mcp[cli]" cohestra-sdk
```

The server lives in `mcp/server.py` in this repository. No separate package needed if you're already working inside the repo.

## Start

```bash
# Stdio transport (default — for Claude Code / Claude Desktop)
COHESTRA_BASE_URL=http://localhost:8080 python3 mcp/server.py

# With bearer token
COHESTRA_BASE_URL=https://cohestra.yourcluster:8080 \
COHESTRA_TOKEN=your-bearer-token \
python3 mcp/server.py

# Interactive inspector (development)
mcp dev mcp/server.py
```

## Wire into Claude Code

Add to `.claude/settings.json` in your project root:

```json
{
  "mcpServers": {
    "cohestra": {
      "command": "python3",
      "args": ["mcp/server.py"],
      "env": {
        "COHESTRA_BASE_URL": "http://localhost:8080"
      }
    }
  }
}
```

Set `COHESTRA_TOKEN` in `env` if your API requires authentication.

## Wire into Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS):

```json
{
  "mcpServers": {
    "cohestra": {
      "command": "python3",
      "args": ["/path/to/cohestra/mcp/server.py"],
      "env": {
        "COHESTRA_BASE_URL": "http://localhost:8080"
      }
    }
  }
}
```

## Available Tools

### Read Operations

| Tool | Description |
|---|---|
| `list_deployments` | List active deployment actors, optionally filtered by environment and namespace |
| `deployment_summary` | All deployments with status, parallelism, health, and last error |
| `describe_deployment` | Full actor state for one deployment |
| `list_deployment_versions` | Version history (spec, savepoint URI, health) |
| `describe_cluster` | Cluster actor state for an env/namespace pair (freeze status) |

### Lifecycle Operations

| Tool | Description |
|---|---|
| `register_deployment` | Start a deployment actor (idempotent) |
| `deploy` | Submit a controlled rollout to a new image and spec |
| `savepoint` | Trigger a savepoint |
| `suspend` | Suspend a running deployment (savepoints first) |
| `resume` | Resume a suspended deployment |
| `rollback` | Roll back to a recorded version |
| `scale` | Change deployment parallelism |

### Cluster Operations

| Tool | Description |
|---|---|
| `freeze_cluster` | Block all runtime mutations in a namespace |
| `unfreeze_cluster` | Remove a namespace freeze |

## Example Prompts

Once the server is connected, you can ask your AI assistant:

```
Show me the status of every deployment in prod/streaming.
```

```
The orders job is lagging. Scale it to parallelism 16, approved, reason "traffic spike".
```

```
Take a savepoint of prod/streaming/orders before the maintenance window.
```

```
Something went wrong with the latest orders deploy. Roll it back.
```

```
Freeze the prod/streaming namespace — we have a P0 incident.
```

## Idempotency Keys

Write operations (`deploy`, `scale`, `rollback`, `suspend`, `resume`, `savepoint`) require an `Idempotency-Key`. The server auto-generates one for each call. Pass an explicit `idempotency_key` argument when you need to retry safely:

```
Deploy orders with idempotency_key="release-v2.3.1-attempt-1"
```

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `COHESTRA_BASE_URL` | `http://localhost:8080` | Control plane base URL |
| `COHESTRA_TOKEN` | _(none)_ | Bearer token for authenticated deployments |
