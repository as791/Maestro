# ADR-0001: Public library boundary

**Status:** Accepted
**Date:** 2026-06-25
**Deciders:** Maestro maintainers

## Context

Maestro must support community deployments and enterprises with proprietary infrastructure integrations. The original implementation placed all packages under `internal`, preventing reuse, and coupled worker registration to the simulated backend.

## Decision

Publish a vendor-neutral core at `github.com/maestro-flink/maestro`.

- `domain`, `workflows`, `activities`, and `control` are supported public packages.
- `activities.Backend` is the external-integration contract.
- Root registration helpers install stable workflow and activity names.
- Reference binaries and HTTP wiring remain under `cmd` and `internal`.
- Community and proprietary adapters use the same interface without requiring changes to deterministic workflows.

## Options considered

### Public core with adapter interface

Low coupling and a clear compatibility promise, at the cost of maintaining stable contracts.

### Internal monolith

Simpler initially, but unsuitable for embedding, third-party adapters, or enterprise customization.

### Plugin loading

Runtime flexibility, but higher operational and security complexity than compile-time Go interfaces.

## Consequences

- Exported contracts require semantic-versioning discipline.
- Workflow and serialization compatibility must be reviewed separately from Go source compatibility.
- Enterprise adapters can remain private.
- The hosting organization must control the canonical module path before the first tagged release.
