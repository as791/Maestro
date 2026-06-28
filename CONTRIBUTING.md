# Contributing to Maestro

Maestro accepts community and enterprise contributions under the Apache License 2.0.

## Before opening a change

- Use an issue for public API changes, workflow-state changes, new adapters, or behavioral changes.
- Keep vendor-specific integrations outside the core unless they define a generally reusable contract.
- Never add credentials, customer manifests, production event histories, or proprietary data.

## Development

```bash
go test -race ./...
go vet ./...
go build ./...
docker compose config --quiet
```

Pull requests should include tests, documentation, and a compatibility assessment. Commits are accepted under the Developer Certificate of Origin; add a `Signed-off-by` line using `git commit -s`.

## Compatibility requirements

- Exported Go APIs follow semantic versioning.
- Temporal workflow names, activity names, signal/query names, workflow IDs, serialized fields, and deterministic decisions are compatibility-sensitive.
- Workflow changes must include replay or workflow-environment tests.
- Breaking changes require an accepted ADR and a new major module version.

See [API compatibility](docs/api-compatibility.md) and [governance](GOVERNANCE.md).
