# API and workflow compatibility

Maestro follows semantic versioning after `v1.0.0`. Before `v1`, minor releases may contain breaking changes, which must be called out in release notes.

## Stable surface

- Exported types and functions in the root, `activities`, `control`, `domain`, and `workflows` packages.
- HTTP paths and schemas under `/api/v1`.
- Temporal workflow and activity names.
- Signal and query names.
- Workflow ID formats.
- Serialized workflow state and command fields.

## Compatibility rules

- Additive fields must remain safe when absent during replay.
- Existing activity implementations must continue compiling within a major version.
- Workflow code must remain deterministic for existing histories.
- Renames require aliases or migration tooling.
- Workflow behavior changes use Temporal version gates where replay can diverge.
- Deprecated APIs remain available until the next major version unless removal addresses an actively exploitable vulnerability.

The `internal` tree and reference binaries are not public Go APIs.
