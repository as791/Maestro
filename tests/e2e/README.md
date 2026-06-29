# Visual smoke test

The snapshots document the two main entry points:

- `snapshots/docs-getting-started.png` — installation and first deployment flow.
- `snapshots/operations-console.png` — console populated with deterministic API data.

Refresh the console snapshot after UI changes:

```sh
python3 tests/e2e/serve_fixture.py
# Open http://127.0.0.1:4173/ui/ and capture the viewport.
```

Build the documentation before refreshing its snapshot:

```sh
npm --prefix docs-site run build
python3 -m http.server 4174 --directory docs-site/build
# Open http://127.0.0.1:4174/docs/getting-started/ and capture the viewport.
```
