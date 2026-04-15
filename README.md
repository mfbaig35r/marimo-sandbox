# marimo-sandbox

A FastMCP server that runs Python code inside auditable [Marimo](https://marimo.io)
notebooks. Every execution is saved as a human-readable `.py` file you can open,
inspect, and re-run at any time.

## Why

When an AI agent (Claude Code, etc.) runs Python on your behalf, you get back
stdout and maybe a traceback. You can't see the full code in context, can't
re-run it, can't modify it interactively.

marimo-sandbox fixes this by wrapping every execution in a Marimo notebook:

- **Auditable** — the exact code that ran is saved as a `.py` file alongside its output
- **Viewable** — `marimo edit <notebook>` opens it in the browser with reactive cells
- **Re-runnable** — the notebook is standalone; `python notebook.py` works without the server
- **Persistent** — all runs are stored in SQLite with stdout, stderr, and status

## Install

```bash
pip install marimo-sandbox
# or with uv:
uv pip install marimo-sandbox
```

Requires Python 3.11+ and marimo:
```bash
pip install marimo
```

## Add to Claude Code

```bash
claude mcp add marimo-sandbox -- python -m marimo_sandbox
```

Or with `uv`:
```bash
claude mcp add marimo-sandbox -- uvx marimo-sandbox
```

Set a custom data directory (where notebooks and the database are stored):
```bash
claude mcp add marimo-sandbox \
  -e MARIMO_SANDBOX_DIR=/your/preferred/path \
  -- python -m marimo_sandbox
```

## Tools

### `run_python`

Run Python code and get back results + a notebook you can open.

```
code              Python source to execute
description       Short label for this run (shown in list_runs)
timeout_seconds   Max execution time (default 60)
sandbox           Run in Docker with --network=none (default False)
packages          PyPI packages to install before running (e.g. ["pandas", "httpx"])
```

Returns: `run_id`, `status`, `stdout`, `stderr`, `error`, `notebook_path`, `view_command`, and `packages_installed` (if any).

Packages are installed via `uv pip install` when uv is available, falling back to `pip`. Installation happens before the notebook runs, so the packages are immediately importable in your code.

### `rerun`

Re-execute a previous run's code by `run_id`, optionally with modifications.

```
run_id            Run to re-execute
code              Override the code (default: use original)
description       Override the description (default: original + " (rerun)")
timeout_seconds   Max execution time (default 60)
sandbox           Run in Docker sandbox (default False)
packages          PyPI packages to install before running
```

### `open_notebook`

Open a previous run in Marimo's interactive editor.

```
run_id   ID returned by run_python
port     Local port for the editor (default 2718)
```

Returns a `url` to open in your browser. You can then edit cells and re-run them.

### `list_runs`

List recent runs with status, description, and timestamp.

```
limit    Max results (default 20)
status   Filter: 'success', 'error', or 'pending'
```

### `get_run`

Full details of a specific run.

```
run_id                   Run to look up
include_code             Include submitted code (default True)
include_notebook_source  Include full .py notebook source (default False)
```

### `delete_run`

Remove a run's database record and its notebook files from disk.

```
run_id         Run to delete
delete_files   Also remove the notebook directory (default True)
```

### `purge_runs`

Bulk-delete runs older than N days to reclaim disk space.

```
older_than_days   Delete runs older than this many days (default 30)
delete_files      Also remove notebook directories (default True)
```

Returns `deleted_runs`, `files_deleted`, and `run_ids`.

### `check_setup`

Verify marimo, Docker, and uv are available and show the data directory.

## Notebooks

Generated notebooks live at:
```
~/.marimo-sandbox/notebooks/{run_id}/notebook.py
```

Open any of them directly:
```bash
marimo edit ~/.marimo-sandbox/notebooks/run_a1b2c3d4/notebook.py
```

Or run headlessly:
```bash
python ~/.marimo-sandbox/notebooks/run_a1b2c3d4/notebook.py
```

## Sandbox mode (Docker)

For untrusted code, `run_python(sandbox=True)` runs inside Docker with:
- `--network=none` — no outbound connections
- `--memory=512m` — memory cap
- `--cpus=1` — CPU cap
- `--read-only` — read-only root filesystem
- writable `/sandbox` mount for the notebook and result file

Build the sandbox image first:
```bash
docker build -f Dockerfile.sandbox -t marimo-sandbox:latest .
```

Add packages your code needs to `Dockerfile.sandbox` and rebuild.

## Configuration

| Env var | Default | Description |
|---|---|---|
| `MARIMO_SANDBOX_DIR` | `~/.marimo-sandbox` | Where notebooks and DB are stored |
| `MARIMO_SANDBOX_DOCKER_IMAGE` | `marimo-sandbox:latest` | Docker image for sandbox mode |

## Notebook structure

Every generated notebook has four fixed cells:

| Cell | Purpose |
|---|---|
| `__setup__` | Imports marimo, returns `(mo,)` |
| `__context__` | Displays run metadata (description, run_id, timestamp) |
| `__execution__` | Your code, plus a `sandbox_executed` sentinel in the return tuple |
| `__record__` | Depends on `sandbox_executed` — only runs on success; writes result sidecar |

The `__record__` → `__execution__` dependency means: if your code raises an
exception, `__record__` never runs (Marimo's DAG won't execute a cell whose
dependencies failed). The executor detects the missing sidecar and reports an
error with the captured stderr.

## Development

```bash
# Install in editable mode with dev dependencies
uv pip install -e ".[dev]"

# Lint
ruff check src/ tests/

# Unit tests (fast, no subprocess)
pytest tests/ -m "not slow" -v

# Integration tests (run real Marimo subprocesses)
pytest tests/ -m slow -v
```

## Known limitations

- Top-level `return` statements in submitted code are rejected (they exit the
  cell function before the sentinel is set). Wrap such code in a function.
- `sys.exit()` in user code is detected and reported as an error.
- Generated notebooks always import `marimo`. If marimo is not installed in the
  execution environment, the notebook will fail.
