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
- **Persistent** — all runs stored in SQLite with stdout, stderr, status, code hash, and artifacts
- **Safe** — static risk analysis runs before every execution; critical patterns can require approval

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
dry_run           If True, return static risk analysis only — do not execute (default False)
require_approval  If True, block execution when critical risk patterns are found (default False)
```

Returns: `run_id`, `status`, `stdout`, `stderr`, `error`, `duration_ms`, `notebook_path`,
`view_command`, `code_hash`, `artifacts`, and optionally `risk_findings`, `packages_installed`,
`freeze`.

Packages are installed via `uv pip install` when uv is available, falling back to `pip`. A
full `pip freeze` snapshot is captured after installation and stored with the run.

#### Structured outputs

Your code can expose typed data to agents via the `__outputs__` dict:

```python
import pandas as pd
df = pd.read_csv("data.csv")
__outputs__["summary"] = df.describe().to_dict()
__outputs__["row_count"] = len(df)
```

Retrieve these values later with `get_run_outputs`.

#### Static risk analysis

Every call to `run_python` runs an AST-based risk scan before execution. Findings appear
in `risk_findings` in the response. Use `dry_run=True` to get the analysis without running:

```
risk_findings severity tiers:
  critical  subprocess calls, os.system/popen, eval/exec/compile
  high      dangerous imports (os, subprocess, socket, requests, …)
  medium    open() with write/append mode
  low       os.environ[] access
```

Use `require_approval=True` to block execution when critical patterns are found. The response
will include an `approval_token` — pass it to `approve_run` to proceed.

### `approve_run`

Confirm a blocked run and execute it. Tokens expire after 1 hour.

```
approval_token   Token returned by run_python when status='awaiting_confirmation'
reason           Optional note explaining the approval
```

### `list_pending_approvals`

List all runs currently awaiting approval, including expiry status and critical finding count.

### `list_artifacts`

List files created by a run's code (everything in the notebook directory except the
notebook itself and the result sidecar).

```
run_id   Run to inspect
```

Returns `artifact_count` and `artifacts` — each entry has `path`, `size_bytes`, `extension`.

### `read_artifact`

Read the content of an artifact file. Path traversal is rejected. Large files are
refused (default limit: 5 MB).

```
run_id          Run that created the file
artifact_path   Relative path from list_artifacts
max_size_bytes  Size limit in bytes (default 5 000 000)
```

Returns `content` (str) for text files or `content_base64` for binary files, plus
`media_type`, `size_bytes`, `is_text`.

### `get_run_outputs`

Retrieve the structured `__outputs__` dict written by the run. Returns `{}` if the
run hasn't completed successfully or didn't populate `__outputs__`.

```
run_id   Run to read outputs from
```

### `rerun`

Re-execute a previous run's code by `run_id`, optionally with modifications.

```
run_id            Run to re-execute
code              Override the code (default: use original)
description       Override the description (default: original + " (rerun)")
timeout_seconds   Max execution time (default 60)
sandbox           Run in Docker sandbox (default False)
packages          PyPI packages to install (default: reuse original run's packages)
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

A result sidecar is written alongside the notebook on success:
```
~/.marimo-sandbox/notebooks/{run_id}/{run_id}_result.json
```

Any other files your code writes to disk are captured as artifacts and
accessible via `list_artifacts` / `read_artifact`.

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
| `__execution__` | Initialises `__outputs__: dict = {}`; runs your code; returns `(sandbox_executed, __outputs__)` |
| `__record__` | Depends on `sandbox_executed` and `__outputs__` — only runs on success; writes result sidecar with outputs |

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

# Type check
mypy src/

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
