# Kernel Not Found — Developer Troubleshooting

Reference for developers and Claude Code instances diagnosing `open_notebook`
failures where the browser shows "kernel not found" or a blank/error page.

## Quick Diagnostic

Run `check_setup` first. It reports:

- `marimo_system_version` — the CLI binary on PATH
- `marimo_library_version` — the Python library in the current environment
- `notes` — any detected issues (version mismatch, missing tools)

If the user can't run `check_setup`, have them run manually:

```bash
marimo --version          # system binary
python -c "import marimo; print(marimo.__version__)"   # library
```

## Root Causes (ordered by likelihood)

### 1. Version mismatch: system marimo != venv marimo

**Symptom:** `check_setup` shows different `marimo_system_version` and
`marimo_library_version`. Browser shows "kernel not found" immediately.

**Why:** Notebooks are generated with the library's format version. If the
system `marimo` binary is older, it can't parse the notebook format.

**Fix:**

```bash
# Option A: upgrade system marimo to match library
pip install marimo==<library_version>
# or if installed via pipx:
pipx upgrade marimo

# Option B: downgrade library to match system
pip install marimo==<system_version>
```

The `open_notebook` implementation now prefers the venv's marimo binary
(`DATA_DIR/envs/<hash>/bin/marimo`) when it exists, which avoids this for
runs that have a cached venv.

### 2. Stale browser cache

**Symptom:** `open_notebook` returns `success: true` with a URL, marimo
process is running, but the browser shows an old error or blank page.

**Fix:** Hard refresh — `Cmd+Shift+R` (macOS) / `Ctrl+Shift+R` (Linux/Windows).

### 3. Port occupied by a stale process

**Symptom:** `open_notebook` returns success but the browser shows a
different notebook, or the new marimo process fails to bind.

**How it's handled now:** `_free_port` sends SIGTERM, waits 0.75s, then
checks if the port is still open. If it is, it re-queries `lsof` and sends
SIGKILL to stragglers. This covers zombie marimo processes that ignore
SIGTERM.

**Manual fix if it still happens:**

```bash
lsof -ti tcp:2718 | xargs kill -9
```

### 4. Venv was cleaned but run still references it

**Symptom:** `open_notebook` returns `success: false` with error containing
"Virtual environment not found".

**Fix:** Re-run with the same packages to recreate the venv:

```
rerun(run_id="<the_run_id>")
```

Or call `open_notebook` on the new run.

### 5. marimo binary not found or not runnable

**Symptom:** `open_notebook` returns `success: false` with error containing
"marimo not found or not runnable".

**Fix:** Install marimo:

```bash
pip install marimo
# or
pipx install marimo
```

### 6. PEP 723 / --sandbox conflict

**Background:** Notebooks include a `# /// script` PEP 723 header listing
dependencies. If marimo sees this header without `--no-sandbox`, it spawns a
`uv`-managed sandbox environment that conflicts with our pre-activated venv.

**How it's handled:** `open_notebook` always passes `--no-sandbox` to
override auto-detection. If this flag is missing (e.g., user runs
`marimo edit` manually), they'll hit "kernel not found".

**Manual workaround:**

```bash
marimo edit <notebook_path> --no-sandbox --no-token
```

## Architecture Notes

### Poll loop: _server_is_healthy vs _port_is_open

The old implementation checked if the TCP port was open (`_port_is_open`).
This was unreliable because marimo opens the port before the kernel is ready.

The current implementation uses `_server_is_healthy(port)` which hits
`http://127.0.0.1:{port}/health` and checks for `{"status": "healthy"}`.
This endpoint is unauthenticated and only returns 200 + healthy when the
full server (including kernel) is ready.

`_port_is_open` is retained as a helper for `_free_port` (checking if a
port is still occupied after SIGTERM), but is NOT used for readiness checks.

### Pre-flight validation order

`_impl_open_notebook` validates in this order:

1. Run exists in DB
2. Notebook file exists on disk
3. Free the port (SIGTERM + SIGKILL fallback)
4. Venv directory exists (if run has `env_hash`)
5. Resolve `marimo_bin` (prefer venv's binary)
6. `get_marimo_version(marimo_bin)` — confirms binary is runnable
7. Launch `marimo edit` + poll with `_server_is_healthy`

Each step returns a specific, actionable error message on failure.

### Code deduplication

`_impl_open_notebook` is defined in `marimo_sandbox/server.py` and imported
by `snowbox/server.py`. Both `open_notebook` tool functions are thin
wrappers. Any fixes to the open logic only need to happen in one place.

## Relevant Files

| File | What |
|------|------|
| `marimo_sandbox/server.py` | `_impl_open_notebook`, `_server_is_healthy`, `_free_port`, `check_setup` |
| `marimo_sandbox/executor.py` | `get_marimo_version` |
| `snowbox/server.py` | `open_notebook` (delegates to `_impl_open_notebook`) |
| `tests/test_server.py` | Tests for all of the above |
