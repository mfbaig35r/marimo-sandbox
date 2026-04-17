# Changelog

## [1.0.9] - 2026-04-17

### Added
- **Two-phase Docker execution** — `run_python(sandbox=True, packages=["httpx"])`
  now installs packages inside a Docker container (phase 1, with network), then
  executes the notebook in a fully sandboxed container (phase 2, `--network=none`,
  `--read-only`). Previously, packages were silently installed on the host and
  unavailable inside the sandbox.
- `_docker_install_packages()` helper — runs a throwaway container with
  `pip install --target=/sandbox/.packages` (tries uv first, falls back to pip)
- `_docker_exec_cmd()` helper — builds the sandboxed execution command with
  `PYTHONPATH=/sandbox/.packages` when packages are present
- Shared pip cache volume (`docker-pip-cache/`) for cross-run install speed
- `uv` added to `Dockerfile.sandbox` for faster in-container installs
- 9 new unit tests for two-phase Docker logic, PYTHONPATH injection, install
  failure handling, and sandbox+packages routing in server
- 2 new integration tests: package installation inside Docker, network isolation
  verification after package install

### Changed
- `sandbox=True` with packages no longer creates a useless host venv — only
  `env_hash` is computed for record-keeping
- `execute()` and `execute_async()` accept `packages` and `pip_cache_dir`
  parameters, forwarded to Docker when `sandbox=True`

## [1.0.8] - 2026-04-17

### Fixed
- **Docker sandbox was completely broken** — `_run_docker` and `execute_async`
  passed a redundant `"python"` argument that conflicted with the Dockerfile's
  `ENTRYPOINT ["python"]`, causing the container to fail with
  `can't open file '/sandbox/python'`. Removed the duplicate argument from both
  sync and async paths.

### Added
- 8 unit tests for Docker sandbox command construction, dispatch routing,
  custom image support, timeout forwarding, and error handling
- 5 Docker integration tests: stdout capture, sidecar creation, error propagation,
  network isolation (`--network=none`), and numpy availability
- `docker_available` fixture checks for both Docker daemon and the
  `marimo-sandbox:latest` image — tests skip cleanly in CI

## [1.0.0] - 2026-04-15

### Summary
First stable release with 17 MCP tools, full test coverage, and CI pipeline.

### Tools
- `run_python` — execute Python in an auditable Marimo notebook with optional
  Docker sandboxing, package installation, async mode, static risk analysis,
  and approval gates for critical code patterns
- `open_notebook` — launch Marimo's interactive editor for any run
- `list_runs` / `get_run` — query run history with pagination
- `delete_run` / `purge_runs` — remove runs and notebook files
- `rerun` — re-execute a previous run with optional code/package overrides
- `cancel_run` — stop an async run via SIGTERM
- `diff_runs` — compare two runs (code, env, status, artifacts, outputs)
- `list_artifacts` / `read_artifact` / `get_run_outputs` — inspect run outputs
- `approve_run` / `list_pending_approvals` — approval gate for risky code
- `list_environments` / `clean_environments` — manage hash-based venv cache
- `check_setup` — verify marimo, Docker, and uv availability

### Architecture
- `analyzer.py` — AST-based static risk analysis (subprocess, eval, file writes, etc.)
- `database.py` — SQLite persistence with automatic schema migrations
- `env_manager.py` — hash-based venv cache with uv/pip install
- `executor.py` — subprocess and Docker execution with sidecar-based success detection
- `generator.py` — Marimo notebook generation with PEP 723 metadata injection
- `models.py` — Pydantic models (RunRecord, RunStatus, ArtifactInfo)
- `server.py` — FastMCP tool registrations

### Quality
- Typed codebase (mypy strict, py.typed marker)
- 200+ tests across unit, integration, and Docker paths
- CI: ruff lint, mypy, pytest on Python 3.11/3.12/3.13, integration tests

## [0.x] - 2026-04-14 to 2026-04-15

Development releases (v0.1.0 through v0.9.2). Iterative build-out of all core
features: execution engine, notebook generation, SQLite persistence, package
management, risk analysis, approval gates, async execution, environment caching,
run diffing, and provenance tracking.
