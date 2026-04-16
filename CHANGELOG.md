# Changelog

## [1.0.5] - 2026-04-16

### Fixed
- mypy `no-any-return` error in `_server_is_healthy` (wrap comparison in `bool()`)
- CI test failures: extract `_impl_check_setup()` so tests don't depend on
  FastMCP `.fn` attribute; snowbox now imports `_impl_check_setup` directly

## [1.0.4] - 2026-04-16

### Changed
- `open_notebook` logic extracted into `_impl_open_notebook()` for reuse by snowbox
- Server readiness now checked via HTTP `/health` endpoint instead of TCP port probe
- `_free_port()` sends SIGKILL fallback when SIGTERM doesn't release the port
- Pre-flight validation: checks venv existence and marimo binary before launching
- Better error messages with "run check_setup to diagnose" hints
- `check_setup` now reports `marimo_system_version` and `marimo_library_version`
  and warns on version mismatch
- Removed old `open_interactive()` from executor (logic moved to server)

### Added
- `get_marimo_version()` static method on `NotebookExecutor`
- 10 new tests: `_server_is_healthy`, `_free_port` SIGKILL fallback,
  `open_notebook` pre-flight, `check_setup` version mismatch, `get_marimo_version`
- `docs/kernel-not-found-dev.md` and `docs/kernel-not-found-user.md`

## [1.0.1] - 2026-04-15

### Fixed
- mypy `python_version` reverted to `"3.11"` (correct minimum target); CI typecheck
  job now runs on Python 3.11 so cryptography stubs are 3.11-compatible
- `_build_explanation()` now includes the unified diff text inline when code changed,
  so agents see the actual diff in the `explanation` string without a separate lookup

## [1.0.0] - 2026-04-15

### Added
- `offset` parameter on `list_runs` tool — enables cursor-style pagination through run history
- `total` and `offset` fields in `list_runs` response
- `count_runs(status=...)` — optional status filter for accurate totals when paginating
  with a status filter
- `list_runs_older_than(older_than_days)` on `Database` — read-only preview query for
  dry-run purge support
- `dry_run=True` on `purge_runs` tool — returns `would_delete_runs` and `run_ids` preview
  without performing any deletions
- `diff_text` field in `code_diff` within `diff_runs` response — full unified-diff text
  when code changed; `None` when unchanged; truncated to 8 000 characters if very large

### Changed
- `list_runs` and `get_run` implementations extracted to `_impl_list_runs` / `_impl_get_run`
  — consistent with all other tools, enables direct unit testing without FastMCP wrapper
- README updated to document all 17 tools (previously documented 13)
- `purge_runs` docstring updated to include `dry_run` parameter

## [0.9.2] - 2026-04-15

### Fixed
- `env_manager.py`: `_load_meta` return type changed from `dict` to `dict[str, Any]` —
  resolves `no-any-return` mypy error; no behaviour change

### Added
- `get_run()` response now includes four provenance fields that were stored in the DB but
  never surfaced: `code_hash`, `env_hash`, `freeze`, and `risk_findings`
- `risk_findings` field added to `RunRecord` Pydantic model with JSON deserialisation
  (same pattern as `packages` / `artifacts`)
- `typecheck` job added to CI — runs `mypy src/marimo_sandbox/ --ignore-missing-imports`
  on Python 3.11 after every push/PR

## [0.9.1] - 2026-04-15

### Added
- `diff_runs(run_id, compare_to=None)` tool — compare two runs and explain what changed:
  - Auto-resolves reference to `parent_run_id` when `compare_to` is omitted
  - Compares code (SHA-256 hash + unified-diff line counts), environment (hash + package
    set diff), status, artifacts (set diff), and structured `__outputs__` (shallow key diff)
  - Duration change flagged when delta exceeds 20% of reference run time
  - Classifies the relationship between runs: `parent_child`, `siblings`, or `unrelated`
  - Returns a plain-English `explanation` string summarising all changes
- `_build_explanation()` internal helper — pure function; no DB or filesystem access
- `import difflib` — stdlib; used for `unified_diff` line counting

## [0.9.0] - 2026-04-15

### Added
- `env_manager.py` — `EnvManager` class with hash-based venv cache:
  - `env_hash(packages)` — deterministic 16-char SHA-256 hash of sorted package list
  - `get_or_create(packages)` — cache hit reuses existing venv; cache miss creates one,
    installs packages, writes `freeze.txt` and `meta.json`
  - `list_envs()`, `delete_env()`, `clean_old_envs(days)` helpers
- `EnvInfo` dataclass — env_hash, python_path, packages, freeze, created_at,
  last_used_at, size_bytes
- `env_hash` column on `runs` table — links each run to its venv
- `list_environments()` tool — list cached venvs (16th MCP tool)
- `clean_environments(older_than_days=90)` tool — delete stale venvs (16th+1 = 16 total)
- `python_path` param on `execute()` and `execute_async()` — uses venv Python instead
  of `sys.executable`; defaults keep all existing behaviour for no-package runs

### Changed
- `run_python` now routes package installs through `EnvManager.get_or_create()` instead
  of `executor.install_packages()` — same packages across runs reuse an existing venv

## [0.8.0] - 2026-04-15

### Added
- `RunStatus.RUNNING` and `RunStatus.CANCELLED` — two new execution states
- `async_mode=True` on `run_python` — returns `status="running"` immediately;
  a daemon thread watches the process and updates the DB when it finishes
- `cancel_run(run_id)` tool — send SIGTERM to a running async process and mark
  it `cancelled` in the DB (14th MCP tool)
- `pid` column on `runs` table — stored immediately after async launch
- Startup recovery in `Database.__init__` — any runs stuck in `running` from a
  previous crashed server are reset to `error` on next open
- `_finish_result()` helper on `NotebookExecutor` — shared logic between sync
  and async completion paths
- `execute_async()` on `NotebookExecutor` — launch a Popen without waiting

## [0.7.0] - 2026-04-15

### Added
- `parent_run_id` column on `runs` table — `rerun` now links the new run back to its
  origin; `get_run` exposes this field in the response
- `purge_expired_approvals()` on `Database` — called automatically at the top of
  `list_pending_approvals` to remove stale rows

### Changed
- `import os` no longer triggers a `high` / `dangerous_import` finding; actual dangerous
  `os` calls (`os.system`, `os.popen`) continue to fire `critical` / `shell_execution`
- Integration tests added to CI as a separate `integration-test` job (Python 3.12,
  `pytest -m slow`)

## [0.6.0] - 2026-04-15

### Added
- `analyzer.py` — `StaticRiskAnalyzer` (AST-based) detects critical/high/medium/low risk
  patterns: subprocess calls, shell execution, dynamic code eval, dangerous imports,
  file writes, env reads
- `dry_run=True` on `run_python` — returns risk analysis without executing code
- `require_approval=True` on `run_python` — blocks runs with critical findings and
  issues an approval token; execution proceeds via `approve_run(token)`
- `approve_run` tool — confirm a blocked run and execute it (tokens expire in 1 hour)
- `list_pending_approvals` tool — list runs awaiting confirmation
- `pending_approvals` SQLite table — stores blocked runs with TTL expiry
- `risk_findings` column on `runs` table — advisory findings persisted per run
- `risk_findings` field in `run_python` response (when findings exist)

## [0.5.0] - 2026-04-15

### Added
- `ArtifactInfo` model — typed descriptor for user-created files (path, size, extension)
- `code_hash` (SHA-256) computed per run and stored in DB + returned in response
- `freeze` — full `pip freeze` snapshot captured after package installation
- `artifacts` — list of user-created files detected after execution
- `__outputs__: dict` initialized in `__execution__` cell and written to result sidecar
- `list_artifacts` tool — list files created by a run's code
- `read_artifact` tool — read text or binary artifact content (path-traversal safe)
- `get_run_outputs` tool — retrieve the structured `__outputs__` dict from a run
- Three new columns migrated automatically: `code_hash`, `freeze`, `artifacts`

## [0.4.0] - 2026-04-15

### Added
- `RunRecord` Pydantic model — `get_run` and `list_runs` now return typed objects;
  `packages` JSON parsing handled by a model validator (no manual deserialization)
- `RunStatus` StrEnum (`pending` / `success` / `error`) — replaces bare string literals
- `DeletedRunInfo` model for `delete_runs_older_than` return type
- `py.typed` marker (PEP 561) — library is now typed
- mypy config in `pyproject.toml` (`disallow_untyped_defs`, `warn_return_any`)
- `pydantic>=2.0` added as direct dependency (was already a transitive dep via fastmcp)

### Changed
- `_impl_run_python`, `_impl_delete_run`, `_impl_rerun`, `_impl_purge_runs` extracted —
  MCP tool decorators are now thin wrappers; implementation testable without FastMCP
- All `Optional[X]` replaced with `X | None` (Python 3.10+ union syntax)
- `count_runs` now uses a direct cursor instead of `_fetchone` (avoids spurious
  `packages` key on scalar queries)

### Fixed
- `shutil.rmtree(ignore_errors=True)` replaced — failure now reported as `warning`
  in response instead of silently claiming `files_deleted: True`
- `_deserialize_row` removed — mutation-and-return footgun eliminated

## [0.3.0] - 2026-04-15

### Added
- `packages` column in SQLite — packages are now persisted per run and shown in `get_run`; existing DBs are migrated automatically
- `rerun` now defaults to the original run's stored packages when `packages=` is not supplied

### Fixed
- Mutable default argument `packages=[]` in `run_python` and `rerun` replaced with `None`
- `uv` installed in CI so the real uv install path is exercised in the test matrix

## [0.2.0] - 2026-04-15

### Added
- `packages=` parameter on `run_python` — install PyPI deps via uv (fallback: pip) before executing
- `delete_run` tool — remove a run's database record and notebook files
- `rerun` tool — re-execute a previous run's code by run_id, optionally with modified code
- `purge_runs` tool — bulk-delete runs older than N days
- `uv_available` field in `check_setup` response

## [0.1.0] - 2026-04-14

### Added
- Initial release
- `run_python` tool — execute arbitrary Python in an auditable Marimo notebook
- `open_notebook` tool — open a run in Marimo's interactive editor
- `list_runs` / `get_run` tools — query run history from SQLite
- `check_setup` tool — verify marimo and Docker availability
- Optional Docker sandbox mode (`sandbox=True`)
