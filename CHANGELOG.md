# Changelog

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
