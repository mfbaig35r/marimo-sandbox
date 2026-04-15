# Changelog

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
