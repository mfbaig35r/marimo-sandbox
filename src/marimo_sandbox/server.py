"""
marimo-sandbox FastMCP server.

Tools
-----
run_python          Execute Python code; returns stdout/stderr and a notebook path.
open_notebook       Launch marimo edit for interactive viewing of a run.
list_runs           List recent runs with status and description.
get_run             Full details of a specific run, including code and output.
check_setup         Verify marimo and Docker availability.
"""

import os
import secrets
import shutil
from pathlib import Path

from fastmcp import FastMCP

from .database import Database
from .executor import NotebookExecutor
from .generator import NotebookGenerator
from .models import RunStatus

# ── Configuration ─────────────────────────────────────────────────────────────

DATA_DIR = (
    Path(os.environ.get("MARIMO_SANDBOX_DIR", "~/.marimo-sandbox"))
    .expanduser()
    .resolve()
)
DATA_DIR.mkdir(parents=True, exist_ok=True)

DOCKER_IMAGE = os.environ.get(
    "MARIMO_SANDBOX_DOCKER_IMAGE", "marimo-sandbox:latest"
)

# ── Singletons ────────────────────────────────────────────────────────────────

db = Database(DATA_DIR / "sandbox.db")
generator = NotebookGenerator(DATA_DIR / "notebooks")
executor = NotebookExecutor(docker_image=DOCKER_IMAGE)

# ── Server ────────────────────────────────────────────────────────────────────

mcp = FastMCP(
    "marimo-sandbox",
    instructions=(
        "Execute Python code in auditable Marimo notebooks. "
        "Every run is saved as a human-readable .py notebook file that you "
        "or the user can open with `marimo edit <path>` for interactive "
        "viewing, editing, and re-execution. "
        "Use run_python to execute code (pass packages= to auto-install PyPI deps). "
        "Use open_notebook to open a run in the browser. "
        "Use list_runs and get_run to inspect history. "
        "Use rerun to re-execute a previous run's code by run_id. "
        "Use delete_run to remove a run's record and files. "
        "Use purge_runs to bulk-delete runs older than N days."
    ),
)


# ── Implementations ───────────────────────────────────────────────────────────


def _impl_run_python(
    code: str,
    description: str,
    timeout_seconds: int,
    sandbox: bool,
    packages: list[str] | None,
) -> dict:
    if packages is None:
        packages = []
    # Normalise line endings and strip BOM
    code = code.lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")

    run_id = "run_" + secrets.token_hex(4)

    # Install packages before generating/executing
    if packages:
        install_result = executor.install_packages(packages)
        if not install_result["success"]:
            return {
                "run_id": None,
                "status": "error",
                "error": f"Package install failed: {install_result['output']}",
                "stdout": "",
                "stderr": "",
                "duration_ms": 0,
                "notebook_path": None,
                "view_command": None,
            }

    # Generate notebook
    try:
        notebook = generator.generate(
            run_id=run_id,
            description=description,
            code=code,
        )
    except ValueError as exc:
        return {
            "run_id": None,
            "status": "error",
            "error": str(exc),
            "stdout": "",
            "stderr": "",
            "duration_ms": 0,
            "notebook_path": None,
            "view_command": None,
        }

    # Persist pending record before executing (so it's visible even if we crash)
    db.create_run(
        run_id=run_id,
        description=description,
        code=code,
        notebook_path=str(notebook.notebook_path),
        packages=packages,
    )

    # Execute
    result = executor.execute(
        notebook=notebook,
        timeout_seconds=timeout_seconds,
        sandbox=sandbox,
    )

    # Update record with outcome
    db.update_run(
        run_id=run_id,
        status=result.status,
        duration_ms=result.duration_ms,
        stdout=result.stdout,
        stderr=result.stderr,
        error=result.error,
    )

    response = {
        "run_id": run_id,
        "status": result.status,
        "stdout": result.stdout or "",
        "stderr": result.stderr or "",
        "error": result.error,
        "duration_ms": result.duration_ms,
        "notebook_path": str(notebook.notebook_path),
        "view_command": f'marimo edit "{notebook.notebook_path}"',
    }
    if packages:
        response["packages_installed"] = packages
    return response


def _impl_delete_run(run_id: str, delete_files: bool = True) -> dict:
    run = db.get_run(run_id)
    if not run:
        return {"success": False, "error": f"Run not found: {run_id}"}
    db.delete_run(run_id)
    files_deleted = False
    if delete_files:
        nb_dir = Path(run.notebook_path).parent
        if nb_dir.exists():
            try:
                shutil.rmtree(nb_dir)
                files_deleted = True
            except OSError as exc:
                return {
                    "success": True,
                    "run_id": run_id,
                    "files_deleted": False,
                    "warning": f"Record deleted but directory removal failed: {exc}",
                }
    return {"success": True, "run_id": run_id, "files_deleted": files_deleted}


def _impl_rerun(
    run_id: str,
    code: str | None,
    description: str | None,
    timeout_seconds: int,
    sandbox: bool,
    packages: list[str] | None,
) -> dict:
    original = db.get_run(run_id)
    if not original:
        return {"error": f"Run not found: {run_id}"}
    return _impl_run_python(
        code=code if code is not None else original.code,
        description=description if description is not None
        else f"{original.description} (rerun)",
        timeout_seconds=timeout_seconds,
        sandbox=sandbox,
        packages=packages if packages is not None else original.packages,
    )


def _impl_purge_runs(older_than_days: int, delete_files: bool) -> dict:
    rows = db.delete_runs_older_than(older_than_days)
    files_deleted = 0
    if delete_files:
        for row in rows:
            nb_dir = Path(row.notebook_path).parent
            if nb_dir.exists():
                try:
                    shutil.rmtree(nb_dir)
                    files_deleted += 1
                except OSError:
                    pass  # best-effort; don't abort the whole purge
    return {
        "deleted_runs": len(rows),
        "files_deleted": files_deleted,
        "run_ids": [r.run_id for r in rows],
    }


# ── Tools ─────────────────────────────────────────────────────────────────────


@mcp.tool()
def run_python(
    code: str,
    description: str = "Python run",
    timeout_seconds: int = 60,
    sandbox: bool = False,
    packages: list[str] | None = None,
) -> dict:
    """
    Execute Python code in a Marimo notebook.

    The code is wrapped in a human-readable Marimo notebook (.py file) stored
    on disk. You and the user can open it at any time with:
        marimo edit <notebook_path>

    The notebook captures stdout and stderr. On success, a result sidecar JSON
    is written alongside the notebook. On failure, the error traceback is
    returned in the 'error' field.

    Args:
        code:             Python source code to run.
        description:      Short label for this run (shown in the notebook header
                          and in list_runs). Be specific — e.g. "Parse CSV and
                          compute mean" is more useful than "test".
        timeout_seconds:  Max execution time in seconds (default 60).
        sandbox:          Run inside Docker with --network=none and resource
                          limits (requires Docker). Default False.
        packages:         PyPI packages to install before running (via uv, fallback pip).

    Returns:
        run_id, status, stdout, stderr, error, duration_ms,
        notebook_path, view_command, packages_installed (if any).
    """
    return _impl_run_python(code, description, timeout_seconds, sandbox, packages)


@mcp.tool()
def open_notebook(run_id: str, port: int = 2718) -> dict:
    """
    Open a run's Marimo notebook in the interactive editor.

    Launches `marimo edit <notebook>` and returns a localhost URL to open in
    the browser. The notebook shows the run metadata, the original code, and
    the execution output — and lets you edit and re-run cells live.

    Args:
        run_id:  The run ID returned by run_python.
        port:    Local port for the Marimo server (default 2718).

    Returns:
        success, url, pid, notebook_path, message  — or success=False + error.
    """
    run = db.get_run(run_id)
    if not run:
        return {"success": False, "error": f"Run not found: {run_id}"}

    notebook_path = Path(run.notebook_path)
    if not notebook_path.exists():
        return {
            "success": False,
            "error": f"Notebook file not found at {notebook_path}",
        }

    return executor.open_interactive(notebook_path, port=port)


@mcp.tool()
def list_runs(
    limit: int = 20,
    status: str | None = None,
) -> dict:
    """
    List recent Python runs.

    Args:
        limit:   Max number of runs to return (default 20).
        status:  Filter to 'success', 'error', or 'pending'. Omit for all.

    Returns:
        count, runs — each entry has run_id, description, status,
        duration_ms, created_at, notebook_path.
    """
    runs = db.list_runs(limit=limit, status=RunStatus(status) if status else None)
    slim = [
        {
            "run_id": r.run_id,
            "description": r.description,
            "status": r.status,
            "duration_ms": r.duration_ms,
            "created_at": str(r.created_at),
            "notebook_path": r.notebook_path,
        }
        for r in runs
    ]
    return {"count": len(slim), "runs": slim}


@mcp.tool()
def get_run(
    run_id: str,
    include_code: bool = True,
    include_notebook_source: bool = False,
) -> dict:
    """
    Get full details of a run.

    Args:
        run_id:                   The run to look up.
        include_code:             Include the submitted Python code (default True).
        include_notebook_source:  Include the full generated notebook .py source
                                  (default False — can be large).

    Returns:
        Full run record: run_id, description, status, stdout, stderr, error,
        duration_ms, notebook_path, view_command, created_at, and optionally
        code and notebook_source.
    """
    run = db.get_run(run_id)
    if not run:
        return {"error": f"Run not found: {run_id}"}

    result = {
        "run_id": run.run_id,
        "description": run.description,
        "status": run.status,
        "duration_ms": run.duration_ms,
        "stdout": run.stdout or "",
        "stderr": run.stderr or "",
        "error": run.error,
        "notebook_path": run.notebook_path,
        "view_command": f'marimo edit "{run.notebook_path}"',
        "created_at": str(run.created_at),
    }

    if include_code:
        result["code"] = run.code

    if include_notebook_source:
        nb_path = Path(run.notebook_path)
        result["notebook_source"] = (
            nb_path.read_text(encoding="utf-8") if nb_path.exists() else None
        )

    return result


@mcp.tool()
def check_setup() -> dict:
    """
    Check that the sandbox environment is ready.

    Returns the data directory, whether marimo and Docker are available,
    total run count, and any setup notes.
    """
    marimo_ok = executor.check_marimo()
    docker_ok = executor.check_docker()

    notes = []
    if not marimo_ok:
        notes.append("marimo is not installed or not on PATH. Run: pip install marimo")
    if not docker_ok:
        notes.append(
            "Docker is not available. sandbox=True in run_python will fail. "
            "Install Docker Desktop to enable sandboxed execution."
        )

    return {
        "data_dir": str(DATA_DIR),
        "marimo_available": marimo_ok,
        "docker_available": docker_ok,
        "uv_available": executor.check_uv(),
        "total_runs": db.count_runs(),
        "ready": marimo_ok,
        "notes": notes,
    }


@mcp.tool()
def delete_run(run_id: str, delete_files: bool = True) -> dict:
    """
    Delete a run's database record and optionally its notebook files.

    Args:
        run_id:        The run to delete.
        delete_files:  Also remove the notebook directory from disk (default True).

    Returns:
        success, run_id, files_deleted — or success=False + error.
    """
    return _impl_delete_run(run_id, delete_files)


@mcp.tool()
def rerun(
    run_id: str,
    code: str | None = None,
    description: str | None = None,
    timeout_seconds: int = 60,
    sandbox: bool = False,
    packages: list[str] | None = None,
) -> dict:
    """
    Re-execute a previous run's code by run_id, optionally with new code.

    Args:
        run_id:           The run to re-execute.
        code:             Override the code (default: use original code).
        description:      Override the description (default: original + " (rerun)").
        timeout_seconds:  Max execution time in seconds (default 60).
        sandbox:          Run inside Docker sandbox (default False).
        packages:         PyPI packages to install before running. Pass None (default)
                          to reuse the packages from the original run; pass [] to
                          explicitly install nothing.

    Returns:
        Same as run_python.
    """
    return _impl_rerun(run_id, code, description, timeout_seconds, sandbox, packages)


@mcp.tool()
def purge_runs(older_than_days: int = 30, delete_files: bool = True) -> dict:
    """
    Bulk-delete runs older than N days to reclaim disk space.

    Args:
        older_than_days:  Delete runs created more than this many days ago (default 30).
        delete_files:     Also remove notebook directories from disk (default True).

    Returns:
        deleted_runs, files_deleted, run_ids.
    """
    return _impl_purge_runs(older_than_days, delete_files)


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    mcp.run()
