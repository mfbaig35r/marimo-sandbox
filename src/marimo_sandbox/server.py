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
from pathlib import Path
from typing import Optional

from fastmcp import FastMCP

from .database import Database
from .executor import NotebookExecutor
from .generator import NotebookGenerator

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
        "Use run_python to execute code. "
        "Use open_notebook to open a run in the browser. "
        "Use list_runs and get_run to inspect history."
    ),
)


# ── Tools ─────────────────────────────────────────────────────────────────────


@mcp.tool()
def run_python(
    code: str,
    description: str = "Python run",
    timeout_seconds: int = 60,
    sandbox: bool = False,
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

    Returns:
        run_id, status, stdout, stderr, error, duration_ms,
        notebook_path, view_command.
    """
    # Normalise line endings and strip BOM
    code = code.lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")

    run_id = "run_" + secrets.token_hex(4)

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

    return {
        "run_id": run_id,
        "status": result.status,
        "stdout": result.stdout or "",
        "stderr": result.stderr or "",
        "error": result.error,
        "duration_ms": result.duration_ms,
        "notebook_path": str(notebook.notebook_path),
        "view_command": f'marimo edit "{notebook.notebook_path}"',
    }


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

    notebook_path = Path(run["notebook_path"])
    if not notebook_path.exists():
        return {
            "success": False,
            "error": f"Notebook file not found at {notebook_path}",
        }

    return executor.open_interactive(notebook_path, port=port)


@mcp.tool()
def list_runs(
    limit: int = 20,
    status: Optional[str] = None,
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
    runs = db.list_runs(limit=limit, status=status)
    slim = [
        {
            "run_id": r["run_id"],
            "description": r["description"],
            "status": r["status"],
            "duration_ms": r["duration_ms"],
            "created_at": str(r["created_at"]),
            "notebook_path": r["notebook_path"],
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
        "run_id": run["run_id"],
        "description": run["description"],
        "status": run["status"],
        "duration_ms": run["duration_ms"],
        "stdout": run["stdout"] or "",
        "stderr": run["stderr"] or "",
        "error": run["error"],
        "notebook_path": run["notebook_path"],
        "view_command": f'marimo edit "{run["notebook_path"]}"',
        "created_at": str(run["created_at"]),
    }

    if include_code:
        result["code"] = run["code"]

    if include_notebook_source:
        nb_path = Path(run["notebook_path"])
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
        "total_runs": db.count_runs(),
        "ready": marimo_ok,
        "notes": notes,
    }


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    mcp.run()
