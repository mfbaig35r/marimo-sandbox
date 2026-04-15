"""
marimo-sandbox FastMCP server.

Tools
-----
run_python              Execute Python code; returns stdout/stderr and a notebook path.
open_notebook           Launch marimo edit for interactive viewing of a run.
list_runs               List recent runs with status and description.
get_run                 Full details of a specific run, including code and output.
check_setup             Verify marimo and Docker availability.
delete_run              Remove a run's record and notebook files.
rerun                   Re-execute a previous run's code.
purge_runs              Bulk-delete runs older than N days.
list_artifacts          List user-created files in a run's notebook directory.
read_artifact           Read the content of an artifact file.
get_run_outputs         Retrieve the structured __outputs__ dict from a run.
approve_run             Confirm a blocked run and execute it.
list_pending_approvals  List runs awaiting approval.
"""

import base64
import hashlib
import json
import mimetypes
import os
import secrets
import shutil
from datetime import datetime, timedelta, timezone
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
        "Use purge_runs to bulk-delete runs older than N days. "
        "Use list_artifacts / read_artifact to inspect files created by a run. "
        "Use get_run_outputs to read the structured __outputs__ dict. "
        "Use dry_run=True on run_python for static risk analysis without execution. "
        "Use require_approval=True to block runs with critical risk findings."
    ),
)


# ── Private helpers ────────────────────────────────────────────────────────────


def _scan_artifacts(notebook_dir: Path, run_id: str) -> list[str]:
    """Return relative paths of user-created files in notebook_dir."""
    exclude = {"notebook.py", f"{run_id}_result.json"}
    return sorted(
        str(item.relative_to(notebook_dir))
        for item in notebook_dir.rglob("*")
        if item.is_file() and item.name not in exclude
    )


# ── Implementations ───────────────────────────────────────────────────────────


def _impl_run_python(
    code: str,
    description: str,
    timeout_seconds: int,
    sandbox: bool,
    packages: list[str] | None,
    dry_run: bool = False,
    require_approval: bool = False,
) -> dict:
    if packages is None:
        packages = []
    # Normalise line endings and strip BOM
    code = code.lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")

    # ── Static risk analysis ─────────────────────────────────────────────────
    from .analyzer import StaticRiskAnalyzer

    findings = StaticRiskAnalyzer(code).analyze()
    findings_dicts = [
        {
            "severity": f.severity,
            "category": f.category,
            "line": f.line,
            "message": f.message,
            "code_snippet": f.code_snippet,
        }
        for f in findings
    ]

    # dry_run: return analysis only, no execution
    if dry_run:
        critical = [f for f in findings if f.severity == "critical"]
        return {
            "status": "analysis_complete",
            "dry_run": True,
            "risk_findings": findings_dicts,
            "requires_confirmation": bool(critical),
            "finding_count": {
                s: sum(1 for f in findings if f.severity == s)
                for s in ("critical", "high", "medium", "low")
            },
        }

    # approval gate: block on critical findings if require_approval=True
    critical = [f for f in findings if f.severity == "critical"]
    if require_approval and critical:
        token = "approval_" + secrets.token_hex(16)
        run_id = "run_" + secrets.token_hex(4)
        expires_at = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        db.create_pending_approval(
            token=token,
            run_id=run_id,
            code=code,
            description=description,
            packages=packages,
            timeout_seconds=timeout_seconds,
            sandbox=sandbox,
            risk_findings_json=json.dumps(findings_dicts),
            expires_at=expires_at,
        )
        return {
            "status": "awaiting_confirmation",
            "run_id": run_id,
            "approval_token": token,
            "expires_at": expires_at,
            "critical_findings": [d for d in findings_dicts if d["severity"] == "critical"],
            "message": "Call approve_run(approval_token) to execute.",
        }

    # ── Normal execution ─────────────────────────────────────────────────────
    code_hash = hashlib.sha256(code.encode()).hexdigest()
    run_id = "run_" + secrets.token_hex(4)

    # Install packages before generating/executing
    freeze: str | None = None
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
        freeze = install_result.get("freeze") or None

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
        code_hash=code_hash,
    )

    # Execute
    result = executor.execute(
        notebook=notebook,
        timeout_seconds=timeout_seconds,
        sandbox=sandbox,
    )

    # Scan for user-created artifact files
    artifacts = _scan_artifacts(notebook.notebook_dir, run_id)

    # Update record with outcome
    db.update_run(
        run_id=run_id,
        status=result.status,
        duration_ms=result.duration_ms,
        stdout=result.stdout,
        stderr=result.stderr,
        error=result.error,
        freeze=freeze,
        artifacts=artifacts or None,
        risk_findings=findings_dicts or None,
    )

    response: dict = {
        "run_id": run_id,
        "status": result.status,
        "stdout": result.stdout or "",
        "stderr": result.stderr or "",
        "error": result.error,
        "duration_ms": result.duration_ms,
        "notebook_path": str(notebook.notebook_path),
        "view_command": f'marimo edit "{notebook.notebook_path}"',
        "code_hash": code_hash,
        "artifacts": artifacts,
    }
    if packages:
        response["packages_installed"] = packages
    if freeze:
        response["freeze"] = freeze
    if findings:
        response["risk_findings"] = findings_dicts
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


def _impl_list_artifacts(run_id: str) -> dict:
    run = db.get_run(run_id)
    if not run:
        return {"error": f"Run not found: {run_id}"}
    notebook_dir = Path(run.notebook_path).parent
    artifact_paths = _scan_artifacts(notebook_dir, run_id)
    artifact_infos = []
    for rel_path in artifact_paths:
        full_path = notebook_dir / rel_path
        stat = full_path.stat()
        artifact_infos.append(
            {
                "path": rel_path,
                "size_bytes": stat.st_size,
                "extension": Path(rel_path).suffix,
            }
        )
    return {
        "run_id": run_id,
        "artifact_count": len(artifact_infos),
        "artifacts": artifact_infos,
    }


def _impl_read_artifact(
    run_id: str,
    artifact_path: str,
    max_size_bytes: int = 5_000_000,
) -> dict:
    run = db.get_run(run_id)
    if not run:
        return {"error": f"Run not found: {run_id}"}
    notebook_dir = Path(run.notebook_path).parent.resolve()
    full_path = (notebook_dir / artifact_path).resolve()
    # Reject path traversal
    try:
        full_path.relative_to(notebook_dir)
    except ValueError:
        return {"error": f"Path traversal detected: {artifact_path}"}
    if not full_path.exists():
        return {"error": f"Artifact not found: {artifact_path}"}
    size_bytes = full_path.stat().st_size
    if size_bytes > max_size_bytes:
        return {
            "error": f"Artifact too large: {size_bytes} bytes (max {max_size_bytes})"
        }
    media_type, _ = mimetypes.guess_type(str(full_path))
    is_text = media_type is not None and (
        media_type.startswith("text/") or media_type == "application/json"
    )
    if is_text:
        return {
            "content": full_path.read_text(errors="replace"),
            "media_type": media_type,
            "size_bytes": size_bytes,
            "is_text": True,
        }
    return {
        "content_base64": base64.b64encode(full_path.read_bytes()).decode(),
        "media_type": media_type,
        "size_bytes": size_bytes,
        "is_text": False,
    }


def _impl_get_run_outputs(run_id: str) -> dict:
    run = db.get_run(run_id)
    if not run:
        return {"error": f"Run not found: {run_id}"}
    notebook_dir = Path(run.notebook_path).parent
    result_path = notebook_dir / f"{run_id}_result.json"
    if not result_path.exists():
        return {"run_id": run_id, "status": "no_result", "outputs": {}}
    try:
        data = json.loads(result_path.read_text())
        return {
            "run_id": run_id,
            "status": data.get("status"),
            "outputs": data.get("outputs", {}),
        }
    except (json.JSONDecodeError, OSError):
        return {"run_id": run_id, "status": "error", "outputs": {}}


def _impl_approve_run(token: str, reason: str = "") -> dict:
    row = db.get_pending_approval(token)
    if not row:
        return {"error": f"Invalid or unknown approval token: {token}"}
    if datetime.now(timezone.utc).isoformat() > row["expires_at"]:
        db.delete_pending_approval(token)
        return {"error": "Approval token has expired (1-hour limit)."}
    db.delete_pending_approval(token)
    return _impl_run_python(
        code=row["code"],
        description=row["description"],
        timeout_seconds=row["timeout_seconds"],
        sandbox=bool(row["sandbox"]),
        packages=json.loads(row["packages"]),
        dry_run=False,
        require_approval=False,  # already approved; skip gate
    )


def _impl_list_pending_approvals() -> dict:
    rows = db.list_pending_approvals()
    now = datetime.now(timezone.utc).isoformat()
    pending = [
        {
            "run_id": r["run_id"],
            "approval_token": r["token"],
            "description": r["description"],
            "created_at": r["created_at"],
            "expires_at": r["expires_at"],
            "expired": r["expires_at"] < now,
            "critical_finding_count": sum(
                1
                for f in json.loads(r["risk_findings"])
                if f["severity"] == "critical"
            ),
        }
        for r in rows
    ]
    return {"count": len(pending), "pending": pending}


# ── Tools ─────────────────────────────────────────────────────────────────────


@mcp.tool()
def run_python(
    code: str,
    description: str = "Python run",
    timeout_seconds: int = 60,
    sandbox: bool = False,
    packages: list[str] | None = None,
    dry_run: bool = False,
    require_approval: bool = False,
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
        dry_run:          If True, return static risk analysis only — do not execute.
        require_approval: If True, block execution when critical risk patterns are found
                          and return an approval_token to confirm via approve_run().

    Returns:
        run_id, status, stdout, stderr, error, duration_ms,
        notebook_path, view_command, code_hash, artifacts,
        risk_findings (if any), packages_installed (if any), freeze (if any).
    """
    return _impl_run_python(
        code, description, timeout_seconds, sandbox, packages,
        dry_run=dry_run, require_approval=require_approval,
    )


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


@mcp.tool()
def list_artifacts(run_id: str) -> dict:
    """
    List user-created files in a run's notebook directory.

    Returns every file except the generated notebook.py and the result sidecar JSON.
    These are files your code wrote to disk during execution (e.g. CSVs, images, reports).

    Args:
        run_id:  The run to inspect.

    Returns:
        run_id, artifact_count, artifacts — each entry has path, size_bytes, extension.
    """
    return _impl_list_artifacts(run_id)


@mcp.tool()
def read_artifact(
    run_id: str,
    artifact_path: str,
    max_size_bytes: int = 5_000_000,
) -> dict:
    """
    Read the content of an artifact file created by a run.

    Args:
        run_id:         The run that created the file.
        artifact_path:  Relative path within the run's directory (from list_artifacts).
        max_size_bytes: Size limit in bytes (default 5 MB). Files larger than this
                        are rejected to prevent memory issues.

    Returns:
        For text files: content (str), media_type, size_bytes, is_text=True.
        For binary files: content_base64 (str), media_type, size_bytes, is_text=False.
    """
    return _impl_read_artifact(run_id, artifact_path, max_size_bytes)


@mcp.tool()
def get_run_outputs(run_id: str) -> dict:
    """
    Retrieve the structured __outputs__ dict written by the run.

    Runs can expose typed data to agents by populating the `__outputs__` dict
    inside their code (e.g. `__outputs__["result"] = df.to_dict()`).
    This tool reads the sidecar JSON to return those values directly.

    Args:
        run_id:  The run to read outputs from.

    Returns:
        run_id, status, outputs (dict). If the run hasn't completed successfully,
        status is 'no_result' and outputs is empty.
    """
    return _impl_get_run_outputs(run_id)


@mcp.tool()
def approve_run(approval_token: str, reason: str = "") -> dict:
    """
    Confirm a blocked run and execute it.

    When run_python is called with require_approval=True and the code contains
    critical risk patterns, execution is blocked and an approval_token is returned.
    Call this tool with that token to proceed with execution.

    Tokens expire after 1 hour.

    Args:
        approval_token:  The token returned by run_python when status='awaiting_confirmation'.
        reason:          Optional note explaining why you approved this run.

    Returns:
        Same as run_python on success, or error if the token is invalid/expired.
    """
    return _impl_approve_run(approval_token, reason)


@mcp.tool()
def list_pending_approvals() -> dict:
    """
    List all runs currently awaiting approval.

    Returns:
        count, pending — each entry has run_id, approval_token, description,
        created_at, expires_at, expired, critical_finding_count.
    """
    return _impl_list_pending_approvals()


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    mcp.run()
