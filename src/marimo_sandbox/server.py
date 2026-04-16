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
cancel_run              Cancel a running async run.
diff_runs               Compare two runs: code, env, status, artifacts, and outputs.
list_environments       List cached virtual environments.
clean_environments      Delete old cached virtual environments.
"""

import base64
import difflib
import hashlib
import json
import mimetypes
import os
import secrets
import shutil
import signal
import socket
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastmcp import FastMCP

from .database import Database
from .env_manager import EnvManager
from .executor import NotebookExecutor
from .generator import GeneratedNotebook, NotebookGenerator
from .models import RunRecord, RunStatus

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
env_manager = EnvManager(DATA_DIR / "envs")

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
        "Use require_approval=True to block runs with critical risk findings. "
        "Use async_mode=True to launch in background; poll with get_run; cancel with cancel_run."
        " Use list_environments / clean_environments to manage the venv cache."
        " Use diff_runs(run_id, compare_to=None) to compare two runs:"
        " code, env, status, artifacts, and outputs."
    ),
)


# ── Background watcher (async mode) ───────────────────────────────────────────


def _watch_run(
    run_id: str,
    process: "subprocess.Popen[str]",
    notebook: GeneratedNotebook,
    timeout_seconds: int,
    start_ms: float,
) -> None:
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    duration_ms = int((time.monotonic() - start_ms) * 1000)
    stdout = process.stdout.read() if process.stdout else ""
    stderr = process.stderr.read() if process.stderr else ""
    result = executor._finish_result(
        notebook, process.returncode or 0, stdout, stderr, duration_ms
    )
    artifacts = _scan_artifacts(notebook.notebook_dir, run_id)
    db.update_run(
        run_id,
        status=result.status,
        duration_ms=result.duration_ms,
        stdout=result.stdout,
        stderr=result.stderr,
        error=result.error,
        artifacts=artifacts or None,
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


def _inject_pep723_header(notebook_path: str, packages: list[str]) -> None:
    """Prepend PEP 723 inline script metadata to *notebook_path*.

    This tells ``marimo edit`` (via uv) which packages to install so the
    notebook opens with a working kernel instead of showing "kernel not found".
    """
    path = Path(notebook_path)
    if not path.exists():
        return
    content = path.read_text(encoding="utf-8")
    if "# /// script" in content:
        return
    # Pin marimo to the installed version so --sandbox doesn't upgrade it and
    # cause a notebook-format mismatch ("kernel not found" in the browser).
    try:
        import marimo as _marimo
        marimo_pin = f"marimo=={_marimo.__version__}"
    except Exception:
        marimo_pin = "marimo"
    pinned = [marimo_pin if p == "marimo" else p for p in packages]
    dep_lines = "\n".join(f'#     "{pkg}",' for pkg in pinned)
    header = (
        "# /// script\n"
        "# requires-python = \">=3.11\"\n"
        "# dependencies = [\n"
        f"{dep_lines}\n"
        "# ]\n"
        "# ///\n"
    )
    path.write_text(header + content, encoding="utf-8")


def _impl_run_python(
    code: str,
    description: str,
    timeout_seconds: int,
    sandbox: bool,
    packages: list[str] | None,
    dry_run: bool = False,
    require_approval: bool = False,
    parent_run_id: str | None = None,
    async_mode: bool = False,
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

    # Resolve packages → hash-based venv (or bare interpreter)
    freeze: str | None = None
    python_path: Path | None = None
    env_hash: str | None = None
    if packages:
        try:
            env_info = env_manager.get_or_create(packages)
            freeze = env_info.freeze or None
            python_path = env_info.python_path
            env_hash = env_info.env_hash
        except Exception as exc:
            return {
                "run_id": None,
                "status": "error",
                "error": f"Package install failed: {exc}",
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

    # Inject PEP 723 metadata so `marimo edit` can find the right kernel
    if packages:
        _inject_pep723_header(str(notebook.notebook_path), packages)

    if async_mode:
        # Persist as 'running' immediately
        db.create_run(
            run_id=run_id,
            description=description,
            code=code,
            notebook_path=str(notebook.notebook_path),
            packages=packages,
            code_hash=code_hash,
            parent_run_id=parent_run_id,
            status="running",
            env_hash=env_hash,
        )
        start_ms = time.monotonic()
        process = executor.execute_async(
            notebook=notebook,
            timeout_seconds=timeout_seconds,
            sandbox=sandbox,
            python_path=python_path,
        )
        db.update_run_pid(run_id, process.pid)
        threading.Thread(
            target=_watch_run,
            args=(run_id, process, notebook, timeout_seconds, start_ms),
            daemon=True,
        ).start()
        response: dict = {
            "run_id": run_id,
            "status": "running",
            "notebook_path": str(notebook.notebook_path),
            "view_command": f'marimo edit "{notebook.notebook_path}"',
            "code_hash": code_hash,
        }
        if packages:
            response["packages_installed"] = packages
        if freeze:
            response["freeze"] = freeze
        if findings:
            response["risk_findings"] = findings_dicts
        return response

    # Persist pending record before executing (so it's visible even if we crash)
    db.create_run(
        run_id=run_id,
        description=description,
        code=code,
        notebook_path=str(notebook.notebook_path),
        packages=packages,
        code_hash=code_hash,
        parent_run_id=parent_run_id,
        env_hash=env_hash,
    )

    # Execute
    result = executor.execute(
        notebook=notebook,
        timeout_seconds=timeout_seconds,
        sandbox=sandbox,
        python_path=python_path,
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

    response = {
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


def _build_explanation(
    run_a: RunRecord,
    run_b: RunRecord,
    relationship: str,
    summary: dict,
    code_diff: dict,
    env_diff: dict,
    status_diff: dict,
    artifact_diff: dict,
    output_diff: dict,
) -> str:
    parts = []
    rel_label = {
        "parent_child": f"Run {run_b.run_id} is a direct rerun of {run_a.run_id}.",
        "siblings": f"Runs {run_a.run_id} and {run_b.run_id} are siblings from the same parent.",
        "unrelated": f"Runs {run_a.run_id} and {run_b.run_id} are unrelated.",
    }[relationship]
    parts.append(rel_label)
    if summary["code_changed"]:
        parts.append(
            f"The code changed ({code_diff['lines_added']} line(s) added, "
            f"{code_diff['lines_removed']} removed)."
        )
        if code_diff.get("diff_text"):
            parts.append(f"Diff:\n{code_diff['diff_text']}")
    else:
        parts.append("The code did not change.")
    if summary["env_changed"]:
        added = env_diff["packages_added"]
        removed = env_diff["packages_removed"]
        env_parts = []
        if added:
            env_parts.append(f"added {', '.join(added)}")
        if removed:
            env_parts.append(f"removed {', '.join(removed)}")
        parts.append(f"The environment changed ({'; '.join(env_parts)}).")
    else:
        parts.append("The environment did not change.")
    if summary["status_changed"]:
        parts.append(f"Status changed from {status_diff['before']} to {status_diff['after']}.")
    if summary["artifacts_changed"]:
        a_parts = []
        if artifact_diff["added"]:
            a_parts.append(f"{len(artifact_diff['added'])} artifact(s) added")
        if artifact_diff["removed"]:
            a_parts.append(f"{len(artifact_diff['removed'])} removed")
        parts.append(f"Artifacts changed ({', '.join(a_parts)}).")
    if output_diff.get("available") and summary["outputs_changed"]:
        n = len(output_diff.get("changed_keys", {}))
        parts.append(f"{n} output field(s) changed.")
    return " ".join(parts)


def _impl_diff_runs(run_id: str, compare_to: str | None = None) -> dict:
    # 1. Fetch run_b (the run being inspected)
    run_b = db.get_run(run_id)
    if not run_b:
        return {"error": f"Run not found: {run_id}"}

    # 2. Resolve reference run
    ref_id = compare_to or run_b.parent_run_id
    if not ref_id:
        return {
            "error": (
                f"Run {run_id} has no parent_run_id. "
                "Provide an explicit compare_to run ID."
            )
        }

    # 3. Fetch run_a (the reference / older run)
    run_a = db.get_run(ref_id)
    if not run_a:
        return {"error": f"Reference run not found: {ref_id}"}

    # 4. Relationship classification
    if run_b.parent_run_id == run_a.run_id:
        relationship = "parent_child"
    elif run_a.parent_run_id is not None and run_a.parent_run_id == run_b.parent_run_id:
        relationship = "siblings"
    else:
        relationship = "unrelated"

    # 5. Code diff
    code_hash_changed = run_a.code_hash != run_b.code_hash
    lines_added = 0
    lines_removed = 0
    diff_text: str | None = None
    if code_hash_changed:
        a_lines = (run_a.code or "").splitlines(keepends=True)
        b_lines = (run_b.code or "").splitlines(keepends=True)
        diff_lines = list(difflib.unified_diff(
            a_lines, b_lines,
            fromfile=f"{run_a.run_id}/code",
            tofile=f"{run_b.run_id}/code",
        ))
        for line in diff_lines:
            if line.startswith("+") and not line.startswith("+++"):
                lines_added += 1
            elif line.startswith("-") and not line.startswith("---"):
                lines_removed += 1
        diff_text = "".join(diff_lines) or None
        if diff_text and len(diff_text) > 8000:
            diff_text = diff_text[:8000] + "\n... (truncated)"
    code_diff = {
        "changed": code_hash_changed,
        "hash_before": run_a.code_hash,
        "hash_after": run_b.code_hash,
        "lines_added": lines_added,
        "lines_removed": lines_removed,
        "diff_text": diff_text,
    }

    # 6. Env diff
    env_hash_changed = run_a.env_hash != run_b.env_hash
    pkgs_before = set(run_a.packages)
    pkgs_after = set(run_b.packages)
    env_diff = {
        "changed": env_hash_changed,
        "hash_before": run_a.env_hash,
        "hash_after": run_b.env_hash,
        "packages_before": sorted(run_a.packages),
        "packages_after": sorted(run_b.packages),
        "packages_added": sorted(pkgs_after - pkgs_before),
        "packages_removed": sorted(pkgs_before - pkgs_after),
    }

    # 7. Status diff
    status_diff = {
        "before": str(run_a.status),
        "after": str(run_b.status),
        "changed": run_a.status != run_b.status,
    }

    # 8. Artifact diff
    artifacts_a = set(run_a.artifacts)
    artifacts_b = set(run_b.artifacts)
    artifact_diff = {
        "changed": artifacts_a != artifacts_b,
        "added": sorted(artifacts_b - artifacts_a),
        "removed": sorted(artifacts_a - artifacts_b),
        "common": sorted(artifacts_a & artifacts_b),
    }

    # 9. Output diff — read sidecar JSONs from disk
    def _read_sidecar_outputs(run: RunRecord) -> dict | None:
        nb_dir = Path(run.notebook_path).parent
        result_path = nb_dir / f"{run.run_id}_result.json"
        if not result_path.exists():
            return None
        try:
            data: dict = json.loads(result_path.read_text())
            return dict(data.get("outputs", {}))
        except (json.JSONDecodeError, OSError):
            return None

    outputs_a = _read_sidecar_outputs(run_a)
    outputs_b = _read_sidecar_outputs(run_b)
    if outputs_a is not None and outputs_b is not None:
        keys_a = set(outputs_a.keys())
        keys_b = set(outputs_b.keys())
        added_keys = sorted(keys_b - keys_a)
        removed_keys = sorted(keys_a - keys_b)
        changed_keys: dict = {}
        for key in keys_a & keys_b:
            if outputs_a[key] != outputs_b[key]:
                changed_keys[key] = {"before": outputs_a[key], "after": outputs_b[key]}
        output_diff: dict = {
            "available": True,
            "changed": bool(added_keys or removed_keys or changed_keys),
            "added_keys": added_keys,
            "removed_keys": removed_keys,
            "changed_keys": changed_keys,
        }
    else:
        output_diff = {
            "available": False,
            "changed": False,
            "added_keys": [],
            "removed_keys": [],
            "changed_keys": {},
        }

    # 10. Duration diff (flag if >20% change)
    before_ms = run_a.duration_ms
    after_ms = run_b.duration_ms
    delta_ms: int | None
    if before_ms is not None and after_ms is not None:
        delta_ms = after_ms - before_ms
        if before_ms == 0:
            duration_changed = after_ms != 0
        else:
            duration_changed = abs(delta_ms) / before_ms > 0.20
    else:
        delta_ms = None
        duration_changed = False
    duration_diff = {
        "before_ms": before_ms,
        "after_ms": after_ms,
        "delta_ms": delta_ms,
        "changed": duration_changed,
    }

    # 11. Summary flags
    summary = {
        "code_changed": code_diff["changed"],
        "env_changed": env_diff["changed"],
        "status_changed": status_diff["changed"],
        "artifacts_changed": artifact_diff["changed"],
        "outputs_changed": output_diff["changed"],
        "duration_changed": duration_diff["changed"],
    }

    # 12. Plain-English explanation
    explanation = _build_explanation(
        run_a=run_a,
        run_b=run_b,
        relationship=relationship,
        summary=summary,
        code_diff=code_diff,
        env_diff=env_diff,
        status_diff=status_diff,
        artifact_diff=artifact_diff,
        output_diff=output_diff,
    )

    return {
        "run_a": run_a.run_id,
        "run_b": run_b.run_id,
        "relationship": relationship,
        "summary": summary,
        "status_diff": status_diff,
        "code_diff": code_diff,
        "env_diff": env_diff,
        "artifact_diff": artifact_diff,
        "output_diff": output_diff,
        "duration_diff": duration_diff,
        "explanation": explanation,
    }


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
        parent_run_id=run_id,
    )


def _impl_purge_runs(older_than_days: int, delete_files: bool, dry_run: bool = False) -> dict:
    if dry_run:
        rows = db.list_runs_older_than(older_than_days)
        return {
            "dry_run": True,
            "would_delete_runs": len(rows),
            "run_ids": [r.run_id for r in rows],
        }
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


def _impl_list_environments() -> dict:
    envs = env_manager.list_envs()
    return {
        "count": len(envs),
        "environments": [
            {
                "env_hash": e.env_hash,
                "packages": e.packages,
                "size_bytes": e.size_bytes,
                "created_at": e.created_at,
                "last_used_at": e.last_used_at,
            }
            for e in envs
        ],
    }


def _impl_clean_environments(older_than_days: int = 90) -> dict:
    envs_before = env_manager.list_envs()
    sizes = {e.env_hash: e.size_bytes for e in envs_before}
    deleted = env_manager.clean_old_envs(older_than_days)
    freed_bytes = sum(sizes.get(h, 0) for h in deleted)
    return {
        "deleted_count": len(deleted),
        "deleted_hashes": deleted,
        "freed_bytes": freed_bytes,
    }


def _impl_cancel_run(run_id: str) -> dict:
    run = db.get_run(run_id)
    if not run:
        return {"error": f"Run not found: {run_id}"}
    if run.status != RunStatus.RUNNING:
        return {"error": f"Run is not running (status: {run.status})"}
    if run.pid:
        try:
            os.kill(run.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass  # already finished
    db.update_run(run_id, status="cancelled", duration_ms=0)
    return {"success": True, "run_id": run_id, "pid": run.pid}


def _impl_list_pending_approvals() -> dict:
    db.purge_expired_approvals()
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
    async_mode: bool = False,
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
        async_mode:       If True, launch execution in the background and return
                          immediately with status="running". Poll with get_run();
                          cancel with cancel_run().

    Returns:
        run_id, status, stdout, stderr, error, duration_ms,
        notebook_path, view_command, code_hash, artifacts,
        risk_findings (if any), packages_installed (if any), freeze (if any).
        When async_mode=True: run_id, status="running", notebook_path, view_command, code_hash.
    """
    return _impl_run_python(
        code, description, timeout_seconds, sandbox, packages,
        dry_run=dry_run, require_approval=require_approval, async_mode=async_mode,
    )


# ── Helpers for open_notebook ────────────────────────────────────────────────


def _port_is_open(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _server_is_healthy(port: int) -> bool:
    """Return True when marimo's HTTP server is fully ready (not just TCP open)."""
    import json as _json
    import urllib.error
    import urllib.request

    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/health",
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            if resp.status != 200:
                return False
            body = _json.loads(resp.read())
            return bool(body.get("status") == "healthy")
    except Exception:
        return False


def _free_port(port: int) -> None:
    """Kill any process bound to *port* so marimo can claim it."""
    try:
        result = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        for pid_str in result.stdout.strip().split():
            try:
                os.kill(int(pid_str), signal.SIGTERM)
            except (ProcessLookupError, ValueError):
                pass
        if result.stdout.strip():
            time.sleep(0.75)
            # SIGKILL fallback: if port is still occupied, force-kill stragglers
            if _port_is_open(port):
                try:
                    retry = subprocess.run(
                        ["lsof", "-ti", f"tcp:{port}"],
                        capture_output=True,
                        text=True,
                        timeout=3,
                    )
                    for pid_str in retry.stdout.strip().split():
                        try:
                            os.kill(int(pid_str), signal.SIGKILL)
                        except (ProcessLookupError, ValueError):
                            pass
                except Exception:
                    pass
    except Exception:
        pass


def _impl_open_notebook(run_id: str, port: int = 2718) -> dict:
    """Core logic for open_notebook, shared by marimo-sandbox and snowbox."""
    run = db.get_run(run_id)
    if not run:
        return {"success": False, "error": f"Run not found: {run_id}"}

    notebook_path = Path(run.notebook_path)
    if not notebook_path.exists():
        return {
            "success": False,
            "error": f"Notebook file not found at {notebook_path}",
        }

    # Kill any existing server on this port so we don't accidentally return
    # success against a stale process serving a different notebook.
    _free_port(port)

    # ── Pre-flight: resolve marimo binary and venv ───────────────────────
    env = os.environ.copy()
    marimo_bin = "marimo"

    if run.env_hash:
        venv_dir = DATA_DIR / "envs" / run.env_hash
        if not venv_dir.exists():
            return {
                "success": False,
                "error": (
                    f"Virtual environment not found at {venv_dir}. "
                    "It may have been cleaned. Re-run with the same packages "
                    "to recreate it, or use clean_environments to check."
                ),
            }
        venv_bin = venv_dir / "bin"
        if venv_bin.exists():
            env["VIRTUAL_ENV"] = str(venv_dir)
            env["PATH"] = str(venv_bin) + os.pathsep + env.get("PATH", "")
            # Prefer the venv's marimo if available
            venv_marimo = venv_bin / "marimo"
            if venv_marimo.exists():
                marimo_bin = str(venv_marimo)

    # Verify marimo is runnable
    version = executor.get_marimo_version(marimo_bin)
    if version is None:
        return {
            "success": False,
            "error": (
                f"marimo not found or not runnable at '{marimo_bin}'. "
                "Run check_setup to diagnose, or install with: pip install marimo"
            ),
        }

    # ── Launch marimo edit ───────────────────────────────────────────────
    process = subprocess.Popen(
        [marimo_bin, "edit", str(notebook_path), "--port", str(port), "--no-token", "--no-sandbox"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raw_out = process.stdout.read() if process.stdout else b""
            raw_err = process.stderr.read() if process.stderr else b""
            stdout_text = raw_out.decode(errors="replace")[:200]
            stderr_text = raw_err.decode(errors="replace")[:400]
            detail = stderr_text or stdout_text or "(no output captured)"
            return {
                "success": False,
                "error": (
                    f"marimo exited with code {exit_code}: {detail}  "
                    "Hint: run check_setup to diagnose."
                ),
            }
        if _server_is_healthy(port):
            return {
                "success": True,
                "url": f"http://127.0.0.1:{port}",
                "pid": process.pid,
                "notebook_path": str(notebook_path),
                "message": "Notebook is open. Navigate to the URL to view it.",
            }
        time.sleep(0.25)

    # Timeout — capture whatever stderr is available before killing
    raw_err = b""
    if process.stderr:
        import selectors
        sel = selectors.DefaultSelector()
        sel.register(process.stderr, selectors.EVENT_READ)
        if sel.select(timeout=0):
            raw_err = process.stderr.read1(4096) if hasattr(process.stderr, "read1") else b""
        sel.close()
    stderr_hint = raw_err.decode(errors="replace")[:300]
    process.terminate()
    msg = "marimo did not become ready within 15 seconds."
    if stderr_hint:
        msg += f" stderr: {stderr_hint}"
    msg += " Hint: run check_setup to diagnose."
    return {"success": False, "error": msg}


@mcp.tool()
def open_notebook(run_id: str, port: int = 2718) -> dict:
    """
    Open a run's Marimo notebook in the interactive editor.

    Activates the run's cached virtualenv (which already has all required
    packages installed) before launching ``marimo edit --no-sandbox``.

    ``--no-sandbox`` is required: if the notebook contains a PEP 723
    ``# /// script`` header, marimo auto-detects it and triggers uv sandbox
    mode.  That uv-managed environment conflicts with our pre-activated venv
    and causes "kernel not found".  ``--no-sandbox`` overrides the
    auto-detection and uses the venv's Python for the kernel.

    Any existing process already occupying *port* is killed first so the new
    server always serves the requested run.

    Args:
        run_id:  The run ID returned by run_python.
        port:    Local port for the Marimo server (default 2718).

    Returns:
        success, url, pid, notebook_path, message — or success=False + error.
    """
    return _impl_open_notebook(run_id, port)


def _impl_list_runs(
    limit: int = 20,
    status: str | None = None,
    offset: int = 0,
) -> dict:
    status_enum = RunStatus(status) if status else None
    runs = db.list_runs(limit=limit, status=status_enum, offset=offset)
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
    return {
        "total": db.count_runs(status=status_enum),
        "count": len(slim),
        "offset": offset,
        "runs": slim,
    }


@mcp.tool()
def list_runs(
    limit: int = 20,
    status: str | None = None,
    offset: int = 0,
) -> dict:
    """
    List recent Python runs.

    Args:
        limit:   Max number of runs to return (default 20).
        status:  Filter to 'success', 'error', or 'pending'. Omit for all.
        offset:  Number of runs to skip (for pagination, default 0).

    Returns:
        total, count, offset, runs — each entry has run_id, description, status,
        duration_ms, created_at, notebook_path.
    """
    return _impl_list_runs(limit, status, offset)


def _impl_get_run(
    run_id: str,
    include_code: bool = True,
    include_notebook_source: bool = False,
) -> dict:
    run = db.get_run(run_id)
    if not run:
        return {"error": f"Run not found: {run_id}"}

    result: dict[str, object] = {
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
        "parent_run_id": run.parent_run_id,
    }

    result["code_hash"] = run.code_hash
    result["env_hash"] = run.env_hash
    result["freeze"] = run.freeze
    result["risk_findings"] = run.risk_findings

    if include_code:
        result["code"] = run.code

    if include_notebook_source:
        nb_path = Path(run.notebook_path)
        result["notebook_source"] = (
            nb_path.read_text(encoding="utf-8") if nb_path.exists() else None
        )

    return result


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
    return _impl_get_run(run_id, include_code, include_notebook_source)


def _impl_check_setup() -> dict:
    marimo_ok = executor.check_marimo()
    docker_ok = executor.check_docker()

    # Version introspection
    system_version = executor.get_marimo_version()
    try:
        import marimo as _marimo
        library_version: str | None = _marimo.__version__
    except Exception:
        library_version = None

    notes: list[str] = []
    if not marimo_ok:
        notes.append("marimo is not installed or not on PATH. Run: pip install marimo")
    if not docker_ok:
        notes.append(
            "Docker is not available. sandbox=True in run_python will fail. "
            "Install Docker Desktop to enable sandboxed execution."
        )
    if (
        system_version
        and library_version
        and system_version != library_version
    ):
        notes.append(
            f"VERSION MISMATCH: system marimo is {system_version} but the "
            f"Python library is {library_version}. This can cause 'kernel not "
            f"found' errors. Fix: pip install marimo=={system_version}  OR  "
            f"pipx upgrade marimo"
        )
    notes.append(
        "If notebooks show 'kernel not found', try a hard refresh "
        "(Cmd+Shift+R / Ctrl+Shift+R) to clear the browser cache."
    )

    return {
        "data_dir": str(DATA_DIR),
        "marimo_available": marimo_ok,
        "marimo_system_version": system_version,
        "marimo_library_version": library_version,
        "docker_available": docker_ok,
        "uv_available": executor.check_uv(),
        "total_runs": db.count_runs(),
        "ready": marimo_ok,
        "notes": notes,
    }


@mcp.tool()
def check_setup() -> dict:
    """
    Check that the sandbox environment is ready.

    Returns the data directory, whether marimo and Docker are available,
    total run count, version info, and any setup notes.
    """
    return _impl_check_setup()


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
def purge_runs(
    older_than_days: int = 30,
    delete_files: bool = True,
    dry_run: bool = False,
) -> dict:
    """
    Bulk-delete runs older than N days to reclaim disk space.

    Args:
        older_than_days:  Delete runs created more than this many days ago (default 30).
        delete_files:     Also remove notebook directories from disk (default True).
        dry_run:          If True, return a preview of what would be deleted without
                          actually deleting anything (default False).

    Returns:
        When dry_run=False: deleted_runs, files_deleted, run_ids.
        When dry_run=True:  dry_run=True, would_delete_runs, run_ids.
    """
    return _impl_purge_runs(older_than_days, delete_files, dry_run=dry_run)


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


@mcp.tool()
def cancel_run(run_id: str) -> dict:
    """
    Cancel a run that is currently executing (async_mode=True).

    Sends SIGTERM to the process and marks the run as 'cancelled' in the DB.

    Args:
        run_id:  The run to cancel (must have status 'running').

    Returns:
        success, run_id, pid — or error if the run is not found or not running.
    """
    return _impl_cancel_run(run_id)


@mcp.tool()
def diff_runs(run_id: str, compare_to: str | None = None) -> dict:
    """
    Compare two runs and explain what changed between them.

    By default compares run_id against its parent (the run it was re-executed from).
    Supply compare_to to override the reference run — either run can have any status.

    Args:
        run_id:     The run to inspect (the "after" / newer run).
        compare_to: ID of the reference run (the "before" / older run).
                    Defaults to run_id's parent_run_id. Required if the run has no parent.

    Returns:
        run_a, run_b            — reference and inspected run IDs.
        relationship            — "parent_child", "siblings", or "unrelated".
        summary                 — dict of bool flags: code_changed, env_changed,
                                  status_changed, artifacts_changed, outputs_changed,
                                  duration_changed.
        status_diff             — before/after status strings and changed flag.
        code_diff               — changed, hash_before, hash_after, lines_added, lines_removed.
        env_diff                — changed, hash_before, hash_after, packages_before,
                                  packages_after, packages_added, packages_removed.
        artifact_diff           — changed, added, removed, common (lists of relative paths).
        output_diff             — available, changed, added_keys, removed_keys, changed_keys
                                  (shallow comparison of __outputs__ dict; available=False if
                                  either run has no result sidecar).
        duration_diff           — before_ms, after_ms, delta_ms, changed (>20% threshold).
        explanation             — 1-3 sentence plain-English summary of what changed.
    """
    return _impl_diff_runs(run_id, compare_to)


@mcp.tool()
def list_environments() -> dict:
    """
    List cached virtual environments (hash-based venv cache).

    Each environment corresponds to a unique set of packages. Environments are
    reused automatically when run_python is called with the same package list.

    Returns:
        count, environments — each entry has env_hash, packages, size_bytes,
        created_at, last_used_at.
    """
    return _impl_list_environments()


@mcp.tool()
def clean_environments(older_than_days: int = 90) -> dict:
    """
    Delete cached virtual environments that haven't been used recently.

    Args:
        older_than_days:  Delete envs whose last_used_at is older than this many
                          days (default 90).

    Returns:
        deleted_count, deleted_hashes, freed_bytes.
    """
    return _impl_clean_environments(older_than_days)


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    mcp.run()
