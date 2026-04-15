"""Unit tests for server-level tools: delete_run, rerun, purge_runs, artifacts, approval gates."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import marimo_sandbox.server as server_module
from marimo_sandbox.models import DeletedRunInfo, RunRecord, RunStatus

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_run(tmp_path: Path, run_id: str = "run_abc") -> RunRecord:
    fake_nb = tmp_path / "notebooks" / run_id / "notebook.py"
    fake_nb.parent.mkdir(parents=True, exist_ok=True)
    fake_nb.touch()
    return RunRecord(
        run_id=run_id,
        code="print('hi')",
        description="My run",
        packages=[],
        status=RunStatus.PENDING,
        notebook_path=str(fake_nb),
        created_at="2026-01-01 00:00:00",
    )


# ── delete_run ────────────────────────────────────────────────────────────────


def test_delete_run_not_found():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = None
        result = server_module._impl_delete_run("run_missing")
    assert result["success"] is False
    assert "run_missing" in result["error"]


def test_delete_run_no_file_deletion(tmp_path):
    run = _make_run(tmp_path)
    with patch.object(server_module, "db") as mock_db, \
         patch("marimo_sandbox.server.shutil.rmtree") as mock_rmtree:
        mock_db.get_run.return_value = run
        mock_db.delete_run.return_value = True
        result = server_module._impl_delete_run("run_abc", delete_files=False)

    assert result["success"] is True
    assert result["files_deleted"] is False
    mock_rmtree.assert_not_called()


def test_delete_run_deletes_directory(tmp_path):
    run = _make_run(tmp_path)
    with patch.object(server_module, "db") as mock_db, \
         patch("marimo_sandbox.server.shutil.rmtree") as mock_rmtree:
        mock_db.get_run.return_value = run
        mock_db.delete_run.return_value = True
        result = server_module._impl_delete_run("run_abc", delete_files=True)

    assert result["success"] is True
    assert result["files_deleted"] is True
    mock_rmtree.assert_called_once()


# ── rerun ─────────────────────────────────────────────────────────────────────


def test_rerun_not_found():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = None
        result = server_module._impl_rerun(
            "run_missing", code=None, description=None,
            timeout_seconds=60, sandbox=False, packages=None,
        )
    assert "error" in result
    assert "run_missing" in result["error"]


def test_rerun_uses_original_code():
    mock_rp = MagicMock(return_value={"status": "success"})
    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module, "_impl_run_python", mock_rp):
        mock_db.get_run.return_value = RunRecord(
            run_id="run_abc",
            code="print('original')",
            description="My run",
            packages=[],
            status=RunStatus.PENDING,
            notebook_path="/tmp/nb.py",
            created_at="2026-01-01 00:00:00",
        )
        server_module._impl_rerun(
            "run_abc", code=None, description=None,
            timeout_seconds=60, sandbox=False, packages=None,
        )

    mock_rp.assert_called_once()
    call_kwargs = mock_rp.call_args.kwargs
    assert call_kwargs["code"] == "print('original')"


def test_rerun_uses_stored_packages():
    mock_rp = MagicMock(return_value={"status": "success"})
    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module, "_impl_run_python", mock_rp):
        mock_db.get_run.return_value = RunRecord(
            run_id="run_abc",
            code="import requests",
            description="My run",
            packages=["requests"],
            status=RunStatus.PENDING,
            notebook_path="/tmp/nb.py",
            created_at="2026-01-01 00:00:00",
        )
        server_module._impl_rerun(
            "run_abc", code=None, description=None,
            timeout_seconds=60, sandbox=False, packages=None,
        )

    mock_rp.assert_called_once()
    call_kwargs = mock_rp.call_args.kwargs
    assert call_kwargs["packages"] == ["requests"]


def test_rerun_explicit_packages_override():
    mock_rp = MagicMock(return_value={"status": "success"})
    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module, "_impl_run_python", mock_rp):
        mock_db.get_run.return_value = RunRecord(
            run_id="run_abc",
            code="import requests",
            description="My run",
            packages=["requests"],
            status=RunStatus.PENDING,
            notebook_path="/tmp/nb.py",
            created_at="2026-01-01 00:00:00",
        )
        server_module._impl_rerun(
            "run_abc", code=None, description=None,
            timeout_seconds=60, sandbox=False, packages=["httpx"],
        )

    mock_rp.assert_called_once()
    call_kwargs = mock_rp.call_args.kwargs
    assert call_kwargs["packages"] == ["httpx"]


# ── purge_runs ────────────────────────────────────────────────────────────────


def test_purge_runs_no_rows():
    with patch.object(server_module, "db") as mock_db:
        mock_db.delete_runs_older_than.return_value = []
        result = server_module._impl_purge_runs(older_than_days=30, delete_files=True)

    assert result["deleted_runs"] == 0
    assert result["files_deleted"] == 0
    assert result["run_ids"] == []


def test_purge_runs_deletes_files(tmp_path):
    dir1 = tmp_path / "run_old1"
    dir2 = tmp_path / "run_old2"
    dir1.mkdir()
    dir2.mkdir()

    with patch.object(server_module, "db") as mock_db, \
         patch("marimo_sandbox.server.shutil.rmtree") as mock_rmtree:
        mock_db.delete_runs_older_than.return_value = [
            DeletedRunInfo(run_id="run_old1", notebook_path=str(dir1 / "nb.py")),
            DeletedRunInfo(run_id="run_old2", notebook_path=str(dir2 / "nb.py")),
        ]
        result = server_module._impl_purge_runs(older_than_days=30, delete_files=True)

    assert result["deleted_runs"] == 2
    assert result["files_deleted"] == 2
    assert mock_rmtree.call_count == 2


# ── list_artifacts ────────────────────────────────────────────────────────────


def test_list_artifacts_run_not_found():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = None
        result = server_module._impl_list_artifacts("run_missing")
    assert "error" in result
    assert "run_missing" in result["error"]


def test_list_artifacts_empty(tmp_path):
    run = _make_run(tmp_path)
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_list_artifacts(run.run_id)
    assert result["artifact_count"] == 0
    assert result["artifacts"] == []


def test_list_artifacts_with_files(tmp_path):
    run = _make_run(tmp_path)
    nb_dir = Path(run.notebook_path).parent
    # Create user-generated files
    (nb_dir / "output.csv").write_text("a,b\n1,2\n")
    (nb_dir / "chart.png").write_bytes(b"\x89PNG\r\n")

    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_list_artifacts(run.run_id)

    assert result["artifact_count"] == 2
    paths = {a["path"] for a in result["artifacts"]}
    assert "output.csv" in paths
    assert "chart.png" in paths
    # Verify metadata fields
    for a in result["artifacts"]:
        assert "size_bytes" in a
        assert "extension" in a
    csv_info = next(a for a in result["artifacts"] if a["path"] == "output.csv")
    assert csv_info["extension"] == ".csv"
    assert csv_info["size_bytes"] > 0


# ── read_artifact ─────────────────────────────────────────────────────────────


def test_read_artifact_run_not_found():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = None
        result = server_module._impl_read_artifact("run_missing", "file.txt")
    assert "error" in result
    assert "run_missing" in result["error"]


def test_read_artifact_text(tmp_path):
    run = _make_run(tmp_path)
    nb_dir = Path(run.notebook_path).parent
    (nb_dir / "report.csv").write_text("col1,col2\n1,2\n3,4\n")

    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_read_artifact(run.run_id, "report.csv")

    assert result["is_text"] is True
    assert "col1,col2" in result["content"]
    assert result["size_bytes"] > 0


def test_read_artifact_binary(tmp_path):
    run = _make_run(tmp_path)
    nb_dir = Path(run.notebook_path).parent
    png_bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR"
    (nb_dir / "plot.png").write_bytes(png_bytes)

    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_read_artifact(run.run_id, "plot.png")

    assert result["is_text"] is False
    assert "content_base64" in result
    import base64
    decoded = base64.b64decode(result["content_base64"])
    assert decoded == png_bytes


def test_read_artifact_too_large(tmp_path):
    run = _make_run(tmp_path)
    nb_dir = Path(run.notebook_path).parent
    (nb_dir / "big.bin").write_bytes(b"x" * 100)

    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_read_artifact(run.run_id, "big.bin", max_size_bytes=50)

    assert "error" in result
    assert "too large" in result["error"]


def test_read_artifact_path_traversal(tmp_path):
    run = _make_run(tmp_path)
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_read_artifact(run.run_id, "../etc/passwd")

    assert "error" in result
    assert "traversal" in result["error"].lower() or "Path traversal" in result["error"]


# ── get_run_outputs ───────────────────────────────────────────────────────────


def test_get_run_outputs_run_not_found():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = None
        result = server_module._impl_get_run_outputs("run_missing")
    assert "error" in result


def test_get_run_outputs_no_sidecar(tmp_path):
    run = _make_run(tmp_path)
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_get_run_outputs(run.run_id)

    assert result["run_id"] == run.run_id
    assert result["status"] == "no_result"
    assert result["outputs"] == {}


def test_get_run_outputs_with_data(tmp_path):
    run = _make_run(tmp_path)
    nb_dir = Path(run.notebook_path).parent
    sidecar = {
        "run_id": run.run_id,
        "status": "success",
        "executed_at": "2026-01-01T00:00:00+00:00",
        "outputs": {"total": 42, "labels": ["a", "b"]},
    }
    (nb_dir / f"{run.run_id}_result.json").write_text(json.dumps(sidecar))

    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_get_run_outputs(run.run_id)

    assert result["run_id"] == run.run_id
    assert result["status"] == "success"
    assert result["outputs"]["total"] == 42
    assert result["outputs"]["labels"] == ["a", "b"]


# ── v0.6: dry_run ─────────────────────────────────────────────────────────────


def test_run_python_dry_run_clean_code():
    with patch.object(server_module, "db") as mock_db:
        result = server_module._impl_run_python(
            code="x = 1 + 2\nprint(x)",
            description="Clean",
            timeout_seconds=60,
            sandbox=False,
            packages=[],
            dry_run=True,
        )
    assert result["status"] == "analysis_complete"
    assert result["dry_run"] is True
    assert result["requires_confirmation"] is False
    mock_db.create_run.assert_not_called()


def test_run_python_dry_run_with_subprocess():
    with patch.object(server_module, "db"):
        result = server_module._impl_run_python(
            code="import subprocess\nsubprocess.run(['ls'])",
            description="Dangerous",
            timeout_seconds=60,
            sandbox=False,
            packages=[],
            dry_run=True,
        )
    assert result["status"] == "analysis_complete"
    assert result["dry_run"] is True
    assert result["requires_confirmation"] is True
    critical = [f for f in result["risk_findings"] if f["severity"] == "critical"]
    assert len(critical) >= 1


def test_run_python_dry_run_no_execution():
    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module, "executor") as mock_exec, \
         patch.object(server_module, "generator") as mock_gen:
        server_module._impl_run_python(
            code="x = 1",
            description="test",
            timeout_seconds=60,
            sandbox=False,
            packages=[],
            dry_run=True,
        )
    mock_db.create_run.assert_not_called()
    mock_exec.execute.assert_not_called()
    mock_gen.generate.assert_not_called()


# ── v0.6: require_approval ────────────────────────────────────────────────────


def test_run_python_require_approval_clean_code():
    """Clean code with require_approval=True should execute normally."""
    mock_exec_result = MagicMock()
    mock_exec_result.status = "success"
    mock_exec_result.duration_ms = 10
    mock_exec_result.stdout = "3"
    mock_exec_result.stderr = None
    mock_exec_result.error = None

    mock_nb = MagicMock()
    mock_nb.notebook_path = Path("/tmp/nb.py")
    mock_nb.notebook_dir = Path("/tmp")
    mock_nb.result_path = Path("/tmp/result.json")

    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module, "executor") as mock_exec, \
         patch.object(server_module, "generator") as mock_gen, \
         patch.object(server_module, "_scan_artifacts", return_value=[]):
        mock_gen.generate.return_value = mock_nb
        mock_exec.execute.return_value = mock_exec_result
        mock_exec.install_packages.return_value = {"success": True, "output": "", "freeze": ""}
        result = server_module._impl_run_python(
            code="x = 1 + 2",
            description="Clean",
            timeout_seconds=60,
            sandbox=False,
            packages=[],
            require_approval=True,
        )
    # No critical findings → executes normally
    assert result.get("status") != "awaiting_confirmation"
    mock_db.create_run.assert_called_once()


def test_run_python_require_approval_critical():
    """Code with critical patterns + require_approval=True should block."""
    with patch.object(server_module, "db") as mock_db:
        result = server_module._impl_run_python(
            code="import subprocess\nsubprocess.run(['rm', '-rf', '/'])",
            description="Dangerous",
            timeout_seconds=60,
            sandbox=False,
            packages=[],
            require_approval=True,
        )
    assert result["status"] == "awaiting_confirmation"
    assert "approval_token" in result
    assert result["approval_token"].startswith("approval_")
    assert "critical_findings" in result
    assert len(result["critical_findings"]) >= 1
    mock_db.create_pending_approval.assert_called_once()


# ── v0.6: approve_run ─────────────────────────────────────────────────────────


def test_approve_run_invalid_token():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_pending_approval.return_value = None
        result = server_module._impl_approve_run("bad_token")
    assert "error" in result
    assert "bad_token" in result["error"]


def test_approve_run_expired_token():
    expired_row = {
        "token": "approval_expired",
        "run_id": "run_001",
        "code": "pass",
        "description": "test",
        "packages": "[]",
        "timeout_seconds": 60,
        "sandbox": 0,
        "risk_findings": "[]",
        "expires_at": "2020-01-01T00:00:00+00:00",  # in the past
    }
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_pending_approval.return_value = expired_row
        result = server_module._impl_approve_run("approval_expired")
    assert "error" in result
    assert "expired" in result["error"].lower()
    mock_db.delete_pending_approval.assert_called_once_with("approval_expired")


def test_approve_run_executes_code():
    """Valid token should delete the approval and call _impl_run_python."""
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    valid_row = {
        "token": "approval_valid",
        "run_id": "run_001",
        "code": "x = 42",
        "description": "Approved run",
        "packages": "[]",
        "timeout_seconds": 60,
        "sandbox": 0,
        "risk_findings": '[{"severity": "critical"}]',
        "expires_at": future,
    }
    mock_rp = MagicMock(return_value={"status": "success", "run_id": "run_new"})
    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module, "_impl_run_python", mock_rp):
        mock_db.get_pending_approval.return_value = valid_row
        server_module._impl_approve_run("approval_valid", reason="looks ok")

    mock_db.delete_pending_approval.assert_called_once_with("approval_valid")
    mock_rp.assert_called_once()
    call_kwargs = mock_rp.call_args.kwargs
    assert call_kwargs["require_approval"] is False
    assert call_kwargs["dry_run"] is False
    assert call_kwargs["code"] == "x = 42"


# ── v0.6: list_pending_approvals ─────────────────────────────────────────────


def test_list_pending_approvals_empty():
    with patch.object(server_module, "db") as mock_db:
        mock_db.list_pending_approvals.return_value = []
        result = server_module._impl_list_pending_approvals()
    assert result["count"] == 0
    assert result["pending"] == []


def test_list_pending_approvals_shows_pending():
    from datetime import datetime, timedelta, timezone
    future = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    rows = [
        {
            "token": "approval_1",
            "run_id": "run_001",
            "description": "Run 1",
            "created_at": "2026-01-01 00:00:00",
            "expires_at": future,
            "risk_findings": '[{"severity": "critical"}, {"severity": "critical"}]',
        },
        {
            "token": "approval_2",
            "run_id": "run_002",
            "description": "Run 2",
            "created_at": "2026-01-01 00:01:00",
            "expires_at": "2020-01-01T00:00:00+00:00",  # expired
            "risk_findings": '[{"severity": "critical"}]',
        },
    ]
    with patch.object(server_module, "db") as mock_db:
        mock_db.list_pending_approvals.return_value = rows
        result = server_module._impl_list_pending_approvals()

    assert result["count"] == 2
    tokens = {p["approval_token"] for p in result["pending"]}
    assert tokens == {"approval_1", "approval_2"}

    pending_1 = next(p for p in result["pending"] if p["approval_token"] == "approval_1")
    assert pending_1["expired"] is False
    assert pending_1["critical_finding_count"] == 2

    pending_2 = next(p for p in result["pending"] if p["approval_token"] == "approval_2")
    assert pending_2["expired"] is True
    assert pending_2["critical_finding_count"] == 1
