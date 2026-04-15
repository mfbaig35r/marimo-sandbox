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


# ── v1.0.0: purge_runs dry_run ────────────────────────────────────────────────


def test_purge_runs_dry_run_returns_preview():
    rows = [
        DeletedRunInfo(run_id="run_old1", notebook_path="/tmp/old1/nb.py"),
        DeletedRunInfo(run_id="run_old2", notebook_path="/tmp/old2/nb.py"),
    ]
    with patch.object(server_module, "db") as mock_db:
        mock_db.list_runs_older_than.return_value = rows
        result = server_module._impl_purge_runs(older_than_days=30, delete_files=True, dry_run=True)

    assert result["dry_run"] is True
    assert result["would_delete_runs"] == 2
    assert set(result["run_ids"]) == {"run_old1", "run_old2"}


def test_purge_runs_dry_run_skips_delete():
    rows = [DeletedRunInfo(run_id="run_old1", notebook_path="/tmp/old1/nb.py")]
    with patch.object(server_module, "db") as mock_db:
        mock_db.list_runs_older_than.return_value = rows
        server_module._impl_purge_runs(older_than_days=30, delete_files=True, dry_run=True)
    mock_db.delete_runs_older_than.assert_not_called()


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


# ── v0.8: cancel_run ──────────────────────────────────────────────────────────


def test_cancel_run_not_found():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = None
        result = server_module._impl_cancel_run("run_missing")
    assert "error" in result
    assert "run_missing" in result["error"]


def test_cancel_run_not_running():
    run = RunRecord(
        run_id="run_done",
        code="pass",
        description="Done",
        packages=[],
        status=RunStatus.SUCCESS,
        notebook_path="/tmp/nb.py",
        created_at="2026-01-01 00:00:00",
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_cancel_run("run_done")
    assert "error" in result
    assert "not running" in result["error"]


def test_cancel_run_success():
    run = RunRecord(
        run_id="run_live",
        code="import time; time.sleep(60)",
        description="Long run",
        packages=[],
        status=RunStatus.RUNNING,
        notebook_path="/tmp/nb.py",
        created_at="2026-01-01 00:00:00",
        pid=9999,
    )
    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module.os, "kill") as mock_kill:
        mock_db.get_run.return_value = run
        result = server_module._impl_cancel_run("run_live")

    assert result["success"] is True
    assert result["run_id"] == "run_live"
    assert result["pid"] == 9999
    mock_kill.assert_called_once_with(9999, server_module.signal.SIGTERM)
    mock_db.update_run.assert_called_once_with("run_live", status="cancelled", duration_ms=0)


def test_cancel_run_process_already_gone():
    run = RunRecord(
        run_id="run_gone",
        code="pass",
        description="Gone",
        packages=[],
        status=RunStatus.RUNNING,
        notebook_path="/tmp/nb.py",
        created_at="2026-01-01 00:00:00",
        pid=9998,
    )
    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module.os, "kill", side_effect=ProcessLookupError):
        mock_db.get_run.return_value = run
        result = server_module._impl_cancel_run("run_gone")

    assert result["success"] is True
    mock_db.update_run.assert_called_once()


def test_run_python_async_mode():
    """async_mode=True should return status='running' without waiting."""
    mock_nb = MagicMock()
    mock_nb.notebook_path = Path("/tmp/nb.py")
    mock_nb.notebook_dir = Path("/tmp")
    mock_nb.result_path = Path("/tmp/result.json")

    mock_process = MagicMock()
    mock_process.pid = 42

    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module, "executor") as mock_exec, \
         patch.object(server_module, "generator") as mock_gen, \
         patch.object(server_module, "threading"):
        mock_gen.generate.return_value = mock_nb
        mock_exec.execute_async.return_value = mock_process
        mock_exec.install_packages.return_value = {"success": True, "output": "", "freeze": ""}
        result = server_module._impl_run_python(
            code="import time; time.sleep(10)",
            description="Async test",
            timeout_seconds=60,
            sandbox=False,
            packages=[],
            async_mode=True,
        )

    assert result["status"] == "running"
    assert "run_id" in result
    assert "notebook_path" in result
    mock_exec.execute.assert_not_called()
    mock_exec.execute_async.assert_called_once()
    # DB should be created with 'running' status
    mock_db.create_run.assert_called_once()
    call_kwargs = mock_db.create_run.call_args.kwargs
    assert call_kwargs.get("status") == "running"


# ── v0.9: list_environments / clean_environments ──────────────────────────────


def test_list_environments_empty():
    with patch.object(server_module, "env_manager") as mock_em:
        mock_em.list_envs.return_value = []
        result = server_module._impl_list_environments()
    assert result["count"] == 0
    assert result["environments"] == []


def test_list_environments_with_entries():
    from marimo_sandbox.env_manager import EnvInfo
    fake_env = EnvInfo(
        env_hash="abc1234567890abc",
        python_path=Path("/fake/bin/python"),
        packages=["requests"],
        freeze="requests==2.31.0\n",
        created_at="2026-01-01T00:00:00+00:00",
        last_used_at="2026-04-01T00:00:00+00:00",
        size_bytes=1024,
    )
    with patch.object(server_module, "env_manager") as mock_em:
        mock_em.list_envs.return_value = [fake_env]
        result = server_module._impl_list_environments()
    assert result["count"] == 1
    env = result["environments"][0]
    assert env["env_hash"] == "abc1234567890abc"
    assert env["packages"] == ["requests"]
    assert env["size_bytes"] == 1024


def test_clean_environments_no_deletions():
    with patch.object(server_module, "env_manager") as mock_em:
        mock_em.list_envs.return_value = []
        mock_em.clean_old_envs.return_value = []
        result = server_module._impl_clean_environments(older_than_days=90)
    assert result["deleted_count"] == 0
    assert result["deleted_hashes"] == []
    assert result["freed_bytes"] == 0


def test_clean_environments_deletes_old():
    from marimo_sandbox.env_manager import EnvInfo
    fake_env = EnvInfo(
        env_hash="oldenv1234567890",
        python_path=Path("/fake/bin/python"),
        packages=["numpy"],
        freeze="numpy==1.26.0\n",
        created_at="2020-01-01T00:00:00+00:00",
        last_used_at="2020-01-01T00:00:00+00:00",
        size_bytes=50_000,
    )
    with patch.object(server_module, "env_manager") as mock_em:
        mock_em.list_envs.return_value = [fake_env]
        mock_em.clean_old_envs.return_value = ["oldenv1234567890"]
        result = server_module._impl_clean_environments(older_than_days=90)
    assert result["deleted_count"] == 1
    assert "oldenv1234567890" in result["deleted_hashes"]
    assert result["freed_bytes"] == 50_000


def test_list_pending_approvals_empty():
    with patch.object(server_module, "db") as mock_db:
        mock_db.list_pending_approvals.return_value = []
        result = server_module._impl_list_pending_approvals()
    assert result["count"] == 0
    assert result["pending"] == []


def test_rerun_sets_parent_run_id():
    mock_rp = MagicMock(return_value={"status": "success"})
    with patch.object(server_module, "db") as mock_db, \
         patch.object(server_module, "_impl_run_python", mock_rp):
        mock_db.get_run.return_value = RunRecord(
            run_id="run_origin",
            code="print('hello')",
            description="Origin run",
            packages=[],
            status=RunStatus.PENDING,
            notebook_path="/tmp/nb.py",
            created_at="2026-01-01 00:00:00",
        )
        server_module._impl_rerun(
            "run_origin", code=None, description=None,
            timeout_seconds=60, sandbox=False, packages=None,
        )

    mock_rp.assert_called_once()
    call_kwargs = mock_rp.call_args.kwargs
    assert call_kwargs["parent_run_id"] == "run_origin"


# ── v1.0.0: list_runs pagination ──────────────────────────────────────────────


def test_list_runs_returns_total():
    with patch.object(server_module, "db") as mock_db:
        mock_db.list_runs.return_value = []
        mock_db.count_runs.return_value = 42
        result = server_module._impl_list_runs(limit=20, offset=0)
    assert "total" in result
    assert result["total"] == 42
    assert result["offset"] == 0


def test_list_runs_offset_forwarded():
    with patch.object(server_module, "db") as mock_db:
        mock_db.list_runs.return_value = []
        mock_db.count_runs.return_value = 0
        server_module._impl_list_runs(limit=10, offset=5)
    mock_db.list_runs.assert_called_once_with(limit=10, status=None, offset=5)


# ── v0.9.2: get_run provenance fields ─────────────────────────────────────────


def test_get_run_includes_provenance_fields(tmp_path):
    run = RunRecord(
        run_id="run_prov",
        code="x = 1",
        description="Provenance test",
        packages=[],
        status=RunStatus.SUCCESS,
        notebook_path=str(tmp_path / "nb.py"),
        created_at="2026-01-01 00:00:00",
        code_hash="abc123",
        env_hash="def456",
        freeze="requests==2.31.0\n",
        risk_findings=[],
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_get_run("run_prov")

    assert "code_hash" in result
    assert "env_hash" in result
    assert "freeze" in result
    assert "risk_findings" in result
    assert result["code_hash"] == "abc123"
    assert result["env_hash"] == "def456"
    assert result["freeze"] == "requests==2.31.0\n"


def test_get_run_risk_findings_deserialized(tmp_path):
    findings = [{"severity": "high", "category": "dangerous_import", "line": 1,
                 "message": "import os", "code_snippet": "import os"}]
    run = RunRecord(
        run_id="run_rf",
        code="import os",
        description="Risk findings test",
        packages=[],
        status=RunStatus.SUCCESS,
        notebook_path=str(tmp_path / "nb.py"),
        created_at="2026-01-01 00:00:00",
        risk_findings=findings,
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run
        result = server_module._impl_get_run("run_rf")

    assert isinstance(result["risk_findings"], list)
    assert len(result["risk_findings"]) == 1
    assert result["risk_findings"][0]["severity"] == "high"


# ── v0.9.1: diff_runs ─────────────────────────────────────────────────────────


def _make_run_pair(
    tmp_path: Path,
    id_a: str = "run_aaa",
    id_b: str = "run_bbb",
    *,
    code_a: str = "print('hello')",
    code_b: str = "print('hello')",
    status_a: RunStatus = RunStatus.SUCCESS,
    status_b: RunStatus = RunStatus.SUCCESS,
    artifacts_a: list[str] | None = None,
    artifacts_b: list[str] | None = None,
    packages_a: list[str] | None = None,
    packages_b: list[str] | None = None,
    parent_b: str | None = None,
    parent_a: str | None = None,
    duration_a: int | None = 100,
    duration_b: int | None = 100,
    code_hash_a: str | None = None,
    code_hash_b: str | None = None,
    env_hash_a: str | None = None,
    env_hash_b: str | None = None,
) -> tuple["RunRecord", "RunRecord"]:
    import hashlib

    def _nb(run_id: str) -> Path:
        p = tmp_path / "notebooks" / run_id / "notebook.py"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
        return p

    run_a = RunRecord(
        run_id=id_a,
        code=code_a,
        description="Run A",
        packages=packages_a or [],
        status=status_a,
        notebook_path=str(_nb(id_a)),
        created_at="2026-01-01 00:00:00",
        duration_ms=duration_a,
        artifacts=artifacts_a or [],
        parent_run_id=parent_a,
        code_hash=code_hash_a or hashlib.sha256(code_a.encode()).hexdigest(),
        env_hash=env_hash_a,
    )
    run_b = RunRecord(
        run_id=id_b,
        code=code_b,
        description="Run B",
        packages=packages_b or [],
        status=status_b,
        notebook_path=str(_nb(id_b)),
        created_at="2026-01-02 00:00:00",
        duration_ms=duration_b,
        artifacts=artifacts_b or [],
        parent_run_id=parent_b,
        code_hash=code_hash_b or hashlib.sha256(code_b.encode()).hexdigest(),
        env_hash=env_hash_b,
    )
    return run_a, run_b


def _mock_get_run(run_a: "RunRecord", run_b: "RunRecord"):
    """Return a side_effect function that dispatches by run_id."""
    mapping = {run_a.run_id: run_a, run_b.run_id: run_b}

    def _get(run_id: str) -> "RunRecord | None":
        return mapping.get(run_id)

    return _get


def test_diff_runs_run_not_found():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = None
        result = server_module._impl_diff_runs("run_missing")
    assert "error" in result
    assert "run_missing" in result["error"]


def test_diff_runs_no_parent_no_compare_to(tmp_path):
    _, run_b = _make_run_pair(tmp_path, parent_b=None)
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = run_b
        result = server_module._impl_diff_runs(run_b.run_id, compare_to=None)
    assert "error" in result
    assert "compare_to" in result["error"] or "parent_run_id" in result["error"]


def test_diff_runs_compare_to_not_found(tmp_path):
    _, run_b = _make_run_pair(tmp_path, parent_b=None)
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = [run_b, None]
        result = server_module._impl_diff_runs(run_b.run_id, compare_to="run_ghost")
    assert "error" in result
    assert "run_ghost" in result["error"]


def test_diff_runs_same_code_same_env(tmp_path):
    run_a, run_b = _make_run_pair(
        tmp_path,
        parent_b="run_aaa",  # b's parent is a
        code_a="print('hi')",
        code_b="print('hi')",
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)
    assert result["summary"]["code_changed"] is False
    assert result["summary"]["env_changed"] is False
    assert result["summary"]["status_changed"] is False
    assert result["summary"]["artifacts_changed"] is False


def test_diff_runs_code_changed(tmp_path):
    import hashlib

    code_a = "print('hello')\n"
    code_b = "print('hello')\nprint('world')\n"
    run_a, run_b = _make_run_pair(
        tmp_path,
        code_a=code_a,
        code_b=code_b,
        code_hash_a=hashlib.sha256(code_a.encode()).hexdigest(),
        code_hash_b=hashlib.sha256(code_b.encode()).hexdigest(),
        parent_b="run_aaa",
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)
    assert result["code_diff"]["changed"] is True
    assert result["code_diff"]["lines_added"] == 1
    assert result["code_diff"]["lines_removed"] == 0


def test_diff_runs_status_changed(tmp_path):
    run_a, run_b = _make_run_pair(
        tmp_path,
        status_a=RunStatus.ERROR,
        status_b=RunStatus.SUCCESS,
        parent_b="run_aaa",
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)
    assert result["status_diff"]["changed"] is True
    assert result["status_diff"]["before"] == "error"
    assert result["status_diff"]["after"] == "success"
    assert result["summary"]["status_changed"] is True


def test_diff_runs_artifacts_changed(tmp_path):
    run_a, run_b = _make_run_pair(
        tmp_path,
        artifacts_a=["report.csv"],
        artifacts_b=["report.csv", "chart.png"],
        parent_b="run_aaa",
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)
    assert result["artifact_diff"]["changed"] is True
    assert "chart.png" in result["artifact_diff"]["added"]
    assert result["artifact_diff"]["removed"] == []
    assert "report.csv" in result["artifact_diff"]["common"]


def test_diff_runs_output_diff_with_sidecars(tmp_path):
    run_a, run_b = _make_run_pair(tmp_path, parent_b="run_aaa")
    # Write sidecar JSON files
    sidecar_a = {"status": "success", "outputs": {"total": 10, "label": "old"}}
    sidecar_b = {"status": "success", "outputs": {"total": 20, "label": "old"}}
    nb_dir_a = Path(run_a.notebook_path).parent
    nb_dir_b = Path(run_b.notebook_path).parent
    (nb_dir_a / f"{run_a.run_id}_result.json").write_text(json.dumps(sidecar_a))
    (nb_dir_b / f"{run_b.run_id}_result.json").write_text(json.dumps(sidecar_b))

    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)
    assert result["output_diff"]["available"] is True
    assert result["output_diff"]["changed"] is True
    assert "total" in result["output_diff"]["changed_keys"]
    assert result["output_diff"]["changed_keys"]["total"]["before"] == 10
    assert result["output_diff"]["changed_keys"]["total"]["after"] == 20
    assert result["output_diff"]["added_keys"] == []


def test_diff_runs_output_diff_no_sidecar(tmp_path):
    run_a, run_b = _make_run_pair(tmp_path, parent_b="run_aaa")
    # No sidecar files written
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)
    assert result["output_diff"]["available"] is False
    assert result["output_diff"]["changed"] is False


def test_diff_runs_auto_parent(tmp_path):
    run_a, run_b = _make_run_pair(tmp_path, parent_b="run_aaa")
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id, compare_to=None)
    assert result["run_a"] == run_a.run_id
    assert result["run_b"] == run_b.run_id


def test_diff_runs_explicit_compare_to(tmp_path):
    run_a, run_b = _make_run_pair(tmp_path, parent_b=None)  # b has no parent
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id, compare_to=run_a.run_id)
    assert result["run_a"] == run_a.run_id
    assert result["run_b"] == run_b.run_id
    assert "error" not in result


def test_diff_runs_relationship_parent_child(tmp_path):
    run_a, run_b = _make_run_pair(tmp_path, parent_b="run_aaa")
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)
    assert result["relationship"] == "parent_child"


def test_diff_runs_relationship_siblings(tmp_path):
    # Both a and b share the same parent "run_parent"
    run_a, run_b = _make_run_pair(
        tmp_path,
        parent_a="run_parent",
        parent_b="run_parent",
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id, compare_to=run_a.run_id)
    assert result["relationship"] == "siblings"


def test_diff_runs_relationship_unrelated(tmp_path):
    run_a, run_b = _make_run_pair(tmp_path, parent_a=None, parent_b=None)
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id, compare_to=run_a.run_id)
    assert result["relationship"] == "unrelated"


def test_diff_runs_explanation_is_string(tmp_path):
    run_a, run_b = _make_run_pair(tmp_path, parent_b="run_aaa")
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)
    assert isinstance(result["explanation"], str)
    assert len(result["explanation"]) > 0


def test_diff_runs_diff_text_present_when_changed(tmp_path):
    import hashlib

    code_a = "print('hello')\n"
    code_b = "print('hello')\nprint('world')\n"
    run_a, run_b = _make_run_pair(
        tmp_path,
        code_a=code_a,
        code_b=code_b,
        code_hash_a=hashlib.sha256(code_a.encode()).hexdigest(),
        code_hash_b=hashlib.sha256(code_b.encode()).hexdigest(),
        parent_b="run_aaa",
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)

    diff_text = result["code_diff"]["diff_text"]
    assert diff_text is not None
    assert isinstance(diff_text, str)
    assert "+" in diff_text or "-" in diff_text


def test_diff_runs_diff_text_none_when_unchanged(tmp_path):
    run_a, run_b = _make_run_pair(
        tmp_path,
        code_a="print('hi')",
        code_b="print('hi')",
        parent_b="run_aaa",
    )
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.side_effect = _mock_get_run(run_a, run_b)
        result = server_module._impl_diff_runs(run_b.run_id)

    assert result["code_diff"]["diff_text"] is None


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
