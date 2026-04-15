"""Unit tests for server-level tools: delete_run, rerun, purge_runs."""

from unittest.mock import MagicMock, patch

import marimo_sandbox.server as server_module
from marimo_sandbox.models import DeletedRunInfo, RunRecord, RunStatus

# ── delete_run ────────────────────────────────────────────────────────────────


def test_delete_run_not_found():
    with patch.object(server_module, "db") as mock_db:
        mock_db.get_run.return_value = None
        result = server_module._impl_delete_run("run_missing")
    assert result["success"] is False
    assert "run_missing" in result["error"]


def test_delete_run_no_file_deletion(tmp_path):
    fake_nb = tmp_path / "notebooks" / "run_abc" / "notebook.py"
    fake_nb.parent.mkdir(parents=True)
    fake_nb.touch()

    with patch.object(server_module, "db") as mock_db, \
         patch("marimo_sandbox.server.shutil.rmtree") as mock_rmtree:
        mock_db.get_run.return_value = RunRecord(
            run_id="run_abc",
            code="print('hi')",
            description="My run",
            packages=[],
            status=RunStatus.PENDING,
            notebook_path=str(fake_nb),
            created_at="2026-01-01 00:00:00",
        )
        mock_db.delete_run.return_value = True
        result = server_module._impl_delete_run("run_abc", delete_files=False)

    assert result["success"] is True
    assert result["files_deleted"] is False
    mock_rmtree.assert_not_called()


def test_delete_run_deletes_directory(tmp_path):
    fake_nb = tmp_path / "notebooks" / "run_abc" / "notebook.py"
    fake_nb.parent.mkdir(parents=True)
    fake_nb.touch()

    with patch.object(server_module, "db") as mock_db, \
         patch("marimo_sandbox.server.shutil.rmtree") as mock_rmtree:
        mock_db.get_run.return_value = RunRecord(
            run_id="run_abc",
            code="print('hi')",
            description="My run",
            packages=[],
            status=RunStatus.PENDING,
            notebook_path=str(fake_nb),
            created_at="2026-01-01 00:00:00",
        )
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
