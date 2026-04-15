"""Unit tests for the Database class (SQLite backend)."""

import sqlite3
from pathlib import Path

import pytest

from marimo_sandbox.database import Database
from marimo_sandbox.models import RunStatus


@pytest.fixture
def db(tmp_path: Path) -> Database:
    return Database(tmp_path / "test.db")


def test_create_and_get_run(db: Database) -> None:
    db.create_run("run_001", "Test run", "print('hi')", "/tmp/nb.py")
    row = db.get_run("run_001")
    assert row is not None
    assert row.run_id == "run_001"
    assert row.description == "Test run"
    assert row.code == "print('hi')"
    assert row.status == RunStatus.PENDING
    assert row.notebook_path == "/tmp/nb.py"


def test_update_run(db: Database) -> None:
    db.create_run("run_002", "Update test", "x = 1", "/tmp/nb2.py")
    db.update_run(
        "run_002", status="success", duration_ms=123, stdout="done", stderr="", error=None
    )
    row = db.get_run("run_002")
    assert row is not None
    assert row.status == RunStatus.SUCCESS
    assert row.duration_ms == 123
    assert row.stdout == "done"
    assert row.error is None


def test_list_runs_empty(db: Database) -> None:
    assert db.list_runs() == []


def test_list_runs_with_status_filter(db: Database) -> None:
    db.create_run("run_ok", "Success run", "pass", "/nb1.py")
    db.update_run("run_ok", status="success", duration_ms=50)

    db.create_run("run_err", "Error run", "raise ValueError()", "/nb2.py")
    db.update_run("run_err", status="error", duration_ms=10, error="boom")

    successes = db.list_runs(status=RunStatus.SUCCESS)
    assert len(successes) == 1
    assert successes[0].run_id == "run_ok"

    errors = db.list_runs(status=RunStatus.ERROR)
    assert len(errors) == 1
    assert errors[0].run_id == "run_err"

    all_runs = db.list_runs()
    assert len(all_runs) == 2


def test_count_runs(db: Database) -> None:
    assert db.count_runs() == 0
    db.create_run("r1", "first", "pass", "/nb.py")
    assert db.count_runs() == 1
    db.create_run("r2", "second", "pass", "/nb2.py")
    assert db.count_runs() == 2


def test_get_run_not_found(db: Database) -> None:
    assert db.get_run("nonexistent") is None


def test_delete_run_existing(db: Database) -> None:
    db.create_run("run_del", "Delete me", "pass", "/tmp/nb.py")
    assert db.count_runs() == 1
    result = db.delete_run("run_del")
    assert result is True
    assert db.get_run("run_del") is None
    assert db.count_runs() == 0


def test_delete_run_nonexistent(db: Database) -> None:
    result = db.delete_run("does_not_exist")
    assert result is False


def test_create_run_with_packages(db: Database) -> None:
    db.create_run("run_pkg", "Pkg test", "import requests", "/tmp/nb.py", packages=["requests"])
    row = db.get_run("run_pkg")
    assert row is not None
    assert row.packages == ["requests"]


def test_create_run_default_packages(db: Database) -> None:
    db.create_run("run_nopkg", "No pkg test", "pass", "/tmp/nb.py")
    row = db.get_run("run_nopkg")
    assert row is not None
    assert row.packages == []


def test_migration_adds_packages_column(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    # Create a DB without the packages column (simulating a pre-v0.3.0 DB)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            code TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            notebook_path TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        "INSERT INTO runs (run_id, description, code, status, notebook_path) "
        "VALUES ('run_legacy', 'Legacy', 'pass', 'pending', '/tmp/nb.py')"
    )
    conn.commit()
    conn.close()

    # Open via Database — migration should add the column and default to '[]'
    migrated = Database(db_path)
    row = migrated.get_run("run_legacy")
    assert row is not None
    assert row.packages == []


def test_delete_runs_older_than(db: Database) -> None:
    # Insert 2 "old" rows with a past created_at via raw SQL
    with db._lock:
        db._conn.execute(
            "INSERT INTO runs (run_id, description, code, status, notebook_path, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("run_old1", "Old 1", "pass", "pending", "/tmp/old1.py", "2020-01-01 00:00:00"),
        )
        db._conn.execute(
            "INSERT INTO runs (run_id, description, code, status, notebook_path, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            ("run_old2", "Old 2", "pass", "pending", "/tmp/old2.py", "2020-06-15 12:00:00"),
        )
        db._conn.commit()

    # Insert 1 fresh row using normal API (created_at defaults to now)
    db.create_run("run_fresh", "Fresh", "pass", "/tmp/fresh.py")

    deleted = db.delete_runs_older_than(days=1)
    assert len(deleted) == 2
    deleted_ids = {r.run_id for r in deleted}
    assert deleted_ids == {"run_old1", "run_old2"}

    # Fresh run must survive
    assert db.get_run("run_fresh") is not None
    assert db.count_runs() == 1


# ── v0.5 additions ────────────────────────────────────────────────────────────


def test_create_run_stores_code_hash(db: Database) -> None:
    db.create_run("run_hash", "Hash test", "x = 1", "/tmp/nb.py", code_hash="abc123")
    row = db.get_run("run_hash")
    assert row is not None
    assert row.code_hash == "abc123"


def test_create_run_no_code_hash(db: Database) -> None:
    db.create_run("run_nohash", "No hash", "pass", "/tmp/nb.py")
    row = db.get_run("run_nohash")
    assert row is not None
    assert row.code_hash is None


def test_update_run_stores_freeze_and_artifacts(db: Database) -> None:
    db.create_run("run_fa", "Freeze+artifacts", "pass", "/tmp/nb.py")
    db.update_run(
        "run_fa",
        status="success",
        duration_ms=100,
        freeze="requests==2.31.0\nnumpy==1.26.0",
        artifacts=["output.csv", "chart.png"],
    )
    row = db.get_run("run_fa")
    assert row is not None
    assert row.freeze == "requests==2.31.0\nnumpy==1.26.0"
    assert row.artifacts == ["output.csv", "chart.png"]


def test_update_run_artifacts_default_empty(db: Database) -> None:
    db.create_run("run_noart", "No artifacts", "pass", "/tmp/nb.py")
    db.update_run("run_noart", status="success", duration_ms=50)
    row = db.get_run("run_noart")
    assert row is not None
    assert row.artifacts == []


def test_migration_new_columns(tmp_path: Path) -> None:
    """Legacy DB without v0.5 columns opens without error; fields default correctly."""
    db_path = tmp_path / "legacy_v04.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE runs (
            run_id TEXT PRIMARY KEY,
            description TEXT NOT NULL,
            code TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            notebook_path TEXT NOT NULL,
            packages TEXT NOT NULL DEFAULT '[]',
            duration_ms INTEGER,
            stdout TEXT,
            stderr TEXT,
            error TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        "INSERT INTO runs (run_id, description, code, status, notebook_path) "
        "VALUES ('run_old', 'Old', 'pass', 'pending', '/tmp/nb.py')"
    )
    conn.commit()
    conn.close()

    migrated = Database(db_path)
    row = migrated.get_run("run_old")
    assert row is not None
    assert row.code_hash is None
    assert row.freeze is None or row.freeze == ""
    assert row.artifacts == []


# ── v0.6 additions ────────────────────────────────────────────────────────────


def test_runs_table_has_risk_findings_column(db: Database) -> None:
    db.create_run("run_rf", "Risk findings test", "pass", "/tmp/nb.py")
    db.update_run(
        "run_rf",
        status="success",
        duration_ms=10,
        risk_findings=[{"severity": "high", "category": "dangerous_import",
                        "line": 1, "message": "import os detected"}],
    )
    # Verify via raw SQL that the column exists and has data
    with db._lock:
        row = db._conn.execute(
            "SELECT risk_findings FROM runs WHERE run_id = 'run_rf'"
        ).fetchone()
    assert row is not None
    assert "dangerous_import" in row[0]


def test_create_and_get_pending_approval(db: Database) -> None:
    db.create_pending_approval(
        token="approval_abc123",
        run_id="run_001",
        code="import subprocess; subprocess.run(['ls'])",
        description="Test run",
        packages=[],
        timeout_seconds=60,
        sandbox=False,
        risk_findings_json='[{"severity": "critical"}]',
        expires_at="2099-01-01T00:00:00+00:00",
    )
    row = db.get_pending_approval("approval_abc123")
    assert row is not None
    assert row["token"] == "approval_abc123"
    assert row["run_id"] == "run_001"
    assert row["expires_at"] == "2099-01-01T00:00:00+00:00"
    assert row["sandbox"] == 0


def test_get_pending_approval_missing(db: Database) -> None:
    assert db.get_pending_approval("nonexistent_token") is None


def test_delete_pending_approval(db: Database) -> None:
    db.create_pending_approval(
        token="approval_del",
        run_id="run_002",
        code="pass",
        description="Delete test",
        packages=[],
        timeout_seconds=30,
        sandbox=False,
        risk_findings_json="[]",
        expires_at="2099-01-01T00:00:00+00:00",
    )
    db.delete_pending_approval("approval_del")
    assert db.get_pending_approval("approval_del") is None


def test_list_pending_approvals_returns_all(db: Database) -> None:
    for i in range(3):
        db.create_pending_approval(
            token=f"approval_{i}",
            run_id=f"run_{i:03d}",
            code="pass",
            description=f"Run {i}",
            packages=[],
            timeout_seconds=60,
            sandbox=False,
            risk_findings_json="[]",
            expires_at="2099-01-01T00:00:00+00:00",
        )
    rows = db.list_pending_approvals()
    assert len(rows) == 3
    tokens = {r["token"] for r in rows}
    assert tokens == {"approval_0", "approval_1", "approval_2"}


# ── v0.7 additions ────────────────────────────────────────────────────────────


def test_purge_expired_approvals(db: Database) -> None:
    # Insert one expired and one valid approval
    db.create_pending_approval(
        token="approval_expired",
        run_id="run_001",
        code="pass",
        description="Expired",
        packages=[],
        timeout_seconds=60,
        sandbox=False,
        risk_findings_json="[]",
        expires_at="2020-01-01T00:00:00+00:00",  # in the past
    )
    db.create_pending_approval(
        token="approval_valid",
        run_id="run_002",
        code="pass",
        description="Valid",
        packages=[],
        timeout_seconds=60,
        sandbox=False,
        risk_findings_json="[]",
        expires_at="2099-01-01T00:00:00+00:00",
    )
    deleted = db.purge_expired_approvals()
    assert deleted == 1
    assert db.get_pending_approval("approval_expired") is None
    assert db.get_pending_approval("approval_valid") is not None


def test_purge_expired_approvals_none_expired(db: Database) -> None:
    db.create_pending_approval(
        token="approval_future",
        run_id="run_001",
        code="pass",
        description="Future",
        packages=[],
        timeout_seconds=60,
        sandbox=False,
        risk_findings_json="[]",
        expires_at="2099-01-01T00:00:00+00:00",
    )
    deleted = db.purge_expired_approvals()
    assert deleted == 0


def test_create_run_with_parent_run_id(db: Database) -> None:
    db.create_run("run_parent", "Parent run", "pass", "/tmp/nb.py")
    db.create_run("run_child", "Child run", "pass", "/tmp/nb2.py", parent_run_id="run_parent")
    child = db.get_run("run_child")
    assert child is not None
    assert child.parent_run_id == "run_parent"


def test_create_run_default_parent_run_id_is_none(db: Database) -> None:
    db.create_run("run_no_parent", "No parent", "pass", "/tmp/nb.py")
    row = db.get_run("run_no_parent")
    assert row is not None
    assert row.parent_run_id is None


# ── v0.8 additions ────────────────────────────────────────────────────────────


def test_create_run_with_running_status(db: Database) -> None:
    db.create_run("run_async", "Async run", "pass", "/tmp/nb.py", status="running")
    row = db.get_run("run_async")
    assert row is not None
    assert row.status == RunStatus.RUNNING


def test_update_run_pid(db: Database) -> None:
    db.create_run("run_pid", "PID test", "pass", "/tmp/nb.py")
    db.update_run_pid("run_pid", 12345)
    row = db.get_run("run_pid")
    assert row is not None
    assert row.pid == 12345


def test_startup_recovery_resets_running_runs(tmp_path: Path) -> None:
    """Runs stuck in 'running' should be reset to 'error' on DB open."""
    db_path = tmp_path / "recovery.db"
    db1 = Database(db_path)
    db1.create_run("run_stuck", "Stuck run", "pass", "/tmp/nb.py", status="running")
    row = db1.get_run("run_stuck")
    assert row is not None
    assert row.status == RunStatus.RUNNING

    # Re-open the database (simulates server restart)
    db2 = Database(db_path)
    row2 = db2.get_run("run_stuck")
    assert row2 is not None
    assert row2.status == RunStatus.ERROR
    assert row2.error is not None and "Interrupted" in row2.error


def test_update_run_cancelled_status(db: Database) -> None:
    db.create_run("run_cancel", "Cancel test", "pass", "/tmp/nb.py")
    db.update_run("run_cancel", status="cancelled", duration_ms=0)
    row = db.get_run("run_cancel")
    assert row is not None
    assert row.status == RunStatus.CANCELLED


# ── v1.0.0 additions ──────────────────────────────────────────────────────────


def test_list_runs_offset(db: Database) -> None:
    """Inserting 5 runs and offsetting by 3 should return exactly 2 runs."""
    for i in range(5):
        db.create_run(f"run_{i:03d}", f"Run {i}", "pass", f"/tmp/nb{i}.py")

    result = db.list_runs(limit=20, offset=3)
    assert len(result) == 2


def test_list_runs_offset_beyond_end(db: Database) -> None:
    """Offset greater than total count should return an empty list."""
    db.create_run("run_001", "Run 1", "pass", "/tmp/nb1.py")
    db.create_run("run_002", "Run 2", "pass", "/tmp/nb2.py")

    result = db.list_runs(limit=20, offset=100)
    assert result == []


# ── v0.9 additions ────────────────────────────────────────────────────────────


def test_create_run_with_env_hash(db: Database) -> None:
    db.create_run("run_env", "Env test", "pass", "/tmp/nb.py", env_hash="abc1234567890abc")
    row = db.get_run("run_env")
    assert row is not None
    assert row.env_hash == "abc1234567890abc"


def test_create_run_default_env_hash_is_none(db: Database) -> None:
    db.create_run("run_noenv", "No env", "pass", "/tmp/nb.py")
    row = db.get_run("run_noenv")
    assert row is not None
    assert row.env_hash is None
