"""Unit tests for the Database class (SQLite backend)."""

from pathlib import Path

import pytest

from marimo_sandbox.database import Database


@pytest.fixture
def db(tmp_path: Path) -> Database:
    return Database(tmp_path / "test.db")


def test_create_and_get_run(db: Database) -> None:
    db.create_run("run_001", "Test run", "print('hi')", "/tmp/nb.py")
    row = db.get_run("run_001")
    assert row is not None
    assert row["run_id"] == "run_001"
    assert row["description"] == "Test run"
    assert row["code"] == "print('hi')"
    assert row["status"] == "pending"
    assert row["notebook_path"] == "/tmp/nb.py"


def test_update_run(db: Database) -> None:
    db.create_run("run_002", "Update test", "x = 1", "/tmp/nb2.py")
    db.update_run(
        "run_002", status="success", duration_ms=123, stdout="done", stderr="", error=None
    )
    row = db.get_run("run_002")
    assert row is not None
    assert row["status"] == "success"
    assert row["duration_ms"] == 123
    assert row["stdout"] == "done"
    assert row["error"] is None


def test_list_runs_empty(db: Database) -> None:
    assert db.list_runs() == []


def test_list_runs_with_status_filter(db: Database) -> None:
    db.create_run("run_ok", "Success run", "pass", "/nb1.py")
    db.update_run("run_ok", status="success", duration_ms=50)

    db.create_run("run_err", "Error run", "raise ValueError()", "/nb2.py")
    db.update_run("run_err", status="error", duration_ms=10, error="boom")

    successes = db.list_runs(status="success")
    assert len(successes) == 1
    assert successes[0]["run_id"] == "run_ok"

    errors = db.list_runs(status="error")
    assert len(errors) == 1
    assert errors[0]["run_id"] == "run_err"

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
    deleted_ids = {r["run_id"] for r in deleted}
    assert deleted_ids == {"run_old1", "run_old2"}

    # Fresh run must survive
    assert db.get_run("run_fresh") is not None
    assert db.count_runs() == 1
