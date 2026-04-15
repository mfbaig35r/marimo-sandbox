"""
SQLite persistence for marimo-sandbox.
"""

import json
import sqlite3
import threading
from pathlib import Path

from .models import DeletedRunInfo, RunRecord, RunStatus

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id        TEXT PRIMARY KEY,
    description   TEXT NOT NULL,
    code          TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'pending',
    notebook_path TEXT NOT NULL,
    packages      TEXT NOT NULL DEFAULT '[]',
    duration_ms   INTEGER,
    stdout        TEXT,
    stderr        TEXT,
    error         TEXT,
    created_at    TEXT DEFAULT CURRENT_TIMESTAMP
);
"""

_PENDING_APPROVALS_SCHEMA = """
CREATE TABLE IF NOT EXISTS pending_approvals (
    token           TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    code            TEXT NOT NULL,
    description     TEXT NOT NULL,
    packages        TEXT DEFAULT '[]',
    timeout_seconds INTEGER DEFAULT 60,
    sandbox         INTEGER DEFAULT 0,
    risk_findings   TEXT NOT NULL,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
    expires_at      TEXT NOT NULL
);
"""


class Database:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute(_SCHEMA)
            self._conn.execute(_PENDING_APPROVALS_SCHEMA)
            self._conn.commit()
            # v0.3 migration: packages column
            try:
                self._conn.execute(
                    "ALTER TABLE runs ADD COLUMN packages TEXT NOT NULL DEFAULT '[]'"
                )
                self._conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists
            # v0.5 migration: code_hash, freeze, artifacts columns
            for col_ddl in [
                "ALTER TABLE runs ADD COLUMN code_hash TEXT",
                "ALTER TABLE runs ADD COLUMN freeze TEXT DEFAULT ''",
                "ALTER TABLE runs ADD COLUMN artifacts TEXT DEFAULT '[]'",
            ]:
                try:
                    self._conn.execute(col_ddl)
                    self._conn.commit()
                except sqlite3.OperationalError:
                    pass  # column already exists
            # v0.6 migration: risk_findings column
            try:
                self._conn.execute(
                    "ALTER TABLE runs ADD COLUMN risk_findings TEXT DEFAULT '[]'"
                )
                self._conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists

    # ── Internal helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _parse_run(row: dict) -> RunRecord:
        result: RunRecord = RunRecord.model_validate(dict(row))
        return result

    def _fetchone(self, sql: str, params: tuple = ()) -> dict | None:
        with self._lock:
            cur = self._conn.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row is not None else None

    def _fetchall(self, sql: str, params: tuple = ()) -> list[dict]:
        with self._lock:
            cur = self._conn.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]

    def _execute(self, sql: str, params: tuple = ()) -> None:
        with self._lock:
            self._conn.execute(sql, params)
            self._conn.commit()

    # ── Public API ───────────────────────────────────────────────────────────

    def create_run(
        self,
        run_id: str,
        description: str,
        code: str,
        notebook_path: str,
        packages: list[str] | None = None,
        code_hash: str | None = None,
    ) -> None:
        self._execute(
            "INSERT INTO runs "
            "(run_id, description, code, status, notebook_path, packages, code_hash) "
            "VALUES (?, ?, ?, 'pending', ?, ?, ?)",
            (run_id, description, code, notebook_path, json.dumps(packages or []), code_hash),
        )

    def update_run(
        self,
        run_id: str,
        status: str,
        duration_ms: int,
        stdout: str | None = None,
        stderr: str | None = None,
        error: str | None = None,
        freeze: str | None = None,
        artifacts: list[str] | None = None,
        risk_findings: list[dict] | None = None,
    ) -> None:
        self._execute(
            """UPDATE runs
               SET status=?, duration_ms=?, stdout=?, stderr=?, error=?,
                   freeze=?, artifacts=?, risk_findings=?
               WHERE run_id=?""",
            (
                status, duration_ms, stdout, stderr, error,
                freeze,
                json.dumps(artifacts or []),
                json.dumps(risk_findings or []),
                run_id,
            ),
        )

    def get_run(self, run_id: str) -> RunRecord | None:
        row = self._fetchone("SELECT * FROM runs WHERE run_id = ?", (run_id,))
        return self._parse_run(row) if row is not None else None

    def list_runs(
        self,
        limit: int = 20,
        status: RunStatus | None = None,
    ) -> list[RunRecord]:
        if status:
            return [
                self._parse_run(r)
                for r in self._fetchall(
                    "SELECT * FROM runs WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                    (status, limit),
                )
            ]
        return [
            self._parse_run(r)
            for r in self._fetchall(
                "SELECT * FROM runs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        ]

    def count_runs(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
            return int(row["n"]) if row else 0

    def delete_run(self, run_id: str) -> bool:
        """Delete a single run record. Returns True if it existed."""
        if self.get_run(run_id) is None:
            return False
        self._execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
        return True

    def delete_runs_older_than(self, days: int) -> list[DeletedRunInfo]:
        """Delete runs older than `days` days. Returns deleted rows (run_id, notebook_path)."""
        rows = self._fetchall(
            "SELECT run_id, notebook_path FROM runs WHERE created_at < datetime('now', ?)",
            (f"-{days} days",),
        )
        if rows:
            placeholders = ",".join("?" * len(rows))
            self._execute(
                f"DELETE FROM runs WHERE run_id IN ({placeholders})",
                tuple(r["run_id"] for r in rows),
            )
        return [DeletedRunInfo(run_id=r["run_id"], notebook_path=r["notebook_path"]) for r in rows]

    # ── Pending approvals ────────────────────────────────────────────────────

    def create_pending_approval(
        self,
        token: str,
        run_id: str,
        code: str,
        description: str,
        packages: list[str],
        timeout_seconds: int,
        sandbox: bool,
        risk_findings_json: str,
        expires_at: str,
    ) -> None:
        self._execute(
            """INSERT INTO pending_approvals
               (token, run_id, code, description, packages, timeout_seconds,
                sandbox, risk_findings, expires_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                token, run_id, code, description,
                json.dumps(packages or []),
                timeout_seconds,
                1 if sandbox else 0,
                risk_findings_json,
                expires_at,
            ),
        )

    def get_pending_approval(self, token: str) -> dict | None:
        return self._fetchone(
            "SELECT * FROM pending_approvals WHERE token = ?", (token,)
        )

    def delete_pending_approval(self, token: str) -> None:
        self._execute("DELETE FROM pending_approvals WHERE token = ?", (token,))

    def list_pending_approvals(self) -> list[dict]:
        return self._fetchall(
            "SELECT * FROM pending_approvals ORDER BY created_at DESC"
        )
