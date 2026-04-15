"""
Hash-based virtual environment cache for marimo-sandbox.

Venvs live at: ~/.marimo-sandbox/envs/{hash}/
where hash = sha256("\n".join(sorted(packages)))[:16]

Cache hit  — reuse existing venv's Python, load freeze from envs/{hash}/freeze.txt
Cache miss — create venv, install packages, write freeze.txt, update last_used_at
No packages — return None (caller should use sys.executable)
"""

import hashlib
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class EnvInfo:
    env_hash: str
    python_path: Path
    packages: list[str]
    freeze: str
    created_at: str
    last_used_at: str
    size_bytes: int


class EnvManager:
    def __init__(self, envs_dir: Path) -> None:
        self._envs_dir = envs_dir
        self._envs_dir.mkdir(parents=True, exist_ok=True)

    def env_hash(self, packages: list[str]) -> str:
        data = "\n".join(sorted(packages))
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def _meta_path(self, h: str) -> Path:
        return self._envs_dir / h / "meta.json"

    def _freeze_path(self, h: str) -> Path:
        return self._envs_dir / h / "freeze.txt"

    def _python_path(self, h: str) -> Path:
        env_dir = self._envs_dir / h
        # Support both Unix and Windows layouts
        unix_python = env_dir / "bin" / "python"
        win_python = env_dir / "Scripts" / "python.exe"
        return unix_python if unix_python.exists() or not win_python.exists() else win_python

    def _load_meta(self, h: str) -> dict[str, Any]:
        meta_path = self._meta_path(h)
        if meta_path.exists():
            try:
                result: dict[str, Any] = json.loads(meta_path.read_text())
                return result
            except (json.JSONDecodeError, OSError):
                pass
        return {}

    def _save_meta(self, h: str, packages: list[str]) -> None:
        now = datetime.now(timezone.utc).isoformat()
        meta = self._load_meta(h)
        meta["packages"] = packages
        meta["env_hash"] = h
        if "created_at" not in meta:
            meta["created_at"] = now
        meta["last_used_at"] = now
        self._meta_path(h).write_text(json.dumps(meta, indent=2))

    def _touch_last_used(self, h: str) -> None:
        meta = self._load_meta(h)
        meta["last_used_at"] = datetime.now(timezone.utc).isoformat()
        self._meta_path(h).write_text(json.dumps(meta, indent=2))

    def _dir_size(self, path: Path) -> int:
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

    def get_or_create(self, packages: list[str]) -> EnvInfo:
        """Return EnvInfo, creating + populating the venv if necessary."""
        h = self.env_hash(packages)
        env_dir = self._envs_dir / h
        python = self._python_path(h)

        if env_dir.exists() and python.exists() and self._freeze_path(h).exists():
            # Cache hit
            self._touch_last_used(h)
            meta = self._load_meta(h)
            freeze = self._freeze_path(h).read_text()
            return EnvInfo(
                env_hash=h,
                python_path=python,
                packages=meta.get("packages", packages),
                freeze=freeze,
                created_at=meta.get("created_at", ""),
                last_used_at=meta.get("last_used_at", ""),
                size_bytes=self._dir_size(env_dir),
            )

        # Cache miss — create the venv
        env_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [sys.executable, "-m", "venv", str(env_dir)],
            check=True,
            capture_output=True,
        )

        # Install packages into the venv
        venv_pip = env_dir / "bin" / "pip"
        if not venv_pip.exists():
            venv_pip = env_dir / "Scripts" / "pip.exe"
        subprocess.run(
            [str(venv_pip), "install", *packages],
            check=True,
            capture_output=True,
        )

        # Capture freeze
        venv_python = self._python_path(h)
        freeze_result = subprocess.run(
            [str(venv_python), "-m", "pip", "freeze"],
            capture_output=True,
            text=True,
        )
        freeze = freeze_result.stdout if freeze_result.returncode == 0 else ""
        self._freeze_path(h).write_text(freeze)

        self._save_meta(h, packages)
        meta = self._load_meta(h)
        return EnvInfo(
            env_hash=h,
            python_path=venv_python,
            packages=packages,
            freeze=freeze,
            created_at=meta.get("created_at", ""),
            last_used_at=meta.get("last_used_at", ""),
            size_bytes=self._dir_size(env_dir),
        )

    def list_envs(self) -> list[EnvInfo]:
        results = []
        for env_dir in self._envs_dir.iterdir():
            if not env_dir.is_dir():
                continue
            h = env_dir.name
            python = self._python_path(h)
            if not python.exists():
                continue
            meta = self._load_meta(h)
            freeze = self._freeze_path(h).read_text() if self._freeze_path(h).exists() else ""
            results.append(EnvInfo(
                env_hash=h,
                python_path=python,
                packages=meta.get("packages", []),
                freeze=freeze,
                created_at=meta.get("created_at", ""),
                last_used_at=meta.get("last_used_at", ""),
                size_bytes=self._dir_size(env_dir),
            ))
        return sorted(results, key=lambda e: e.last_used_at, reverse=True)

    def delete_env(self, env_hash: str) -> bool:
        """Delete a cached venv. Returns True if it existed."""
        import shutil
        env_dir = self._envs_dir / env_hash
        if not env_dir.exists():
            return False
        shutil.rmtree(env_dir)
        return True

    def clean_old_envs(self, older_than_days: int) -> list[str]:
        """Delete envs whose last_used_at is older than N days. Returns deleted hashes."""
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        cutoff_iso = cutoff.isoformat()
        deleted = []
        for env in self.list_envs():
            if env.last_used_at and env.last_used_at < cutoff_iso:
                if self.delete_env(env.env_hash):
                    deleted.append(env.env_hash)
        return deleted
