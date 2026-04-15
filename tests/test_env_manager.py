"""Unit tests for EnvManager (hash-based venv cache)."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from marimo_sandbox.env_manager import EnvManager


@pytest.fixture
def envs_dir(tmp_path: Path) -> Path:
    return tmp_path / "envs"


@pytest.fixture
def mgr(envs_dir: Path) -> EnvManager:
    return EnvManager(envs_dir)


# ── env_hash ─────────────────────────────────────────────────────────────────


def test_env_hash_is_16_chars(mgr: EnvManager) -> None:
    h = mgr.env_hash(["requests", "numpy"])
    assert len(h) == 16
    assert h.isalnum()


def test_env_hash_same_packages_different_order(mgr: EnvManager) -> None:
    assert mgr.env_hash(["numpy", "requests"]) == mgr.env_hash(["requests", "numpy"])


def test_env_hash_different_packages(mgr: EnvManager) -> None:
    assert mgr.env_hash(["requests"]) != mgr.env_hash(["numpy"])


def test_env_hash_empty_packages(mgr: EnvManager) -> None:
    h = mgr.env_hash([])
    assert len(h) == 16


# ── list_envs with no envs ────────────────────────────────────────────────────


def test_list_envs_empty(mgr: EnvManager) -> None:
    assert mgr.list_envs() == []


# ── delete_env ────────────────────────────────────────────────────────────────


def test_delete_env_nonexistent(mgr: EnvManager) -> None:
    assert mgr.delete_env("deadbeef12345678") is False


def test_delete_env_existing(mgr: EnvManager, envs_dir: Path) -> None:
    env_dir = envs_dir / "abc1234567890abc"
    env_dir.mkdir(parents=True)
    (env_dir / "meta.json").write_text("{}")
    assert mgr.delete_env("abc1234567890abc") is True
    assert not env_dir.exists()


# ── get_or_create — cache hit ─────────────────────────────────────────────────


def test_get_or_create_cache_hit(mgr: EnvManager, envs_dir: Path) -> None:
    packages = ["requests==2.31.0"]
    h = mgr.env_hash(packages)
    env_dir = envs_dir / h
    env_dir.mkdir(parents=True)

    # Simulate a populated venv
    bin_dir = env_dir / "bin"
    bin_dir.mkdir()
    python = bin_dir / "python"
    python.write_text("#!/usr/bin/env python3")

    freeze_content = "requests==2.31.0\n"
    (env_dir / "freeze.txt").write_text(freeze_content)

    meta = {
        "env_hash": h,
        "packages": packages,
        "created_at": "2026-01-01T00:00:00+00:00",
        "last_used_at": "2026-01-01T00:00:00+00:00",
    }
    (env_dir / "meta.json").write_text(json.dumps(meta))

    with patch("subprocess.run") as mock_run:
        info = mgr.get_or_create(packages)

    # Should NOT call subprocess for a cache hit
    mock_run.assert_not_called()
    assert info.env_hash == h
    assert info.freeze == freeze_content
    assert info.packages == packages
    assert info.python_path == python


# ── get_or_create — cache miss ────────────────────────────────────────────────


def test_get_or_create_cache_miss_creates_venv(mgr: EnvManager, envs_dir: Path) -> None:
    packages = ["httpx"]
    h = mgr.env_hash(packages)
    env_dir = envs_dir / h

    def fake_run(cmd, **kwargs):
        result = MagicMock()
        result.returncode = 0
        result.stdout = "httpx==0.27.0\n"
        # Simulate venv creation by making the python binary exist
        if "venv" in cmd:
            (env_dir / "bin").mkdir(parents=True, exist_ok=True)
            (env_dir / "bin" / "python").write_text("#!/usr/bin/env python3")
            (env_dir / "bin" / "pip").write_text("#!/usr/bin/env pip")
        return result

    with patch("subprocess.run", side_effect=fake_run):
        info = mgr.get_or_create(packages)

    assert info.env_hash == h
    assert info.packages == packages
    assert (envs_dir / h / "meta.json").exists()


# ── clean_old_envs ────────────────────────────────────────────────────────────


def test_clean_old_envs_removes_stale(mgr: EnvManager, envs_dir: Path) -> None:
    old_hash = "oldenv1234567890"
    new_hash = "newenv1234567890"

    for h, last_used in [
        (old_hash, "2020-01-01T00:00:00+00:00"),
        (new_hash, "2099-01-01T00:00:00+00:00"),
    ]:
        env_dir = envs_dir / h
        env_dir.mkdir(parents=True)
        (env_dir / "bin").mkdir()
        (env_dir / "bin" / "python").write_text("fake")
        (env_dir / "freeze.txt").write_text("pkg==1.0\n")
        meta = {
            "env_hash": h, "packages": ["pkg"],
            "created_at": last_used, "last_used_at": last_used,
        }
        (env_dir / "meta.json").write_text(json.dumps(meta))

    deleted = mgr.clean_old_envs(older_than_days=30)
    assert old_hash in deleted
    assert new_hash not in deleted
    assert not (envs_dir / old_hash).exists()
    assert (envs_dir / new_hash).exists()


def test_clean_old_envs_nothing_to_clean(mgr: EnvManager, envs_dir: Path) -> None:
    h = "freshenv1234abcd"
    env_dir = envs_dir / h
    env_dir.mkdir(parents=True)
    (env_dir / "bin").mkdir()
    (env_dir / "bin" / "python").write_text("fake")
    (env_dir / "freeze.txt").write_text("")
    meta = {"env_hash": h, "packages": [], "created_at": "2099-01-01T00:00:00+00:00",
            "last_used_at": "2099-01-01T00:00:00+00:00"}
    (env_dir / "meta.json").write_text(json.dumps(meta))

    deleted = mgr.clean_old_envs(older_than_days=30)
    assert deleted == []
