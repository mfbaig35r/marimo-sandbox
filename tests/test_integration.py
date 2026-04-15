"""
Integration tests — run real Marimo subprocesses end-to-end.

Marked @pytest.mark.slow so they are excluded from the fast unit test run:
    pytest tests/ -m "not slow"   # unit tests only
    pytest tests/ -m slow         # integration only
"""

from pathlib import Path

import pytest

from marimo_sandbox.database import Database
from marimo_sandbox.executor import NotebookExecutor
from marimo_sandbox.generator import NotebookGenerator


@pytest.fixture
def setup(tmp_path: Path):
    db = Database(tmp_path / "test.db")
    gen = NotebookGenerator(tmp_path / "notebooks")
    exe = NotebookExecutor()
    return db, gen, exe


@pytest.mark.slow
def test_run_simple_code(setup, tmp_path: Path) -> None:
    db, gen, exe = setup
    code = 'print("hello from marimo sandbox")'
    nb = gen.generate("run_hello", "Hello test", code)
    db.create_run("run_hello", "Hello test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=60)

    db.update_run(
        "run_hello",
        status=result.status,
        duration_ms=result.duration_ms,
        stdout=result.stdout,
        stderr=result.stderr,
        error=result.error,
    )

    assert result.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"
    assert result.stdout is not None
    assert "hello from marimo sandbox" in result.stdout


@pytest.mark.slow
def test_run_produces_sidecar(setup) -> None:
    db, gen, exe = setup
    code = "x = 1 + 1"
    nb = gen.generate("run_sidecar", "Sidecar test", code)
    db.create_run("run_sidecar", "Sidecar test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=60)

    assert result.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"
    assert nb.result_path.exists(), "Expected _result.json sidecar to be written"


@pytest.mark.slow
def test_run_error_code(setup) -> None:
    db, gen, exe = setup
    code = 'raise ValueError("integration boom")'
    nb = gen.generate("run_error", "Error test", code)
    db.create_run("run_error", "Error test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=60)

    db.update_run(
        "run_error",
        status=result.status,
        duration_ms=result.duration_ms,
        stdout=result.stdout,
        stderr=result.stderr,
        error=result.error,
    )

    assert result.status == "error"
    assert not nb.result_path.exists(), "Sidecar should not exist on error"


@pytest.mark.slow
def test_run_with_packages(setup, tmp_path: Path) -> None:
    db, gen, exe = setup
    install_result = exe.install_packages(["requests"])
    assert install_result["success"], f"Package install failed: {install_result['output']}"

    code = "import requests; print(requests.__version__)"
    nb = gen.generate("run_requests", "Requests version test", code)
    db.create_run("run_requests", "Requests version test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=120)

    db.update_run(
        "run_requests",
        status=result.status,
        duration_ms=result.duration_ms,
        stdout=result.stdout,
        stderr=result.stderr,
        error=result.error,
    )

    assert result.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"
    assert result.stdout is not None
    # Version string should contain at least one digit and dot
    assert any(ch.isdigit() for ch in result.stdout)
