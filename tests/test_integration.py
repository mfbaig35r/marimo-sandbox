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
def test_async_run_submit_and_poll(setup) -> None:
    """Launch async, poll until done, verify success."""
    import time as _time
    db, gen, exe = setup
    code = 'print("async hello")'
    nb = gen.generate("run_async", "Async test", code)
    db.create_run("run_async", "Async test", code, str(nb.notebook_path), status="running")

    process = exe.execute_async(nb, timeout_seconds=60)
    db.update_run_pid("run_async", process.pid)

    # Poll until finished
    deadline = _time.monotonic() + 30
    while _time.monotonic() < deadline:
        if process.poll() is not None:
            break
        _time.sleep(0.2)

    stdout = process.stdout.read() if process.stdout else ""
    stderr = process.stderr.read() if process.stderr else ""
    duration_ms = 100
    result = exe._finish_result(nb, process.returncode or 0, stdout, stderr, duration_ms)
    db.update_run("run_async", status=result.status, duration_ms=result.duration_ms,
                  stdout=result.stdout, stderr=result.stderr, error=result.error)

    row = db.get_run("run_async")
    assert row is not None
    assert row.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"


@pytest.mark.slow
def test_async_run_cancel(setup) -> None:
    """Launch a long-running async run and cancel it."""
    import os as _os
    import signal as _signal
    import time as _time

    db, gen, exe = setup
    code = "import time; time.sleep(120)"
    nb = gen.generate("run_cancel", "Cancel test", code)
    db.create_run("run_cancel", "Cancel test", code, str(nb.notebook_path), status="running")

    process = exe.execute_async(nb, timeout_seconds=120)
    db.update_run_pid("run_cancel", process.pid)

    _time.sleep(0.5)  # Give process time to start
    assert process.poll() is None, "Process should still be running"

    try:
        _os.kill(process.pid, _signal.SIGTERM)
    except ProcessLookupError:
        pass
    process.wait(timeout=5)

    db.update_run("run_cancel", status="cancelled", duration_ms=0)
    row = db.get_run("run_cancel")
    assert row is not None
    assert row.status == "cancelled"


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


# ── Docker sandbox integration tests ────────────────────────────────────────


@pytest.fixture
def docker_available() -> bool:
    if not NotebookExecutor.check_docker():
        return False
    # Also check that the sandbox image is built locally
    import subprocess
    try:
        r = subprocess.run(
            ["docker", "images", "-q", "marimo-sandbox:latest"],
            capture_output=True, text=True, timeout=5,
        )
        return r.returncode == 0 and bool(r.stdout.strip())
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


@pytest.mark.slow
def test_docker_sandbox_simple_code(setup, docker_available: bool) -> None:
    if not docker_available:
        pytest.skip("Docker not available")
    db, gen, exe = setup
    code = 'print("hello from docker sandbox")'
    nb = gen.generate("run_docker_hello", "Docker hello test", code)
    db.create_run("run_docker_hello", "Docker hello test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=120, sandbox=True)

    assert result.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"
    assert result.stdout is not None
    assert "hello from docker sandbox" in result.stdout


@pytest.mark.slow
def test_docker_sandbox_produces_sidecar(setup, docker_available: bool) -> None:
    if not docker_available:
        pytest.skip("Docker not available")
    db, gen, exe = setup
    code = "x = 2 + 2"
    nb = gen.generate("run_docker_sidecar", "Docker sidecar test", code)
    db.create_run("run_docker_sidecar", "Docker sidecar test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=120, sandbox=True)

    assert result.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"
    assert nb.result_path.exists(), "Expected _result.json sidecar from Docker run"


@pytest.mark.slow
def test_docker_sandbox_error_code(setup, docker_available: bool) -> None:
    if not docker_available:
        pytest.skip("Docker not available")
    db, gen, exe = setup
    code = 'raise ValueError("docker boom")'
    nb = gen.generate("run_docker_error", "Docker error test", code)
    db.create_run("run_docker_error", "Docker error test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=120, sandbox=True)

    assert result.status == "error"
    assert not nb.result_path.exists()


@pytest.mark.slow
def test_docker_sandbox_no_network(setup, docker_available: bool) -> None:
    """Code that tries to reach the network should fail inside the sandbox."""
    if not docker_available:
        pytest.skip("Docker not available")
    db, gen, exe = setup
    code = (
        "import urllib.request\n"
        "urllib.request.urlopen('http://httpbin.org/get')\n"
        "print('network worked')"
    )
    nb = gen.generate("run_docker_nonet", "Docker no-network test", code)
    db.create_run("run_docker_nonet", "Docker no-network test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=120, sandbox=True)

    assert result.status == "error"
    # Should NOT have succeeded in fetching
    if result.stdout:
        assert "network worked" not in result.stdout


@pytest.mark.slow
def test_docker_sandbox_numpy_available(setup, docker_available: bool) -> None:
    """numpy is baked into the Docker image and should be importable."""
    if not docker_available:
        pytest.skip("Docker not available")
    db, gen, exe = setup
    code = "import numpy as np; print(f'numpy {np.__version__}')"
    nb = gen.generate("run_docker_numpy", "Docker numpy test", code)
    db.create_run("run_docker_numpy", "Docker numpy test", code, str(nb.notebook_path))

    result = exe.execute(nb, timeout_seconds=120, sandbox=True)

    assert result.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"
    assert result.stdout is not None
    assert "numpy" in result.stdout


@pytest.mark.slow
def test_docker_sandbox_with_packages(setup, tmp_path: Path, docker_available: bool) -> None:
    """Install a package inside Docker and verify it's importable."""
    if not docker_available:
        pytest.skip("Docker not available")
    db, gen, exe = setup
    code = "import httpx; print(f'httpx {httpx.__version__}')"
    nb = gen.generate("run_docker_pkg", "Docker package test", code)
    db.create_run("run_docker_pkg", "Docker package test", code, str(nb.notebook_path))

    pip_cache = tmp_path / "pip-cache"
    result = exe.execute(
        nb, timeout_seconds=180, sandbox=True,
        packages=["httpx"], pip_cache_dir=pip_cache,
    )

    assert result.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"
    assert result.stdout is not None
    assert "httpx" in result.stdout


@pytest.mark.slow
def test_docker_sandbox_packages_network_isolated(
    setup, tmp_path: Path, docker_available: bool,
) -> None:
    """After package install, the execution phase should have no network."""
    if not docker_available:
        pytest.skip("Docker not available")
    db, gen, exe = setup
    code = (
        "import httpx\n"
        "try:\n"
        "    httpx.get('http://httpbin.org/get', timeout=3)\n"
        "    print('network worked')\n"
        "except Exception as e:\n"
        "    print(f'blocked: {e}')\n"
    )
    nb = gen.generate("run_docker_pkg_nonet", "Docker pkg+nonet test", code)
    db.create_run(
        "run_docker_pkg_nonet", "Docker pkg+nonet test", code, str(nb.notebook_path),
    )

    pip_cache = tmp_path / "pip-cache"
    result = exe.execute(
        nb, timeout_seconds=180, sandbox=True,
        packages=["httpx"], pip_cache_dir=pip_cache,
    )

    assert result.status == "success", f"stderr: {result.stderr}\nerror: {result.error}"
    assert result.stdout is not None
    assert "blocked" in result.stdout
    assert "network worked" not in result.stdout
