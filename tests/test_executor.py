"""Unit tests for executor helper functions."""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from marimo_sandbox.executor import NotebookExecutor
from marimo_sandbox.generator import GeneratedNotebook, _has_top_level_return, _validate_syntax

# These helpers live in generator.py but are used by the executor indirectly
# (via NotebookGenerator.generate). Testing them here separately keeps
# test_generator.py focused on notebook output.


def test_validate_syntax_ok() -> None:
    assert _validate_syntax("import os\nprint(os.getcwd())") is None


def test_validate_syntax_empty_string() -> None:
    assert _validate_syntax("") is None


def test_validate_syntax_multiline_ok() -> None:
    code = "\n".join([
        "def greet(name):",
        "    return f'hello {name}'",
        "",
        "print(greet('world'))",
    ])
    assert _validate_syntax(code) is None


def test_validate_syntax_error_returns_string() -> None:
    result = _validate_syntax("x = (1 +")
    assert isinstance(result, str)
    assert "SyntaxError" in result


def test_validate_syntax_error_includes_line_number() -> None:
    result = _validate_syntax("a = 1\nb = (\nc = 3")
    assert result is not None
    # Should mention a line number
    assert any(ch.isdigit() for ch in result)


def test_has_top_level_return_simple() -> None:
    assert _has_top_level_return("return 42") is True


def test_has_top_level_return_in_function_only() -> None:
    code = "def f():\n    return 1\n\nx = f()"
    assert _has_top_level_return(code) is False


def test_has_top_level_return_false_on_no_return() -> None:
    assert _has_top_level_return("x = 1\nprint(x)") is False


def test_has_top_level_return_false_on_syntax_error() -> None:
    # Should not raise even for invalid code
    assert _has_top_level_return("def(:\n    return 1") is False


# ── install_packages tests ────────────────────────────────────────────────────


@pytest.fixture
def executor() -> NotebookExecutor:
    return NotebookExecutor()


def test_install_packages_empty(executor: NotebookExecutor) -> None:
    with patch("subprocess.run") as mock_run:
        result = executor.install_packages([])
        mock_run.assert_not_called()
    assert result["success"] is True
    assert result["output"] == ""
    assert result["freeze"] == ""


def test_install_packages_empty_returns_empty_freeze(executor: NotebookExecutor) -> None:
    result = executor.install_packages([])
    assert "freeze" in result
    assert result["freeze"] == ""


def test_install_packages_uv_success(executor: NotebookExecutor) -> None:
    install_result = MagicMock()
    install_result.returncode = 0
    install_result.stdout = "Successfully installed requests"

    freeze_result = MagicMock()
    freeze_result.returncode = 0
    freeze_result.stdout = "requests==2.31.0\n"

    with patch("subprocess.run", side_effect=[install_result, freeze_result]) as mock_run:
        result = executor.install_packages(["requests"])
        # Two calls: uv install + pip freeze
        assert mock_run.call_count == 2
        first_cmd = mock_run.call_args_list[0][0][0]
        assert first_cmd[0] == "uv"
    assert result["success"] is True
    assert "Successfully installed" in result["output"]
    assert "requests==2.31.0" in result["freeze"]


def test_install_packages_returns_freeze_key(executor: NotebookExecutor) -> None:
    install_result = MagicMock()
    install_result.returncode = 0
    install_result.stdout = "ok"

    freeze_result = MagicMock()
    freeze_result.returncode = 0
    freeze_result.stdout = "numpy==1.26.0\n"

    with patch("subprocess.run", side_effect=[install_result, freeze_result]):
        result = executor.install_packages(["numpy"])

    assert "freeze" in result
    assert result["freeze"] == "numpy==1.26.0\n"


def test_install_packages_uv_missing_falls_back_to_pip(executor: NotebookExecutor) -> None:
    pip_result = MagicMock()
    pip_result.returncode = 0
    pip_result.stdout = "Successfully installed requests"

    freeze_result = MagicMock()
    freeze_result.returncode = 0
    freeze_result.stdout = "requests==2.31.0\n"

    def side_effect(cmd, **kwargs):
        if cmd[0] == "uv":
            raise FileNotFoundError("uv not found")
        if cmd[1] == "freeze" or (len(cmd) > 2 and cmd[2] == "freeze"):
            return freeze_result
        return pip_result

    with patch("subprocess.run", side_effect=side_effect) as mock_run:
        result = executor.install_packages(["requests"])
        # 3 calls: uv (FileNotFoundError) + pip install + pip freeze
        assert mock_run.call_count == 3
    assert result["success"] is True
    assert "freeze" in result


# ── _finish_result tests ──────────────────────────────────────────────────────


def _make_nb(tmp_path: Path, run_id: str = "run_abc") -> GeneratedNotebook:
    nb_path = tmp_path / "notebook.py"
    nb_path.touch()
    return GeneratedNotebook(
        run_id=run_id, notebook_path=nb_path, notebook_dir=tmp_path, content=""
    )


def test_finish_result_success_with_sidecar(tmp_path: Path, executor: NotebookExecutor) -> None:
    nb = _make_nb(tmp_path)
    # Create the sidecar so _finish_result sees success
    nb.result_path.touch()
    result = executor._finish_result(nb, returncode=0, stdout="hi", stderr="", duration_ms=100)
    assert result.status == "success"
    assert result.stdout == "hi"


def test_finish_result_error_no_sidecar_nonzero(tmp_path: Path, executor: NotebookExecutor) -> None:
    nb = _make_nb(tmp_path)
    result = executor._finish_result(
        nb, returncode=1, stdout="", stderr="Traceback...", duration_ms=50
    )
    assert result.status == "error"
    assert result.error == "Traceback..."


def test_finish_result_error_clean_exit_no_sidecar(
    tmp_path: Path, executor: NotebookExecutor
) -> None:
    nb = _make_nb(tmp_path)
    result = executor._finish_result(nb, returncode=0, stdout="", stderr="", duration_ms=10)
    assert result.status == "error"
    assert result.error is not None
    assert "sys.exit" in result.error


# ── execute_async tests ───────────────────────────────────────────────────────


def test_execute_async_returns_popen(tmp_path: Path, executor: NotebookExecutor) -> None:
    nb_path = tmp_path / "notebook.py"
    nb_path.write_text("import sys; sys.exit(0)")
    nb = GeneratedNotebook(
        run_id="run_abc", notebook_path=nb_path, notebook_dir=tmp_path, content=""
    )
    mock_process = MagicMock()
    mock_process.pid = 1234
    with patch("subprocess.Popen", return_value=mock_process) as mock_popen:
        process = executor.execute_async(nb, timeout_seconds=5)
    assert process is mock_process
    mock_popen.assert_called_once()


# ── Docker sandbox tests ─────────────────────────────────────────────────────


def test_run_docker_passes_correct_flags(tmp_path: Path, executor: NotebookExecutor) -> None:
    """Verify _run_docker builds the correct docker run command."""
    nb_path = tmp_path / "notebook.py"
    nb_path.touch()

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ""
    mock_result.stderr = ""

    with patch("subprocess.run", return_value=mock_result) as mock_run:
        executor._run_docker(nb_path, timeout=30)

    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]

    assert cmd[0] == "docker"
    assert cmd[1] == "run"
    assert "--rm" in cmd
    assert "--memory=512m" in cmd
    assert "--cpus=1" in cmd
    assert "--network=none" in cmd
    assert "--read-only" in cmd
    # tmpfs with noexec
    tmpfs_arg = [a for a in cmd if a.startswith("--tmpfs")]
    assert len(tmpfs_arg) == 1
    assert "noexec" in tmpfs_arg[0]
    # Volume mount for notebook dir
    vol_args = [a for a in cmd if a.startswith(str(tmp_path))]
    assert len(vol_args) == 1
    assert vol_args[0].endswith(":/sandbox:rw")
    # Entrypoint is "python", so only the script path is passed as arg
    assert cmd[-1] == f"/sandbox/{nb_path.name}"
    assert cmd[-2] == "marimo-sandbox:latest"  # no extra "python" between image and path


def test_run_docker_uses_configured_image(tmp_path: Path) -> None:
    """Verify custom docker_image is passed to docker run."""
    custom_executor = NotebookExecutor(docker_image="my-custom-image:v2")
    nb_path = tmp_path / "notebook.py"
    nb_path.touch()

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ""
    mock_result.stderr = ""

    with patch("subprocess.run", return_value=mock_result) as mock_run:
        custom_executor._run_docker(nb_path, timeout=30)

    cmd = mock_run.call_args[0][0]
    assert "my-custom-image:v2" in cmd


def test_run_docker_passes_timeout(tmp_path: Path, executor: NotebookExecutor) -> None:
    """Verify timeout is forwarded to subprocess.run."""
    nb_path = tmp_path / "notebook.py"
    nb_path.touch()

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = ""
    mock_result.stderr = ""

    with patch("subprocess.run", return_value=mock_result) as mock_run:
        executor._run_docker(nb_path, timeout=45)

    kwargs = mock_run.call_args[1]
    assert kwargs["timeout"] == 45


def test_execute_sandbox_true_calls_docker(tmp_path: Path, executor: NotebookExecutor) -> None:
    """Verify execute() dispatches to _run_docker when sandbox=True."""
    nb = _make_nb(tmp_path)
    nb.result_path.touch()  # pretend success

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "ok"
    mock_result.stderr = ""

    with patch.object(executor, "_run_docker", return_value=mock_result) as mock_docker, \
         patch.object(executor, "_run_subprocess") as mock_sub:
        result = executor.execute(nb, timeout_seconds=30, sandbox=True)

    mock_docker.assert_called_once_with(
        nb.notebook_path, 30, packages=None, pip_cache_dir=None,
    )
    mock_sub.assert_not_called()
    assert result.status == "success"


def test_execute_sandbox_false_calls_subprocess(
    tmp_path: Path, executor: NotebookExecutor
) -> None:
    """Verify execute() dispatches to _run_subprocess when sandbox=False."""
    nb = _make_nb(tmp_path)
    nb.result_path.touch()

    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "ok"
    mock_result.stderr = ""

    with patch.object(executor, "_run_subprocess", return_value=mock_result) as mock_sub, \
         patch.object(executor, "_run_docker") as mock_docker:
        result = executor.execute(nb, timeout_seconds=30, sandbox=False)

    mock_sub.assert_called_once()
    mock_docker.assert_not_called()
    assert result.status == "success"


def test_run_docker_with_packages_two_phase(tmp_path: Path, executor: NotebookExecutor) -> None:
    """Verify _run_docker runs install phase then execute phase when packages given."""
    nb_path = tmp_path / "notebook.py"
    nb_path.touch()

    install_result = MagicMock()
    install_result.returncode = 0
    install_result.stdout = "installed ok"
    install_result.stderr = ""

    exec_result = MagicMock()
    exec_result.returncode = 0
    exec_result.stdout = "hello"
    exec_result.stderr = ""

    with patch("subprocess.run", side_effect=[install_result, exec_result]) as mock_run:
        result = executor._run_docker(nb_path, timeout=30, packages=["httpx"])

    assert mock_run.call_count == 2
    # First call: install phase (no --network=none)
    install_cmd = mock_run.call_args_list[0][0][0]
    assert "--entrypoint" in install_cmd
    assert "--network=none" not in install_cmd
    assert "--target=/sandbox/.packages" in " ".join(install_cmd)
    # Second call: execute phase (has --network=none and PYTHONPATH)
    exec_cmd = mock_run.call_args_list[1][0][0]
    assert "--network=none" in exec_cmd
    assert "PYTHONPATH=/sandbox/.packages" in exec_cmd
    assert result.stdout == "hello"


def test_run_docker_no_packages_single_phase(tmp_path: Path, executor: NotebookExecutor) -> None:
    """Verify _run_docker skips install phase when no packages."""
    nb_path = tmp_path / "notebook.py"
    nb_path.touch()

    exec_result = MagicMock()
    exec_result.returncode = 0
    exec_result.stdout = ""
    exec_result.stderr = ""

    with patch("subprocess.run", return_value=exec_result) as mock_run:
        executor._run_docker(nb_path, timeout=30)

    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert "--network=none" in cmd
    assert "PYTHONPATH=/sandbox/.packages" not in cmd


def test_run_docker_install_failure_skips_execute(
    tmp_path: Path, executor: NotebookExecutor
) -> None:
    """Verify _run_docker returns error without executing when install fails."""
    nb_path = tmp_path / "notebook.py"
    nb_path.touch()

    install_result = MagicMock()
    install_result.returncode = 1
    install_result.stdout = ""
    install_result.stderr = "no such package"

    with patch("subprocess.run", return_value=install_result) as mock_run:
        result = executor._run_docker(nb_path, timeout=30, packages=["nonexistent"])

    # Only install phase ran, execute was skipped
    mock_run.assert_called_once()
    assert result.returncode == 1
    assert "Package install failed" in result.stderr


def test_docker_install_packages_uses_pip_cache(
    tmp_path: Path, executor: NotebookExecutor
) -> None:
    """Verify _docker_install_packages mounts pip cache volume when provided."""
    nb_dir = tmp_path / "notebooks"
    nb_dir.mkdir()
    cache_dir = tmp_path / "cache"

    install_result = MagicMock()
    install_result.returncode = 0
    install_result.stdout = "ok"
    install_result.stderr = ""

    with patch("subprocess.run", return_value=install_result) as mock_run:
        result = executor._docker_install_packages(
            nb_dir, ["httpx"], pip_cache_dir=cache_dir,
        )

    assert result["success"] is True
    cmd = mock_run.call_args[0][0]
    vol_args = " ".join(cmd)
    assert f"{cache_dir}:/pip-cache:rw" in vol_args
    assert "--cache-dir=/pip-cache" in vol_args


def test_docker_exec_cmd_sets_pythonpath(executor: NotebookExecutor) -> None:
    """Verify _docker_exec_cmd includes PYTHONPATH when has_packages=True."""
    cmd = executor._docker_exec_cmd(Path("/tmp/nb"), "notebook.py", has_packages=True)
    assert "-e" in cmd
    idx = cmd.index("-e")
    assert cmd[idx + 1] == "PYTHONPATH=/sandbox/.packages"


def test_docker_exec_cmd_no_pythonpath_without_packages(executor: NotebookExecutor) -> None:
    """Verify _docker_exec_cmd omits PYTHONPATH when has_packages=False."""
    cmd = executor._docker_exec_cmd(Path("/tmp/nb"), "notebook.py", has_packages=False)
    assert "PYTHONPATH=/sandbox/.packages" not in cmd


def test_execute_async_sandbox_true_uses_docker_cmd(
    tmp_path: Path, executor: NotebookExecutor
) -> None:
    """Verify execute_async builds docker command when sandbox=True."""
    nb_path = tmp_path / "notebook.py"
    nb_path.write_text("print('hi')")
    nb = GeneratedNotebook(
        run_id="run_dock", notebook_path=nb_path, notebook_dir=tmp_path, content=""
    )
    mock_process = MagicMock()
    mock_process.pid = 5678

    with patch("subprocess.Popen", return_value=mock_process) as mock_popen:
        process = executor.execute_async(nb, timeout_seconds=10, sandbox=True)

    cmd = mock_popen.call_args[0][0]
    assert cmd[0] == "docker"
    assert "--network=none" in cmd
    assert "--read-only" in cmd
    assert process is mock_process


def test_execute_docker_timeout_returns_error(
    tmp_path: Path, executor: NotebookExecutor
) -> None:
    """Verify TimeoutExpired from docker run is handled gracefully."""
    nb = _make_nb(tmp_path)

    with patch.object(
        executor, "_run_docker",
        side_effect=subprocess.TimeoutExpired(cmd="docker run", timeout=10),
    ):
        result = executor.execute(nb, timeout_seconds=10, sandbox=True)

    assert result.status == "error"
    assert "Timed out" in result.error


# ── install_packages tests (continued) ──────────────────────────────────────


def test_install_packages_both_fail(executor: NotebookExecutor) -> None:
    uv_result = MagicMock()
    uv_result.returncode = 1
    uv_result.stderr = "uv error: no such package"

    pip_result = MagicMock()
    pip_result.returncode = 1
    pip_result.stderr = "pip error: no such package"

    def side_effect(cmd, **kwargs):
        if cmd[0] == "uv":
            return uv_result
        return pip_result

    with patch("subprocess.run", side_effect=side_effect):
        result = executor.install_packages(["nonexistent-pkg-xyz"])
    assert result["success"] is False
    assert result["output"]  # some error message present
    assert result["freeze"] == ""
