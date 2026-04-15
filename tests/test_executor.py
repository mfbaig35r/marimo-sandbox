"""Unit tests for executor helper functions."""

from unittest.mock import MagicMock, patch

import pytest

from marimo_sandbox.executor import NotebookExecutor
from marimo_sandbox.generator import _has_top_level_return, _validate_syntax

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


def test_install_packages_uv_success(executor: NotebookExecutor) -> None:
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "Successfully installed requests"
    with patch("subprocess.run", return_value=mock_result) as mock_run:
        result = executor.install_packages(["requests"])
        # Only one call — uv succeeded, pip should not be called
        assert mock_run.call_count == 1
        called_cmd = mock_run.call_args[0][0]
        assert called_cmd[0] == "uv"
    assert result["success"] is True
    assert "Successfully installed" in result["output"]


def test_install_packages_uv_missing_falls_back_to_pip(executor: NotebookExecutor) -> None:
    pip_result = MagicMock()
    pip_result.returncode = 0
    pip_result.stdout = "Successfully installed requests"

    def side_effect(cmd, **kwargs):
        if cmd[0] == "uv":
            raise FileNotFoundError("uv not found")
        return pip_result

    with patch("subprocess.run", side_effect=side_effect) as mock_run:
        result = executor.install_packages(["requests"])
        assert mock_run.call_count == 2
    assert result["success"] is True


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
