"""Unit tests for NotebookGenerator and helper functions."""

from pathlib import Path

import pytest

from marimo_sandbox.generator import (
    NotebookGenerator,
    _has_top_level_return,
    _validate_syntax,
)


@pytest.fixture
def gen(tmp_path: Path) -> NotebookGenerator:
    return NotebookGenerator(tmp_path / "notebooks")


def test_generates_notebook_file(gen: NotebookGenerator, tmp_path: Path) -> None:
    nb = gen.generate("run_abc", "Hello test", "x = 1")
    assert nb.notebook_path.exists()


def test_run_id_in_content(gen: NotebookGenerator) -> None:
    nb = gen.generate("run_xyz", "My run", "pass")
    assert "run_xyz" in nb.content


def test_user_code_in_content(gen: NotebookGenerator) -> None:
    code = "result = 2 + 2\nprint(result)"
    nb = gen.generate("run_code", "Arithmetic", code)
    assert "result = 2 + 2" in nb.content
    assert "print(result)" in nb.content


def test_jinja2_syntax_not_processed(gen: NotebookGenerator) -> None:
    """Jinja2-style {{ }} markers in user code must pass through unchanged."""
    code = "template = '{{ x }}'\nprint(template)"
    nb = gen.generate("run_jinja", "Jinja test", code)
    assert "{{ x }}" in nb.content


def test_rejects_syntax_error(gen: NotebookGenerator) -> None:
    with pytest.raises(ValueError, match="SyntaxError"):
        gen.generate("run_bad", "Bad syntax", "def foo(:\n    pass")


def test_rejects_top_level_return(gen: NotebookGenerator) -> None:
    with pytest.raises(ValueError, match="top-level"):
        gen.generate("run_ret", "Return test", "x = 1\nreturn x")


def test_sentinel_no_underscore(gen: NotebookGenerator) -> None:
    """The sentinel must be `sandbox_executed`, not `_sandbox_executed`."""
    nb = gen.generate("run_sentinel", "Sentinel check", "pass")
    assert "sandbox_executed = True" in nb.content
    assert "_sandbox_executed" not in nb.content


def test_description_html_escaped_in_context(gen: NotebookGenerator) -> None:
    """HTML-special chars in description must be escaped in the md context cell."""
    nb = gen.generate("run_html", "Test <b>bold</b> & more", "pass")
    assert "&lt;b&gt;" in nb.content
    assert "&amp;" in nb.content


def test_description_raw_in_docstring(gen: NotebookGenerator) -> None:
    """The raw description (unescaped) should appear in the module docstring."""
    nb = gen.generate("run_raw", "Raw <desc>", "pass")
    assert "Raw <desc>" in nb.content


# ── _validate_syntax ─────────────────────────────────────────────────────────


def test_validate_syntax_ok() -> None:
    assert _validate_syntax("x = 1\nprint(x)") is None


def test_validate_syntax_error() -> None:
    result = _validate_syntax("def foo(:\n    pass")
    assert result is not None
    assert "SyntaxError" in result


# ── _has_top_level_return ─────────────────────────────────────────────────────


def test_has_top_level_return_true() -> None:
    assert _has_top_level_return("x = 1\nreturn x") is True


def test_has_top_level_return_false() -> None:
    assert _has_top_level_return("def foo():\n    return 1\nx = foo()") is False


def test_has_top_level_return_syntax_error() -> None:
    # Should not raise; returns False on unparsable code
    assert _has_top_level_return("def(:\n    return") is False
