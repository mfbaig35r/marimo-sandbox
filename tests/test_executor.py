"""Unit tests for executor helper functions."""

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
