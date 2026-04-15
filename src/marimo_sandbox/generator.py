"""
Marimo notebook generator for marimo-sandbox.

Cell model rules enforced here:
  - Cells communicate via return values, never via mutation of shared state.
  - __execution__ returns a sentinel (sandbox_executed) and __outputs__ dict
    that __record__ declares as parameters — this creates explicit dependency
    edges in the DAG so __record__ only runs if __execution__ succeeded.
  - User code is injected via string substitution AFTER the %%PLACEHOLDER%%
    markers are resolved, so user code is never processed by any template engine
    (avoids {{ }} / {% %} conflicts with Jinja2-style syntax in user code).
  - All intermediate variables inside cells use _ prefix (cell-local scope).
"""

import ast
import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ── Template ─────────────────────────────────────────────────────────────────

# Placeholder line replaced after %%...%% substitution; never template-processed.
_CODE_PLACEHOLDER = "    # __SANDBOX_USER_CODE__"

_TEMPLATE = '''\
"""
Marimo Sandbox Run
==================
Description : %%DESCRIPTION%%
Run ID      : %%RUN_ID%%
Generated   : %%GENERATED_AT%%

To open interactively:
    marimo edit "%%NOTEBOOK_PATH%%"
"""
import marimo

app = marimo.App(width="medium")


@app.cell
def __setup__():
    import marimo as mo
    return (mo,)


@app.cell
def __context__(mo):
    mo.md("""
    # %%DESCRIPTION_ESC%%

    | | |
    |---|---|
    | **Run ID** | `%%RUN_ID%%` |
    | **Generated** | %%GENERATED_AT%% |
    """)
    return ()


@app.cell
def __execution__():
    __outputs__: dict = {}
    # ── USER CODE ─────────────────────────────────────────────────────────────
    # __SANDBOX_USER_CODE__
    # ── END USER CODE ─────────────────────────────────────────────────────────

    sandbox_executed = True
    return (sandbox_executed, __outputs__)


@app.cell
def __record__(sandbox_executed, __outputs__, mo):
    import json as _json
    import os as _os
    import pathlib as _pathlib
    import datetime as _dt

    _ = sandbox_executed  # dependency anchor

    _outputs = __outputs__ if isinstance(__outputs__, dict) else {}
    _result = {
        "run_id": "%%RUN_ID%%",
        "status": "success",
        "executed_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "outputs": _outputs,
    }
    _nb_dir = (
        _pathlib.Path(_os.path.abspath(__file__)).parent
        if "__file__" in globals()
        else _pathlib.Path(".")
    )
    try:
        (_nb_dir / "%%RUN_ID%%_result.json").write_text(
            _json.dumps(_result, indent=2, default=str)
        )
    except OSError:
        pass

    mo.md("✅ **Run complete**")
    return ()


if __name__ == "__main__":
    app.run()
'''


# ── Code validation ──────────────────────────────────────────────────────────


def _validate_syntax(code: str) -> Optional[str]:
    """Return an error string if code has a syntax error, else None."""
    try:
        ast.parse(code)
        return None
    except SyntaxError as exc:
        return f"SyntaxError on line {exc.lineno}: {exc.msg}"


def _has_top_level_return(code: str) -> bool:
    """
    Detect bare `return` at module scope.

    A top-level return inside a Marimo cell function would prematurely exit
    __execution__ before the sandbox_executed sentinel is set, breaking the
    dependency chain. Users should wrap such code in a function.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return False
    return any(
        isinstance(node, ast.Return)
        for node in ast.iter_child_nodes(tree)
    )


# ── Indentation ──────────────────────────────────────────────────────────────


def _indent(code: str, spaces: int = 4) -> str:
    """
    Indent all non-empty lines by `spaces` spaces.
    Empty lines are left as truly empty (no trailing whitespace).
    """
    prefix = " " * spaces
    return "\n".join(
        prefix + line if line.strip() else ""
        for line in code.splitlines()
    )


# ── Public API ───────────────────────────────────────────────────────────────


class GeneratedNotebook:
    def __init__(
        self,
        run_id: str,
        notebook_path: Path,
        notebook_dir: Path,
        content: str,
    ) -> None:
        self.run_id = run_id
        self.notebook_path = notebook_path
        self.notebook_dir = notebook_dir
        self.content = content

    @property
    def result_path(self) -> Path:
        """Path where __record__ writes the JSON sidecar on success."""
        return self.notebook_dir / f"{self.run_id}_result.json"


class NotebookGenerator:
    def __init__(self, notebooks_dir: Path) -> None:
        self.notebooks_dir = notebooks_dir
        self.notebooks_dir.mkdir(parents=True, exist_ok=True)

    def generate(
        self,
        run_id: str,
        description: str,
        code: str,
    ) -> GeneratedNotebook:
        """
        Generate a Marimo notebook for the given Python code.

        Raises ValueError on syntax errors or top-level return statements.
        """
        if err := _validate_syntax(code):
            raise ValueError(err)
        if _has_top_level_return(code):
            raise ValueError(
                "Code contains a top-level `return` statement. "
                "Wrap it in a function — top-level return exits the cell "
                "before the dependency sentinel is set."
            )

        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        notebook_dir = self.notebooks_dir / run_id
        notebook_dir.mkdir(parents=True, exist_ok=True)
        notebook_path = notebook_dir / "notebook.py"

        # Apply %%PLACEHOLDER%% substitutions (user code NOT included here)
        rendered = (
            _TEMPLATE
            .replace("%%RUN_ID%%", run_id)
            .replace("%%DESCRIPTION%%", description)
            .replace("%%DESCRIPTION_ESC%%", html.escape(description))
            .replace("%%GENERATED_AT%%", generated_at)
            .replace("%%NOTEBOOK_PATH%%", str(notebook_path))
        )

        # Inject user code via string substitution — %% markers never touch
        # user code, so {{ }}, {# #}, {% %} in user code are preserved as-is.
        code_indented = _indent(code, spaces=4)
        content = rendered.replace(_CODE_PLACEHOLDER, code_indented)

        notebook_path.write_text(content, encoding="utf-8")

        return GeneratedNotebook(
            run_id=run_id,
            notebook_path=notebook_path,
            notebook_dir=notebook_dir,
            content=content,
        )
