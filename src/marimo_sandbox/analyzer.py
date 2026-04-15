"""Static risk analysis for user-submitted Python code."""
import ast
from dataclasses import dataclass


@dataclass
class RiskFinding:
    severity: str        # "critical" | "high" | "medium" | "low"
    category: str        # "subprocess" | "shell_execution" | "code_execution"
                         # | "dangerous_import" | "file_write" | "env_read"
    line: int | None
    message: str
    code_snippet: str | None = None


class StaticRiskAnalyzer(ast.NodeVisitor):
    _SUBPROCESS_ATTRS = {"run", "call", "check_call", "check_output", "Popen"}
    _DANGEROUS_IMPORTS = {
        "subprocess", "os", "socket", "requests", "urllib",
        "http", "paramiko", "smtplib", "ftplib",
    }

    def __init__(self, code: str) -> None:
        self._code = code
        self._lines = code.splitlines()
        self._findings: list[RiskFinding] = []

    def analyze(self) -> list[RiskFinding]:
        try:
            tree = ast.parse(self._code)
        except SyntaxError:
            # Already caught by generator; return empty, don't double-report
            return []
        self.visit(tree)
        return self._findings

    def _snippet(self, lineno: int | None) -> str | None:
        if lineno and 1 <= lineno <= len(self._lines):
            return self._lines[lineno - 1]
        return None

    def _add(
        self,
        severity: str,
        category: str,
        lineno: int | None,
        message: str,
    ) -> None:
        self._findings.append(
            RiskFinding(
                severity=severity,
                category=category,
                line=lineno,
                message=message,
                code_snippet=self._snippet(lineno),
            )
        )

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute):
            value = node.func.value
            attr = node.func.attr
            if isinstance(value, ast.Name):
                if value.id == "subprocess" and attr in self._SUBPROCESS_ATTRS:
                    self._add(
                        "critical", "subprocess", node.lineno,
                        f"subprocess.{attr}() detected",
                    )
                elif value.id == "os" and attr in {"system", "popen"}:
                    self._add(
                        "critical", "shell_execution", node.lineno,
                        f"os.{attr}() detected",
                    )
        elif isinstance(node.func, ast.Name):
            name = node.func.id
            if name in {"eval", "exec", "compile"}:
                self._add(
                    "critical", "code_execution", node.lineno,
                    f"{name}() detected",
                )
            elif name == "open":
                self._check_open_mode(node)
        self.generic_visit(node)

    def _check_open_mode(self, node: ast.Call) -> None:
        mode_val: str | None = None
        if len(node.args) >= 2:
            arg = node.args[1]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                mode_val = arg.value
        else:
            for kw in node.keywords:
                if kw.arg == "mode" and isinstance(kw.value, ast.Constant):
                    mode_val = kw.value.value  # type: ignore[assignment]
                    break
        if mode_val and ("w" in mode_val or "a" in mode_val):
            self._add(
                "medium", "file_write", node.lineno,
                f"open() with mode '{mode_val}' detected",
            )

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            base = alias.name.split(".")[0]
            if base in self._DANGEROUS_IMPORTS:
                self._add(
                    "high", "dangerous_import", node.lineno,
                    f"import {alias.name} detected",
                )
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        base = module.split(".")[0]
        if base in self._DANGEROUS_IMPORTS:
            self._add(
                "high", "dangerous_import", node.lineno,
                f"from {module} import ... detected",
            )
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if (
            isinstance(node.value, ast.Attribute)
            and isinstance(node.value.value, ast.Name)
            and node.value.value.id == "os"
            and node.value.attr == "environ"
        ):
            self._add(
                "low", "env_read", node.lineno,
                "os.environ[] access detected",
            )
        self.generic_visit(node)
