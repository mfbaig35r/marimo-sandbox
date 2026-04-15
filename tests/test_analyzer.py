"""Unit tests for StaticRiskAnalyzer."""

from marimo_sandbox.analyzer import RiskFinding, StaticRiskAnalyzer


def _analyze(code: str) -> list[RiskFinding]:
    return StaticRiskAnalyzer(code).analyze()


# ── Clean code ────────────────────────────────────────────────────────────────


def test_no_findings_clean_code() -> None:
    code = "x = 1 + 2\nprint(x)\nresult = [i**2 for i in range(10)]"
    findings = _analyze(code)
    assert findings == []


# ── Critical: subprocess ──────────────────────────────────────────────────────


def test_detects_subprocess_run() -> None:
    code = "import subprocess\nsubprocess.run(['ls', '-la'])"
    findings = _analyze(code)
    critical = [f for f in findings if f.severity == "critical" and f.category == "subprocess"]
    assert len(critical) >= 1
    assert any("subprocess.run" in f.message for f in critical)


def test_detects_subprocess_popen() -> None:
    code = "import subprocess\nproc = subprocess.Popen(['bash', '-c', 'whoami'])"
    findings = _analyze(code)
    critical = [f for f in findings if f.severity == "critical" and f.category == "subprocess"]
    assert len(critical) >= 1
    assert any("subprocess.Popen" in f.message for f in critical)


# ── Critical: shell_execution ─────────────────────────────────────────────────


def test_detects_os_system() -> None:
    code = "import os\nos.system('rm -rf /')"
    findings = _analyze(code)
    critical = [f for f in findings if f.severity == "critical" and f.category == "shell_execution"]
    assert len(critical) >= 1
    assert any("os.system" in f.message for f in critical)


def test_detects_os_popen() -> None:
    code = "import os\nos.popen('cat /etc/passwd')"
    findings = _analyze(code)
    critical = [f for f in findings if f.severity == "critical" and f.category == "shell_execution"]
    assert len(critical) >= 1
    assert any("os.popen" in f.message for f in critical)


# ── Critical: code_execution ──────────────────────────────────────────────────


def test_detects_eval() -> None:
    code = 'result = eval("1 + 2")'
    findings = _analyze(code)
    critical = [f for f in findings if f.severity == "critical" and f.category == "code_execution"]
    assert len(critical) >= 1
    assert any("eval" in f.message for f in critical)


def test_detects_exec() -> None:
    code = 'exec("import os; os.system(\'id\')")'
    findings = _analyze(code)
    critical = [f for f in findings if f.severity == "critical" and f.category == "code_execution"]
    assert len(critical) >= 1
    assert any("exec" in f.message for f in critical)


def test_detects_compile() -> None:
    code = 'code_obj = compile("x = 1", "<string>", "exec")'
    findings = _analyze(code)
    critical = [f for f in findings if f.severity == "critical" and f.category == "code_execution"]
    assert len(critical) >= 1
    assert any("compile" in f.message for f in critical)


# ── High: dangerous_import ────────────────────────────────────────────────────


def test_detects_requests_import() -> None:
    code = "import requests\nresp = requests.get('http://example.com')"
    findings = _analyze(code)
    high = [f for f in findings if f.severity == "high" and f.category == "dangerous_import"]
    assert len(high) >= 1
    assert any("requests" in f.message for f in high)


def test_os_import_not_flagged_as_high() -> None:
    code = "import os\nprint(os.getcwd())"
    findings = _analyze(code)
    high = [f for f in findings if f.severity == "high" and f.category == "dangerous_import"]
    assert not any("import os" in f.message for f in high)


def test_detects_socket_import() -> None:
    code = "import socket\ns = socket.socket()"
    findings = _analyze(code)
    high = [f for f in findings if f.severity == "high" and f.category == "dangerous_import"]
    assert len(high) >= 1
    assert any("socket" in f.message for f in high)


def test_detects_subprocess_import_as_high() -> None:
    code = "import subprocess"
    findings = _analyze(code)
    high = [f for f in findings if f.severity == "high" and f.category == "dangerous_import"]
    assert any("subprocess" in f.message for f in high)


# ── Medium: file_write ────────────────────────────────────────────────────────


def test_detects_file_write_mode() -> None:
    code = "with open('output.txt', 'w') as f:\n    f.write('hello')"
    findings = _analyze(code)
    medium = [f for f in findings if f.severity == "medium" and f.category == "file_write"]
    assert len(medium) >= 1
    assert any("'w'" in f.message for f in medium)


def test_detects_file_append_mode() -> None:
    code = "f = open('log.txt', 'a')\nf.write('entry')"
    findings = _analyze(code)
    medium = [f for f in findings if f.severity == "medium" and f.category == "file_write"]
    assert len(medium) >= 1


def test_no_finding_for_read_mode() -> None:
    code = "with open('data.txt', 'r') as f:\n    content = f.read()"
    findings = _analyze(code)
    write_findings = [f for f in findings if f.category == "file_write"]
    assert write_findings == []


# ── Low: env_read ─────────────────────────────────────────────────────────────


def test_detects_env_read() -> None:
    code = "import os\napi_key = os.environ['API_KEY']"
    findings = _analyze(code)
    low = [f for f in findings if f.severity == "low" and f.category == "env_read"]
    assert len(low) >= 1
    assert any("os.environ" in f.message for f in low)


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_syntax_error_returns_empty_list() -> None:
    findings = _analyze("def foo(:\n    pass")
    assert findings == []


def test_finding_includes_line_number() -> None:
    code = "x = 1\neval('2 + 2')"
    findings = _analyze(code)
    eval_finding = next(f for f in findings if f.category == "code_execution")
    assert eval_finding.line == 2


def test_finding_includes_code_snippet() -> None:
    code = "x = 1\nresult = eval('2 + 2')"
    findings = _analyze(code)
    eval_finding = next(f for f in findings if f.category == "code_execution")
    assert eval_finding.code_snippet is not None
    assert "eval" in eval_finding.code_snippet
