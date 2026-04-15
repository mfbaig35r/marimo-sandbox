"""
Notebook executor for marimo-sandbox.

Execution modes
---------------
subprocess (default)
    Runs `python notebook.py` as a child process. Marimo's app.run() in the
    __main__ guard executes all cells in topological (dependency) order.
    Stdout/stderr are captured. Safe for trusted code.

docker (sandbox=True)
    Same subprocess approach but inside Docker with --network=none, memory
    cap, CPU cap, and read-only root filesystem. Requires Docker CLI.

interactive
    Launches `marimo edit notebook.py` and polls the port until ready.
    Returns a browser URL. Used for inspecting/debugging runs.

Success detection
-----------------
The __record__ cell (which only runs when __execution__ succeeds) writes a
JSON sidecar file: {run_id}_result.json in the notebook's directory.
The executor checks for this file to determine success vs failure, rather
than relying on the subprocess return code alone (which can be 0 even when
sys.exit() was called before __record__ ran).
"""

import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from .generator import GeneratedNotebook


@dataclass
class ExecutionResult:
    status: str              # "success" | "error"
    duration_ms: int
    stdout: str | None = None
    stderr: str | None = None
    error: str | None = None  # human-readable error message


class NotebookExecutor:
    def __init__(self, docker_image: str = "marimo-sandbox:latest") -> None:
        self.docker_image = docker_image

    # ── Main execution entry point ───────────────────────────────────────────

    def execute(
        self,
        notebook: GeneratedNotebook,
        timeout_seconds: int = 60,
        sandbox: bool = False,
        python_path: Path | None = None,
    ) -> ExecutionResult:
        start = time.monotonic()

        try:
            if sandbox:
                raw = self._run_docker(notebook.notebook_path, timeout_seconds)
            else:
                raw = self._run_subprocess(
                    notebook.notebook_path, timeout_seconds, python_path=python_path
                )
        except subprocess.TimeoutExpired:
            return ExecutionResult(
                status="error",
                duration_ms=int((time.monotonic() - start) * 1000),
                error=f"Timed out after {timeout_seconds}s",
            )
        except Exception as exc:
            return ExecutionResult(
                status="error",
                duration_ms=int((time.monotonic() - start) * 1000),
                error=f"Failed to launch notebook: {exc}",
            )

        duration_ms = int((time.monotonic() - start) * 1000)
        return self._finish_result(
            notebook, raw.returncode, raw.stdout or "", raw.stderr or "", duration_ms
        )

    def _finish_result(
        self,
        notebook: GeneratedNotebook,
        returncode: int,
        stdout: str,
        stderr: str,
        duration_ms: int,
    ) -> ExecutionResult:
        """Determine success/error from returncode and sidecar file."""
        # Sidecar written → __execution__ and __record__ both completed
        if notebook.result_path.exists():
            return ExecutionResult(
                status="success",
                duration_ms=duration_ms,
                stdout=stdout or None,
                stderr=stderr or None,
            )

        # No sidecar but clean exit → likely sys.exit() in user code
        if returncode == 0:
            return ExecutionResult(
                status="error",
                duration_ms=duration_ms,
                stdout=stdout or None,
                stderr=stderr or None,
                error=(
                    "Notebook exited before writing results. "
                    "Was sys.exit() called in the code?"
                ),
            )

        # Non-zero exit → uncaught exception; traceback is in stderr
        stderr_stripped = stderr.strip()
        return ExecutionResult(
            status="error",
            duration_ms=duration_ms,
            stdout=stdout or None,
            stderr=stderr or None,
            error=stderr_stripped or "Execution failed (non-zero exit, no stderr captured)",
        )

    def execute_async(
        self,
        notebook: GeneratedNotebook,
        timeout_seconds: int = 60,
        sandbox: bool = False,
        python_path: Path | None = None,
    ) -> subprocess.Popen:
        """Launch execution; return the Popen immediately (don't wait)."""
        interpreter = str(python_path) if python_path else sys.executable
        if sandbox:
            notebook_dir = notebook.notebook_path.parent
            cmd = [
                "docker", "run",
                "--rm",
                "--memory=512m",
                "--cpus=1",
                "--network=none",
                "--read-only",
                "--tmpfs=/tmp:size=64m,noexec",
                "-v", f"{notebook_dir}:/sandbox:rw",
                "-w", "/sandbox",
                self.docker_image,
                "python", f"/sandbox/{notebook.notebook_path.name}",
            ]
        else:
            cmd = [interpreter, str(notebook.notebook_path)]
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(notebook.notebook_path.parent),
        )

    # ── Subprocess runners ───────────────────────────────────────────────────

    def _run_subprocess(
        self, notebook_path: Path, timeout: int, python_path: Path | None = None
    ) -> subprocess.CompletedProcess:
        interpreter = str(python_path) if python_path else sys.executable
        return subprocess.run(
            [interpreter, str(notebook_path)],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(notebook_path.parent),
        )

    def _run_docker(
        self, notebook_path: Path, timeout: int
    ) -> subprocess.CompletedProcess:
        notebook_dir = notebook_path.parent
        cmd = [
            "docker", "run",
            "--rm",
            "--memory=512m",
            "--cpus=1",
            "--network=none",
            "--read-only",
            "--tmpfs=/tmp:size=64m,noexec",
            "-v", f"{notebook_dir}:/sandbox:rw",
            "-w", "/sandbox",
            self.docker_image,
            "python", f"/sandbox/{notebook_path.name}",
        ]
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    # ── Interactive mode ─────────────────────────────────────────────────────

    def open_interactive(
        self, notebook_path: Path, port: int = 2718
    ) -> dict:
        """
        Launch `marimo edit` and poll until the server is ready.

        Returns a dict with success, url, pid, or error.
        Uses socket polling instead of time.sleep() so startup is detected
        as soon as the server is ready (not after a fixed delay).
        """
        process = subprocess.Popen(
            [
                "marimo", "edit",
                str(notebook_path),
                "--port", str(port),
                "--no-token",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            # Process died immediately — capture its stderr for the error
            if process.poll() is not None:
                raw_err = process.stderr.read() if process.stderr is not None else b""
                stderr = raw_err.decode(errors="replace")
                return {
                    "success": False,
                    "error": f"marimo exited immediately: {stderr[:400]}",
                }
            if _port_is_open(port):
                return {
                    "success": True,
                    "url": f"http://localhost:{port}",
                    "pid": process.pid,
                    "notebook_path": str(notebook_path),
                    "message": "Notebook is open. Navigate to the URL to view it.",
                }
            time.sleep(0.25)

        process.terminate()
        return {
            "success": False,
            "error": "marimo did not become ready within 15 seconds",
        }

    # ── Package installation ─────────────────────────────────────────────────

    def install_packages(self, packages: list[str]) -> dict:
        """Install packages via uv (fallback: pip). Returns {success, output, freeze}."""
        if not packages:
            return {"success": True, "output": "", "freeze": ""}
        last_error = "no installer found"
        for cmd in [
            ["uv", "pip", "install", *packages],
            [sys.executable, "-m", "pip", "install", *packages],
        ]:
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                if r.returncode == 0:
                    freeze_r = subprocess.run(
                        [sys.executable, "-m", "pip", "freeze"],
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    freeze = freeze_r.stdout if freeze_r.returncode == 0 else ""
                    return {"success": True, "output": r.stdout, "freeze": freeze}
                last_error = r.stderr
            except FileNotFoundError:
                last_error = f"{cmd[0]} not found"
        return {"success": False, "output": last_error, "freeze": ""}

    @staticmethod
    def check_uv() -> bool:
        try:
            result = subprocess.run(["uv", "--version"], capture_output=True, text=True, timeout=5)
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

    # ── Environment checks ───────────────────────────────────────────────────

    @staticmethod
    def check_marimo() -> bool:
        try:
            result = subprocess.run(
                ["marimo", "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

    @staticmethod
    def check_docker() -> bool:
        try:
            result = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False


# ── Helpers ──────────────────────────────────────────────────────────────────


def _port_is_open(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=0.5):
            return True
    except OSError:
        return False
