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

    When packages are requested, execution is two-phase:
      1. Install phase — docker run WITH network, pip install --target into
         the notebook directory. Uses a shared pip cache volume.
      2. Execute phase — docker run with --network=none, PYTHONPATH pointing
         at the installed packages. Full sandbox restrictions apply.

Success detection
-----------------
The __record__ cell (which only runs when __execution__ succeeds) writes a
JSON sidecar file: {run_id}_result.json in the notebook's directory.
The executor checks for this file to determine success vs failure, rather
than relying on the subprocess return code alone (which can be 0 even when
sys.exit() was called before __record__ ran).
"""

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
        packages: list[str] | None = None,
        pip_cache_dir: Path | None = None,
    ) -> ExecutionResult:
        start = time.monotonic()

        try:
            if sandbox:
                raw = self._run_docker(
                    notebook.notebook_path, timeout_seconds,
                    packages=packages, pip_cache_dir=pip_cache_dir,
                )
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
        packages: list[str] | None = None,
        pip_cache_dir: Path | None = None,
    ) -> subprocess.Popen:
        """Launch execution; return the Popen immediately (don't wait).

        When sandbox=True and packages are provided, the install phase runs
        synchronously before the async execution Popen is returned.
        """
        interpreter = str(python_path) if python_path else sys.executable
        if sandbox:
            notebook_dir = notebook.notebook_path.parent
            # Install packages synchronously before launching async execution
            if packages:
                install = self._docker_install_packages(
                    notebook_dir, packages, pip_cache_dir=pip_cache_dir,
                )
                if not install["success"]:
                    raise RuntimeError(
                        f"Docker package install failed: {install['output']}"
                    )
            cmd = self._docker_exec_cmd(notebook_dir, notebook.notebook_path.name,
                                        has_packages=bool(packages))
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
        self,
        notebook_path: Path,
        timeout: int,
        packages: list[str] | None = None,
        pip_cache_dir: Path | None = None,
    ) -> subprocess.CompletedProcess:
        notebook_dir = notebook_path.parent

        # Phase 1: install packages inside a container (with network access)
        if packages:
            install = self._docker_install_packages(
                notebook_dir, packages, pip_cache_dir=pip_cache_dir,
            )
            if not install["success"]:
                # Return a synthetic failed CompletedProcess
                return subprocess.CompletedProcess(
                    args=[], returncode=1, stdout="",
                    stderr=f"Package install failed:\n{install['output']}",
                )

        # Phase 2: execute notebook (fully sandboxed)
        cmd = self._docker_exec_cmd(
            notebook_dir, notebook_path.name, has_packages=bool(packages),
        )
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

    def _docker_exec_cmd(
        self, notebook_dir: Path, notebook_name: str, has_packages: bool = False,
    ) -> list[str]:
        """Build the docker run command for the execution phase."""
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
        ]
        if has_packages:
            cmd += ["-e", "PYTHONPATH=/sandbox/.packages"]
        cmd += [self.docker_image, f"/sandbox/{notebook_name}"]
        return cmd

    def _docker_install_packages(
        self,
        notebook_dir: Path,
        packages: list[str],
        pip_cache_dir: Path | None = None,
        timeout: int = 120,
    ) -> dict:
        """Run a Docker container to install packages into notebook_dir/.packages.

        Returns {"success": bool, "output": str}.
        The install container has network access but no other sandbox relaxations.
        """
        volumes = ["-v", f"{notebook_dir}:/sandbox:rw"]
        if pip_cache_dir:
            pip_cache_dir.mkdir(parents=True, exist_ok=True)
            volumes += ["-v", f"{pip_cache_dir}:/pip-cache:rw"]

        # Try uv first (faster), fall back to pip
        # The Dockerfile installs uv; if it's missing, fall back to pip.
        install_cmd = (
            "uv pip install --no-python-downloads "
            f"{'--cache-dir=/pip-cache ' if pip_cache_dir else ''}"
            f"--target=/sandbox/.packages {' '.join(packages)} "
            "2>&1 || "
            "pip install --no-warn-script-location "
            f"{'--cache-dir=/pip-cache ' if pip_cache_dir else ''}"
            f"--target=/sandbox/.packages {' '.join(packages)} 2>&1"
        )

        cmd = [
            "docker", "run",
            "--rm",
            "--memory=512m",
            "--cpus=1",
            # Network allowed (need to download packages)
            # No --read-only (pip needs writable /tmp for builds)
            "--tmpfs=/tmp:size=256m",
            *volumes,
            "-w", "/sandbox",
            "--entrypoint", "sh",
            self.docker_image,
            "-c", install_cmd,
        ]

        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            if r.returncode == 0:
                return {"success": True, "output": r.stdout}
            return {"success": False, "output": r.stderr or r.stdout}
        except subprocess.TimeoutExpired:
            return {"success": False, "output": f"Package install timed out after {timeout}s"}
        except FileNotFoundError:
            return {"success": False, "output": "Docker not found"}

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

    @staticmethod
    def get_marimo_version(marimo_bin: str = "marimo") -> str | None:
        """Return the version string from ``marimo --version``, or None."""
        try:
            result = subprocess.run(
                [marimo_bin, "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip().split()[-1]
        except (FileNotFoundError, subprocess.TimeoutExpired, IndexError):
            pass
        return None
