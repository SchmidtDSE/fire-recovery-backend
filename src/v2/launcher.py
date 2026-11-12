"""Starting and watching analysis jobs (design §1).

The API keeps no job state. While an analysis runs, the job itself is the
state: a Kubernetes Job named after the job id on a cluster, or a child process
locally. Kubernetes refuses a second Job with the same name, so two identical
submissions can't start two runs, and no lock is needed.
"""

import os
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Literal, Mapping, Optional, Protocol, Sequence

from src.v2.constants import ENV_IMAGE

JobPhase = Literal["pending", "running", "succeeded", "failed", "unknown"]

# The variables a job reads (see src/v2/run.py).
ENV_REQUEST = "SEVERITY_REQUEST"
ENV_JOB_ID = "JOB_ID"
ENV_NOTIFY_EMAIL = "NOTIFY_EMAIL"
ENV_STATUS_URL = "STATUS_URL"

REPO_ROOT = Path(__file__).resolve().parents[2]
RUN_COMMAND = (sys.executable, "-m", "src.v2.run")


class DispatchUnavailable(RuntimeError):
    """The launcher isn't configured, or can't reach its cluster."""


@dataclass(frozen=True)
class JobStatus:
    """What the platform knows about a job, apart from its result record."""

    phase: JobPhase
    reason: Optional[str] = None

    @property
    def active(self) -> bool:
        return self.phase in ("pending", "running")

    @property
    def finished(self) -> bool:
        return self.phase in ("succeeded", "failed")


@dataclass(frozen=True)
class JobSpec:
    """What a job needs: the canonical request, and where to report."""

    job_id: str
    canonical_json: str
    status_url: str
    notify_email: Optional[str] = None

    def environment(self) -> Dict[str, str]:
        env = {
            ENV_REQUEST: self.canonical_json,
            ENV_JOB_ID: self.job_id,
            ENV_STATUS_URL: self.status_url,
        }
        if self.notify_email:
            env[ENV_NOTIFY_EMAIL] = self.notify_email
        return env


class Launcher(Protocol):
    """Starts jobs and reports on them."""

    def start(self, spec: JobSpec) -> bool:
        """Start a job; False if one with this id already exists (running or finished)."""
        ...

    def status(self, job_id: str) -> JobStatus:
        """The job's phase; "unknown" if no job with this id exists."""
        ...

    def delete(self, job_id: str) -> None:
        """Remove a finished job, so the id can run again. A missing job is fine."""
        ...

    def active_count(self) -> int:
        """How many jobs are pending or running."""
        ...


class SubprocessLauncher:
    """Runs each job as a child process: for local work and tests.

    Children end with the API process, and the table of them is in memory, so
    this is no substitute for a cluster.
    """

    def __init__(
        self,
        command: Sequence[str] = RUN_COMMAND,
        env: Optional[Mapping[str, str]] = None,
        cwd: Path = REPO_ROOT,
    ):
        self._command = list(command)
        self._env = dict(os.environ if env is None else env)
        self._cwd = cwd
        self._processes: Dict[str, subprocess.Popen[bytes]] = {}
        self._lock = threading.Lock()

    def start(self, spec: JobSpec) -> bool:
        with self._lock:
            if spec.job_id in self._processes:
                return False
            env = {**self._env, **spec.environment()}
            env.setdefault(ENV_IMAGE, "local")
            self._processes[spec.job_id] = subprocess.Popen(
                self._command, env=env, cwd=self._cwd
            )
            return True

    def status(self, job_id: str) -> JobStatus:
        with self._lock:
            process = self._processes.get(job_id)
        if process is None:
            return JobStatus("unknown")
        code = process.poll()
        if code is None:
            return JobStatus("running")
        if code == 0:
            return JobStatus("succeeded")
        return JobStatus("failed", reason=f"exited with status {code}")

    def delete(self, job_id: str) -> None:
        with self._lock:
            process = self._processes.get(job_id)
            if process is not None and process.poll() is not None:
                del self._processes[job_id]

    def active_count(self) -> int:
        with self._lock:
            return sum(1 for p in self._processes.values() if p.poll() is None)
