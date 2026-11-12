"""The subprocess launcher, used locally and in tests."""

import json
import sys
import time
from pathlib import Path

from src.v2.service.launcher import (
    REPO_ROOT,
    RUN_COMMAND,
    JobSpec,
    JobStatus,
    SubprocessLauncher,
)

# A stand-in for `python -m src.v2.job.run`: writes its environment, exits with $EXIT.
FAKE_JOB = (
    "import json, os, sys, time; "
    "open(os.environ['OUT'], 'w').write(json.dumps(dict(os.environ))); "
    "time.sleep(float(os.environ.get('SLEEP', '0'))); "
    "sys.exit(int(os.environ.get('EXIT', '0')))"
)


def wait(launcher: SubprocessLauncher, job_id: str, timeout: float = 20.0) -> JobStatus:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status = launcher.status(job_id)
        if status.finished:
            return status
        time.sleep(0.05)
    raise AssertionError("job did not finish")


def spec(job_id: str = "job-1", email: str = "") -> JobSpec:
    return JobSpec(
        job_id=job_id,
        canonical_json='{"operation":"fire_severity_v2"}',
        status_url=f"http://api/v2/severity/{job_id}",
        notify_email=email or None,
    )


def launcher(tmp_path: Path, **env: str) -> SubprocessLauncher:
    return SubprocessLauncher(
        command=[sys.executable, "-c", FAKE_JOB],
        env={"OUT": str(tmp_path / "env.json"), **env},
        cwd=tmp_path,
    )


def test_start_passes_the_request_and_reports_success(tmp_path: Path) -> None:
    runner = launcher(tmp_path)
    assert runner.start(spec(email="someone@example.org"))
    assert wait(runner, "job-1") == JobStatus("succeeded")
    env = json.loads((tmp_path / "env.json").read_text())
    assert env["SEVERITY_REQUEST"] == '{"operation":"fire_severity_v2"}'
    assert env["JOB_ID"] == "job-1"
    assert env["STATUS_URL"] == "http://api/v2/severity/job-1"
    assert env["NOTIFY_EMAIL"] == "someone@example.org"


def test_failure_has_a_reason(tmp_path: Path) -> None:
    runner = launcher(tmp_path, EXIT="1")
    runner.start(spec())
    status = wait(runner, "job-1")
    assert status.phase == "failed" and "1" in (status.reason or "")


def test_one_job_per_id_until_deleted(tmp_path: Path) -> None:
    runner = launcher(tmp_path, SLEEP="0.5")
    assert runner.start(spec())
    assert runner.status("job-1").active
    assert runner.active_count() == 1
    assert not runner.start(spec())
    runner.delete("job-1")  # still running: kept
    assert runner.status("job-1").active
    wait(runner, "job-1")
    assert not runner.start(spec())  # finished jobs stay until deleted
    runner.delete("job-1")
    assert runner.status("job-1") == JobStatus("unknown")
    assert runner.start(spec())
    wait(runner, "job-1")
    assert runner.active_count() == 0


def test_unknown_job(tmp_path: Path) -> None:
    assert launcher(tmp_path).status("nope") == JobStatus("unknown")


def test_the_default_working_directory_can_run_the_job_module() -> None:
    # Local runs start RUN_COMMAND from REPO_ROOT, which moves if this module does.
    module = RUN_COMMAND[-1]
    assert (REPO_ROOT / (module.replace(".", "/") + ".py")).is_file()
