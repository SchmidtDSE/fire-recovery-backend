"""The job runner: one record per run, mail on every outcome, no retries."""

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

from src.models.provenance import Provenance
from src.v2 import run as runner
from src.v2.grid import Grid
from src.v2.job_id import CanonicalRequest, fire_id
from src.v2.record import record_key
from src.v2.run import EXIT_BAD_INPUT, EXIT_FAILED, EXIT_OK, RunContext, main, run


class FakeStore:
    def __init__(self) -> None:
        self.objects: Dict[str, Dict[str, Any]] = {}
        self.settings = SimpleNamespace(bucket="test-bucket")

    def put_json(self, key: str, value: Dict[str, Any]) -> None:
        self.objects[key] = json.loads(json.dumps(value))

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        return self.objects.get(key)

    def public_url(self, key: str) -> str:
        return f"https://store.example/{key}"


class FakeMailer:
    def __init__(self, operator: Optional[str] = "ops@example.org", works: bool = True):
        self.operator = operator
        self.works = works
        self.sent: List[Tuple[Optional[str], str, str]] = []

    def send(self, to: Optional[str], subject: str, body: str) -> bool:
        if not to:
            return False
        self.sent.append((to, subject, body))
        return self.works


@pytest.fixture
def stages(
    monkeypatch: pytest.MonkeyPatch, grid: Grid, provenance: Provenance
) -> Dict[str, int]:
    """Replace the pipeline stages with fast fakes that count their calls."""
    calls = {"compute": 0, "publish": 0}

    def compute(canonical: Any, workdir: Path, threads: int) -> Any:
        calls["compute"] += 1
        cogs = {name: workdir / f"{name}.tif" for name in ("dnbr", "rdnbr", "rbr")}
        return SimpleNamespace(grid=grid, cogs=cogs, provenance=provenance)

    def publish(root: Path, fid: str, store: Any) -> str:
        calls["publish"] += 1
        return f"https://store.example/v2/fires/{fid}/catalog.json"

    monkeypatch.setattr(runner, "compute", compute)
    monkeypatch.setattr(runner, "build_sidecar", lambda *args: {"properties": {}})
    monkeypatch.setattr(
        runner,
        "build_catalog",
        lambda workdir, *a, **k: SimpleNamespace(root=workdir, warnings=0),
    )
    monkeypatch.setattr(runner, "publish", publish)
    return calls


@pytest.fixture
def ctx(tmp_path: Path, canonical: CanonicalRequest) -> RunContext:
    job_id = canonical.job_id()
    return RunContext(
        canonical=canonical,
        job_id=job_id,
        fire_id=fire_id(canonical.fire_event_name, job_id),
        notify_email="requester@example.org",
        status_url=f"https://api.example/v2/severity/{job_id}",
        workdir=tmp_path / "work",
        threads=1,
        git_sha="abc1234",
        image="ghcr.io/schmidtdse/fire-recovery-backend@sha256:0",
    )


def test_success_writes_one_record_and_one_email(
    ctx: RunContext, stages: Dict[str, int]
) -> None:
    store, mailer = FakeStore(), FakeMailer()
    assert run(ctx, store, mailer) == EXIT_OK  # type: ignore[arg-type]

    record = store.objects[record_key(ctx.job_id)]
    assert record["state"] == "succeeded"
    assert record["catalog_url"].endswith(f"{ctx.fire_id}/catalog.json")
    assert set(record["assets"]) == {"dnbr", "rdnbr", "rbr", "thumbnail"}
    assert record["assets"]["rbr"].endswith(
        f"{ctx.fire_id}/{ctx.fire_id}/severity/rbr.tif"
    )
    assert record["scene_counts"] == {"prefire": 2, "postfire": 1}
    assert record["provider"] == "Element 84"
    assert record["git_sha"] == "abc1234"
    assert record["peak_rss_mib"] > 0
    assert "requester@example.org" not in json.dumps(record)

    assert [(to, subject) for to, subject, _ in mailer.sent] == [
        ("requester@example.org", "Fire severity ready: Geology_Fire")
    ]
    assert ctx.status_url in mailer.sent[0][2]
    assert not ctx.workdir.exists()


def test_failure_is_recorded_reported_and_not_retried(
    ctx: RunContext, stages: Dict[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*args: Any) -> Any:
        stages["compute"] += 1
        raise RuntimeError(
            "No STAC provider for sensor 'sentinel-2' has scenes in every window"
        )

    monkeypatch.setattr(runner, "compute", broken)
    store, mailer = FakeStore(), FakeMailer()
    assert run(ctx, store, mailer) == EXIT_FAILED  # type: ignore[arg-type]

    assert stages["compute"] == 1
    assert list(store.objects) == [record_key(ctx.job_id)]
    record = store.objects[record_key(ctx.job_id)]
    assert record["state"] == "failed"
    assert record["error"].startswith("RuntimeError: No STAC provider")
    assert record["catalog_url"] is None

    (to_requester, subject, body), (to_operator, op_subject, op_body) = mailer.sent
    assert (to_requester, subject) == (
        "requester@example.org",
        "Fire severity failed: Geology_Fire",
    )
    assert "Traceback" not in body
    assert to_operator == "ops@example.org"
    assert ctx.job_id in op_subject
    assert "Traceback" in op_body and ctx.canonical.to_json() in op_body
    assert not ctx.workdir.exists()


def test_mail_failure_does_not_change_the_outcome(
    ctx: RunContext, stages: Dict[str, int]
) -> None:
    store, mailer = FakeStore(), FakeMailer(works=False)
    assert run(ctx, store, mailer) == EXIT_OK  # type: ignore[arg-type]
    assert store.objects[record_key(ctx.job_id)]["state"] == "succeeded"


def test_rerun_after_failure_overwrites_the_record(
    ctx: RunContext, stages: Dict[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    store = FakeStore()
    real_compute = runner.compute
    monkeypatch.setattr(
        runner, "compute", lambda *a: (_ for _ in ()).throw(ValueError("bad"))
    )
    assert run(ctx, store, FakeMailer()) == EXIT_FAILED  # type: ignore[arg-type]
    monkeypatch.setattr(runner, "compute", real_compute)
    assert run(ctx, store, FakeMailer()) == EXIT_OK  # type: ignore[arg-type]
    assert store.objects[record_key(ctx.job_id)]["state"] == "succeeded"


def test_no_requester_mail_without_an_address(
    ctx: RunContext, stages: Dict[str, int]
) -> None:
    quiet = RunContext(**{**ctx.__dict__, "notify_email": None})
    mailer = FakeMailer()
    run(quiet, FakeStore(), mailer)  # type: ignore[arg-type]
    assert mailer.sent == []


def test_main_aborts_on_a_job_id_mismatch(
    monkeypatch: pytest.MonkeyPatch, canonical: CanonicalRequest
) -> None:
    monkeypatch.setenv("SEVERITY_REQUEST", canonical.to_json())
    monkeypatch.setenv("JOB_ID", "00000000-0000-5000-8000-000000000000")
    store, mailer = FakeStore(), FakeMailer()
    code = main([], store_factory=lambda: store, mailer_factory=lambda: mailer)  # type: ignore[arg-type,return-value]
    assert code == EXIT_BAD_INPUT
    assert store.objects == {} and mailer.sent == []


def test_main_runs_the_request_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    canonical: CanonicalRequest,
    stages: Dict[str, int],
) -> None:
    monkeypatch.setenv("SEVERITY_REQUEST", canonical.to_json())
    monkeypatch.setenv("JOB_ID", canonical.job_id())
    monkeypatch.setenv("SEVERITY_WORKDIR", str(tmp_path))
    monkeypatch.delenv("NOTIFY_EMAIL", raising=False)
    store = FakeStore()
    code = main([], store_factory=lambda: store, mailer_factory=FakeMailer)  # type: ignore[arg-type,return-value]
    assert code == EXIT_OK
    assert (
        store.objects[record_key(canonical.job_id())]["fire_event_name"]
        == "Geology_Fire"
    )


def test_main_needs_a_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SEVERITY_REQUEST", raising=False)
    assert (
        main([], store_factory=FakeStore, mailer_factory=FakeMailer) == EXIT_BAD_INPUT
    )  # type: ignore[arg-type]
