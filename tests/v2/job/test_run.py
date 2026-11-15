"""The job runner: one record per run, mail on every outcome, no retries."""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import pytest

from src.models.provenance import Provenance
from src.v2.job import run as runner
from src.v2.catalog.build import item_directory
from src.v2.identity.grid import Grid
from src.v2.catalog.perimeter import Perimeter
from src.v2.identity.job_id import CanonicalRequest, fire_id
from src.v2.record import record_key, request_key
from src.v2.job.run import EXIT_BAD_INPUT, EXIT_FAILED, EXIT_OK, RunContext, main, run


class FakeStore:
    def __init__(self) -> None:
        self.objects: Dict[str, Dict[str, Any]] = {}
        self.settings = SimpleNamespace(bucket="test-bucket")

    def put_json(self, key: str, value: Dict[str, Any]) -> None:
        self.objects[key] = json.loads(json.dumps(value))

    def put_text(self, key: str, text: str) -> None:
        self.objects[key] = json.loads(text)

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        return self.objects.get(key)

    def public_url(self, key: str) -> str:
        return f"https://store.example/{key}"


class FakeMailer:
    def __init__(self, operator: Optional[str] = "ops@example.org", works: bool = True):
        self.operator = operator
        self.works = works
        self.sent: List[Tuple[Optional[str], str, str]] = []
        self.images: List[Optional[Path]] = []

    def send(
        self, to: Optional[str], subject: str, body: str, image: Optional[Path] = None
    ) -> bool:
        if not to:
            return False
        self.sent.append((to, subject, body))
        self.images.append(image)
        return self.works


@pytest.fixture
def stages(
    monkeypatch: pytest.MonkeyPatch, grid: Grid, provenance: Provenance
) -> Dict[str, Any]:
    """Replace the pipeline stages with fast fakes that count their calls."""
    calls: Dict[str, Any] = {"compute": 0, "publish": 0, "perimeter": None}

    def compute(canonical: Any, workdir: Path, threads: int) -> Any:
        calls["compute"] += 1
        cogs = {name: workdir / f"{name}.tif" for name in ("dnbr", "rdnbr", "rbr")}
        return SimpleNamespace(grid=grid, cogs=cogs, provenance=provenance)

    def publish(root: Path, fid: str, store: Any) -> str:
        calls["publish"] += 1
        return f"https://store.example/v2/fires/{fid}/catalog.json"

    monkeypatch.setattr(runner, "compute", compute)
    monkeypatch.setattr(
        runner, "build_sidecar", lambda *args, **kwargs: {"properties": {}}
    )

    def build_catalog(workdir: Path, *args: Any, **kwargs: Any) -> Any:
        calls["perimeter"] = kwargs.get("perimeter")
        return SimpleNamespace(root=workdir, warnings=0)

    monkeypatch.setattr(runner, "build_catalog", build_catalog)
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
    ctx: RunContext, stages: Dict[str, Any]
) -> None:
    store, mailer = FakeStore(), FakeMailer()
    assert run(ctx, store, mailer) == EXIT_OK  # type: ignore[arg-type]

    record = store.objects[record_key(ctx.job_id)]
    assert record["state"] == "succeeded"
    assert record["catalog_url"].endswith(f"{ctx.fire_id}/catalog.json")
    assert set(record["assets"]) == {"dnbr", "rdnbr", "rbr", "thumbnail"}
    assert record["assets"]["rbr"].endswith(f"{ctx.fire_id}/{ctx.fire_id}/rbr/rbr.tif")
    assert record["scene_counts"] == {"prefire": 2, "postfire": 1}
    assert record["provider"] == "Element 84"
    assert record["git_sha"] == "abc1234"
    assert record["peak_rss_mib"] > 0
    assert "requester@example.org" not in json.dumps(record)

    assert [(to, subject) for to, subject, _ in mailer.sent] == [
        ("requester@example.org", "[Fire Severity Analysis: Geology_Fire] Ready")
    ]
    assert ctx.status_url in mailer.sent[0][2]
    assert mailer.images == [
        item_directory(ctx.workdir, ctx.fire_id, "rbr") / "rbr.thumb.jpg"
    ]
    assert not ctx.workdir.exists()


def test_a_perimeter_is_built_and_linked_in_the_record(
    ctx: RunContext, stages: Dict[str, Any]
) -> None:
    ring = [(-116098276, 33880794), (-116019318, 33880794),
            (-116019318, 33929925), (-116098276, 33880794)]  # fmt: skip
    with_perimeter = replace(
        ctx, canonical=ctx.canonical.model_copy(update={"perimeter": [[ring]]})
    )
    store = FakeStore()
    assert run(with_perimeter, store, FakeMailer()) == EXIT_OK  # type: ignore[arg-type]

    perimeter = stages["perimeter"]
    assert isinstance(perimeter, Perimeter)
    assert perimeter.fire_event_name == "Geology_Fire"
    assert perimeter.geometry["coordinates"][0][0][0] == [-116.098276, 33.880794]
    assets = store.objects[record_key(ctx.job_id)]["assets"]
    folder = f"{ctx.fire_id}/{ctx.fire_id}-perimeter"
    assert assets["perimeter"].endswith(f"{folder}/perimeter.parquet")
    assert assets["perimeter-tiles"].endswith(f"{folder}/perimeter.pmtiles")


def test_without_a_perimeter_none_is_built(
    ctx: RunContext, stages: Dict[str, Any]
) -> None:
    store = FakeStore()
    assert run(ctx, store, FakeMailer()) == EXIT_OK  # type: ignore[arg-type]
    assert stages["perimeter"] is None
    assert "perimeter" not in store.objects[record_key(ctx.job_id)]["assets"]


def test_failure_is_recorded_reported_and_not_retried(
    ctx: RunContext, stages: Dict[str, Any], monkeypatch: pytest.MonkeyPatch
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
        "[Fire Severity Analysis: Geology_Fire] Failed",
    )
    assert "Traceback" not in body
    assert to_operator == "ops@example.org"
    assert (
        op_subject == f"[Fire Severity Analysis: Geology_Fire] Failed: job {ctx.job_id}"
    )
    assert "Traceback" in op_body and ctx.canonical.to_json() in op_body
    assert not ctx.workdir.exists()


def test_mail_failure_does_not_change_the_outcome(
    ctx: RunContext, stages: Dict[str, Any]
) -> None:
    store, mailer = FakeStore(), FakeMailer(works=False)
    assert run(ctx, store, mailer) == EXIT_OK  # type: ignore[arg-type]
    assert store.objects[record_key(ctx.job_id)]["state"] == "succeeded"


def test_rerun_after_failure_overwrites_the_record(
    ctx: RunContext, stages: Dict[str, Any], monkeypatch: pytest.MonkeyPatch
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
    ctx: RunContext, stages: Dict[str, Any]
) -> None:
    quiet = RunContext(**{**ctx.__dict__, "notify_email": None})
    mailer = FakeMailer()
    run(quiet, FakeStore(), mailer)  # type: ignore[arg-type]
    assert mailer.sent == []


def test_main_aborts_on_a_job_id_mismatch(
    monkeypatch: pytest.MonkeyPatch, canonical: CanonicalRequest
) -> None:
    other = "00000000-0000-5000-8000-000000000000"
    monkeypatch.setenv("JOB_ID", other)
    store, mailer = FakeStore(), FakeMailer()
    store.put_text(request_key(other), canonical.to_json())
    code = main([], store_factory=lambda: store, mailer_factory=lambda: mailer)  # type: ignore[arg-type,return-value]
    assert code == EXIT_BAD_INPUT
    assert set(store.objects) == {request_key(other)} and mailer.sent == []


def test_main_runs_the_request_stored_under_its_job_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    canonical: CanonicalRequest,
    stages: Dict[str, Any],
) -> None:
    monkeypatch.setenv("JOB_ID", canonical.job_id())
    monkeypatch.setenv("SEVERITY_WORKDIR", str(tmp_path))
    monkeypatch.delenv("NOTIFY_EMAIL", raising=False)
    store = FakeStore()
    store.put_text(request_key(canonical.job_id()), canonical.to_json())
    code = main([], store_factory=lambda: store, mailer_factory=FakeMailer)  # type: ignore[arg-type,return-value]
    assert code == EXIT_OK
    assert (
        store.objects[record_key(canonical.job_id())]["fire_event_name"]
        == "Geology_Fire"
    )


def test_main_needs_a_request(
    monkeypatch: pytest.MonkeyPatch, canonical: CanonicalRequest
) -> None:
    def code() -> int:
        return main([], store_factory=FakeStore, mailer_factory=FakeMailer)  # type: ignore[arg-type]

    monkeypatch.delenv("JOB_ID", raising=False)
    assert code() == EXIT_BAD_INPUT
    # A job id with nothing stored under it.
    monkeypatch.setenv("JOB_ID", canonical.job_id())
    assert code() == EXIT_BAD_INPUT


def test_a_broken_mail_template_never_changes_the_outcome(
    ctx: RunContext, stages: Dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    def broken(*args: Any, **kwargs: Any) -> str:
        raise RuntimeError("template error")

    monkeypatch.setattr(runner, "render", broken)
    store, mailer = FakeStore(), FakeMailer()
    assert run(ctx, store, mailer) == EXIT_OK  # type: ignore[arg-type]
    assert store.objects[record_key(ctx.job_id)]["state"] == "succeeded"
    assert mailer.sent == []
