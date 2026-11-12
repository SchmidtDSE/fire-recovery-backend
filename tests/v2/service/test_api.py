"""POST and GET /v2/severity, with a fake store and launcher."""

import copy
import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import pytest
from fastapi.testclient import TestClient

from src.app import app
from src.v2.service import api
from src.v2.service.access import RateLimiter, hash_key
from src.v2.identity.job_id import canonical_request
from src.v2.service.launcher import DispatchUnavailable, JobSpec, JobStatus
from src.v2.record import record_key
from src.v2.identity.request import SeverityRequest

VECTORS = Path(__file__).parents[2] / "fixtures" / "v2_job_id_vectors.json"
BODY: Dict[str, Any] = json.loads(VECTORS.read_text())["vectors"][0]["request_body"]
JOB_ID = canonical_request(SeverityRequest.model_validate(BODY)).job_id()


class FakeStore:
    def __init__(self) -> None:
        self.objects: Dict[str, Dict[str, Any]] = {}

    def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        return self.objects.get(key)


class FakeLauncher:
    def __init__(self) -> None:
        self.statuses: Dict[str, JobStatus] = {}
        self.started: List[JobSpec] = []
        self.deleted: List[str] = []
        self.active = 0
        self.unavailable = False

    def start(self, spec: JobSpec) -> bool:
        if self.unavailable:
            raise DispatchUnavailable("no cluster")
        self.started.append(spec)
        self.statuses[spec.job_id] = JobStatus("pending")
        return True

    def status(self, job_id: str) -> JobStatus:
        if self.unavailable:
            raise DispatchUnavailable("no cluster")
        return self.statuses.get(job_id, JobStatus("unknown"))

    def delete(self, job_id: str) -> None:
        self.deleted.append(job_id)
        self.statuses.pop(job_id, None)

    def active_count(self) -> int:
        return self.active


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def launcher() -> FakeLauncher:
    return FakeLauncher()


@pytest.fixture
def client(store: FakeStore, launcher: FakeLauncher) -> Iterator[TestClient]:
    limiter = RateLimiter(limit=2, window_seconds=3600)
    app.dependency_overrides[api.get_store] = lambda: store
    app.dependency_overrides[api.get_launcher] = lambda: launcher
    app.dependency_overrides[api.get_rate_limiter] = lambda: limiter
    app.dependency_overrides[api.get_api_key_hashes] = lambda: {hash_key("good-key")}
    yield TestClient(app)
    app.dependency_overrides.clear()


SUCCEEDED = {
    "job_id": JOB_ID,
    "state": "succeeded",
    "fire_event_name": "Geology_Fire",
    "catalog_url": "https://store/v2/fires/geology-fire-bbf1eb5e/catalog.json",
    "assets": {"rbr": "https://store/v2/fires/x/rbr.tif"},
    "scene_counts": {"prefire": 2, "postfire": 1},
    "finished_at": "2026-11-10T00:00:00Z",
    "pipeline_version": 1,
}


def test_new_request_starts_a_job(client: TestClient, launcher: FakeLauncher) -> None:
    response = client.post(
        "/v2/severity", json={**BODY, "email": "someone@example.org"}
    )
    assert response.status_code == 202
    body = response.json()
    assert body["job_id"] == JOB_ID
    assert body["state"] == "pending"
    assert body["status_url"] == f"http://testserver/v2/severity/{JOB_ID}"
    assert body["grid"]["epsg"] == 32611
    assert body["grid"]["tiles"]["tms"] == "WebMercatorQuad"
    (spec,) = launcher.started
    assert spec.job_id == JOB_ID
    assert json.loads(spec.canonical_json)["fire_event_name"] == "Geology_Fire"
    assert spec.notify_email == "someone@example.org"


def test_status_url_can_use_the_public_base(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SEVERITY_API_URL", "https://api.example.org/")
    body = client.post("/v2/severity", json=BODY).json()
    assert body["status_url"] == f"https://api.example.org/v2/severity/{JOB_ID}"


def test_cache_hit_returns_links_and_starts_nothing(
    client: TestClient, store: FakeStore, launcher: FakeLauncher
) -> None:
    store.objects[record_key(JOB_ID)] = SUCCEEDED
    response = client.post("/v2/severity", json=BODY)
    assert response.status_code == 200
    assert response.json()["catalog_url"] == SUCCEEDED["catalog_url"]
    assert response.json()["assets"] == SUCCEEDED["assets"]
    assert launcher.started == []


def test_a_small_shift_hits_the_same_analysis(
    client: TestClient, store: FakeStore, launcher: FakeLauncher
) -> None:
    store.objects[record_key(JOB_ID)] = SUCCEEDED
    shifted = copy.deepcopy(BODY)
    shifted["geometry"]["coordinates"][0] = [
        [lon + 0.00005, lat - 0.00004]
        for lon, lat in BODY["geometry"]["coordinates"][0]
    ]
    assert client.post("/v2/severity", json=shifted).status_code == 200


def test_joining_a_running_job(client: TestClient, launcher: FakeLauncher) -> None:
    launcher.statuses[JOB_ID] = JobStatus("running")
    response = client.post("/v2/severity", json=BODY)
    assert response.status_code == 202
    assert response.json()["state"] == "running"
    assert launcher.started == []


def test_a_failed_run_is_restarted(
    client: TestClient, store: FakeStore, launcher: FakeLauncher
) -> None:
    store.objects[record_key(JOB_ID)] = {
        **SUCCEEDED,
        "state": "failed",
        "error": "boom",
    }
    launcher.statuses[JOB_ID] = JobStatus("failed", "BackoffLimitExceeded")
    response = client.post("/v2/severity", json=BODY)
    assert response.status_code == 202
    assert launcher.deleted == [JOB_ID]
    assert len(launcher.started) == 1


def test_capacity_limit(client: TestClient, launcher: FakeLauncher) -> None:
    launcher.active = 4
    response = client.post("/v2/severity", json=BODY)
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "60"
    assert launcher.started == []


def test_per_ip_rate_limit_and_api_keys(
    client: TestClient, launcher: FakeLauncher
) -> None:
    for name in ("A", "B"):
        assert (
            client.post(
                "/v2/severity", json={**BODY, "fire_event_name": name}
            ).status_code
            == 202
        )
    limited = client.post("/v2/severity", json={**BODY, "fire_event_name": "C"})
    assert limited.status_code == 429
    assert int(limited.headers["Retry-After"]) > 0
    assert "API key" in limited.json()["detail"]
    keyed = client.post(
        "/v2/severity",
        json={**BODY, "fire_event_name": "C"},
        headers={"X-API-Key": "good-key"},
    )
    assert keyed.status_code == 202
    wrong = client.post(
        "/v2/severity",
        json={**BODY, "fire_event_name": "D"},
        headers={"X-API-Key": "bad"},
    )
    assert wrong.status_code == 429


@pytest.mark.parametrize(
    "changes",
    [
        {"fire_event_name": "Geology Fire"},
        {"prefire_window": ["2023-01-01", "2023-06-09"]},
        {
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.0, 37.0],
                        [-119.0, 37.0],
                        [-119.0, 39.0],
                        [-122.0, 39.0],
                        [-122.0, 37.0],
                    ]
                ],
            }
        },
    ],
    ids=["name", "window", "oversize"],
)
def test_invalid_requests(
    client: TestClient, launcher: FakeLauncher, changes: Dict[str, Any]
) -> None:
    assert client.post("/v2/severity", json={**BODY, **changes}).status_code == 422
    assert launcher.started == []


def test_dispatch_unavailable(client: TestClient, launcher: FakeLauncher) -> None:
    launcher.unavailable = True
    assert client.post("/v2/severity", json=BODY).status_code == 503


def test_status_of_a_succeeded_analysis(client: TestClient, store: FakeStore) -> None:
    store.objects[record_key(JOB_ID)] = SUCCEEDED
    body = client.get(f"/v2/severity/{JOB_ID}").json()
    assert body["state"] == "succeeded"
    assert body["catalog_url"] == SUCCEEDED["catalog_url"]
    assert body["scene_counts"] == {"prefire": 2, "postfire": 1}


def test_status_of_a_running_job_ignores_an_old_failure(
    client: TestClient, store: FakeStore, launcher: FakeLauncher
) -> None:
    store.objects[record_key(JOB_ID)] = {**SUCCEEDED, "state": "failed", "error": "old"}
    launcher.statuses[JOB_ID] = JobStatus("running")
    assert client.get(f"/v2/severity/{JOB_ID}").json()["state"] == "running"


def test_status_of_a_failed_analysis(
    client: TestClient, store: FakeStore, launcher: FakeLauncher
) -> None:
    store.objects[record_key(JOB_ID)] = {
        **SUCCEEDED,
        "state": "failed",
        "error": "RuntimeError: x",
    }
    body = client.get(f"/v2/severity/{JOB_ID}").json()
    assert (body["state"], body["error"]) == ("failed", "RuntimeError: x")


def test_status_of_a_platform_failure(
    client: TestClient, launcher: FakeLauncher
) -> None:
    launcher.statuses[JOB_ID] = JobStatus(
        "failed", "BackoffLimitExceeded (pod: OOMKilled)"
    )
    body = client.get(f"/v2/severity/{JOB_ID}").json()
    assert (body["state"], body["error"]) == (
        "failed",
        "BackoffLimitExceeded (pod: OOMKilled)",
    )


def test_status_unknown_and_invalid_ids(client: TestClient) -> None:
    assert client.get(f"/v2/severity/{JOB_ID}").status_code == 404
    assert client.get("/v2/severity/not-a-uuid").status_code == 422
