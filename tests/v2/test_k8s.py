"""Target profiles, the Job and Secret specs, status mapping, and the launcher."""

import base64
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import google.auth
import pytest
from google.auth.exceptions import DefaultCredentialsError
from kubernetes.client import V1Job, V1JobCondition, V1JobStatus
from kubernetes.client.exceptions import ApiException

from src.v2.k8s import (
    GkeCluster,
    KubernetesLauncher,
    TargetProfile,
    gke_api_client,
    job_manifest,
    job_name,
    job_status,
    secret_manifest,
    secret_name,
)
from src.v2.launcher import DispatchUnavailable, JobSpec, JobStatus

JOB_ID = "bbf1eb5e-fbf0-527a-a51a-b5575941fbb0"
IMAGE = "ghcr.io/schmidtdse/fire-recovery-backend/severity-batch@sha256:abc"
CI_TARGETS = Path(__file__).resolve().parents[1] / "fixtures" / "targets"
API_ENV = {
    "S3_ENDPOINT": "storage.googleapis.com",
    "S3_BUCKET": "dse-nps-fire-severity-storage",
    "S3_ACCESS_KEY_ID": "key",
    "S3_SECRET_ACCESS_KEY": "secret",
    "SMTP_USER": "sender@example.org",
    "UNRELATED": "never copied",
}


def spec(email: Optional[str] = None) -> JobSpec:
    return JobSpec(
        job_id=JOB_ID,
        canonical_json='{"operation":"fire_severity_v2"}',
        status_url=f"https://api/v2/severity/{JOB_ID}",
        notify_email=email,
    )


@pytest.mark.parametrize("name", ["gke-josh", "nrp"])
def test_shipped_profiles_load_with_requests_equal_to_limits(name: str) -> None:
    profile = TargetProfile.load(name)
    assert profile.namespace == "fire-severity-dev"
    assert profile.image == (
        "ghcr.io/schmidtdse/fire-recovery-backend/severity-batch:latest"
    )
    assert profile.resources["requests"] == profile.resources["limits"]
    assert profile.timeout_seconds == 6 * 3600


def test_gke_profile_names_joshs_cluster_and_spot() -> None:
    profile = TargetProfile.load("gke-josh")
    assert profile.context == "gke_dse-nps_us-west1_josh-k8s-gke"
    assert profile.gke == GkeCluster("dse-nps", "us-west1", "josh-k8s-gke")
    assert profile.spot
    assert TargetProfile.load("nrp").gke is None


class FakeCredentials:
    """Application Default Credentials that hand out numbered tokens."""

    def __init__(self) -> None:
        self.valid = False
        self.token: Optional[str] = None
        self.refreshes = 0

    def refresh(self, request: Any) -> None:
        self.refreshes += 1
        self.token = f"token-{self.refreshes}"
        self.valid = True


class FakeGkeApi:
    def __init__(self) -> None:
        self.urls: List[str] = []

    def get(self, url: str, timeout: float) -> Any:
        self.urls.append(url)
        cluster = {
            "endpoint": "34.1.2.3",
            "masterAuth": {
                "clusterCaCertificate": base64.b64encode(b"--CA--").decode()
            },
        }
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: cluster)


def test_gke_client_uses_the_callers_identity_and_refreshes_its_token() -> None:
    credentials, api = FakeCredentials(), FakeGkeApi()
    cluster = GkeCluster("dse-nps", "us-west1", "josh-k8s-gke")
    config = gke_api_client(cluster, credentials, api).configuration
    assert api.urls == [
        "https://container.googleapis.com/v1/projects/dse-nps/locations/us-west1"
        "/clusters/josh-k8s-gke"
    ]
    assert config.host == "https://34.1.2.3"
    assert Path(config.ssl_ca_cert).read_bytes() == b"--CA--"
    assert config.get_api_key_with_prefix("authorization") == "Bearer token-1"
    assert config.get_api_key_with_prefix("authorization") == "Bearer token-1"
    credentials.valid = False  # expired
    assert config.get_api_key_with_prefix("authorization") == "Bearer token-2"


def test_gke_target_without_google_credentials_is_dispatch_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_credentials(scopes: List[str]) -> Any:
        raise DefaultCredentialsError("no ADC")

    monkeypatch.setattr(google.auth, "default", no_credentials)
    with pytest.raises(DispatchUnavailable, match="Google identity"):
        KubernetesLauncher(TargetProfile.load("gke-josh"), IMAGE).status(JOB_ID)


def test_ci_profiles_load_from_the_targets_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEVERITY_TARGETS_DIR", str(CI_TARGETS))
    profile = TargetProfile.load("ci-kind")
    assert profile.context == "kind-dispatcher"
    assert profile.pod_s3_endpoint == "rustfs.fire-severity-dev.svc.cluster.local:9000"
    assert TargetProfile.load("ci-kind-bad-image").timeout_seconds == 60


def test_invalid_profiles_are_rejected(tmp_path: Path) -> None:
    (tmp_path / "http.json").write_text(json.dumps({"type": "http"}))
    (tmp_path / "bare.json").write_text(
        json.dumps({"type": "kubernetes", "kubernetes": {"context": "x"}})
    )
    with pytest.raises(ValueError, match="not a Kubernetes profile"):
        TargetProfile.load("http", tmp_path)
    with pytest.raises(ValueError, match="no namespace"):
        TargetProfile.load("bare", tmp_path)
    with pytest.raises(ValueError, match="Invalid target name"):
        TargetProfile.load("../gke-josh", tmp_path)
    with pytest.raises(FileNotFoundError):
        TargetProfile.load("missing", tmp_path)


def test_job_manifest() -> None:
    profile = TargetProfile.load("gke-josh")
    manifest = job_manifest(spec(email="someone@example.org"), profile, IMAGE)
    assert manifest["metadata"]["name"] == f"sev-{JOB_ID}"
    assert manifest["metadata"]["labels"] == {
        "app": "severity-v2",
        "severity-v2/job-id": JOB_ID,
    }
    job = manifest["spec"]
    assert job["backoffLimit"] == 0
    assert job["podFailurePolicy"] == {
        "rules": [
            {"action": "Ignore", "onPodConditions": [{"type": "DisruptionTarget"}]}
        ]
    }
    assert job["activeDeadlineSeconds"] == 21600
    assert job["ttlSecondsAfterFinished"] == 86400
    pod = job["template"]["spec"]
    assert pod["restartPolicy"] == "Never"
    assert pod["automountServiceAccountToken"] is False
    assert pod["nodeSelector"] == {"cloud.google.com/gke-spot": "true"}
    assert pod["tolerations"] == [
        {
            "key": "cloud.google.com/gke-spot",
            "operator": "Equal",
            "value": "true",
            "effect": "NoSchedule",
        }
    ]
    (container,) = pod["containers"]
    assert container["image"] == IMAGE
    assert container["args"] == ["python", "-m", "src.v2.run"]
    env = {item["name"]: item["value"] for item in container["env"]}
    assert env["SEVERITY_REQUEST"] == '{"operation":"fire_severity_v2"}'
    assert env["JOB_ID"] == JOB_ID
    assert env["SEVERITY_IMAGE"] == IMAGE
    assert env["SEVERITY_WORKDIR"] == "/scratch"
    # The address is in the Job's Secret, never in its spec.
    assert "NOTIFY_EMAIL" not in env
    assert "someone@example.org" not in json.dumps(manifest)
    assert container["envFrom"] == [{"secretRef": {"name": f"sev-creds-{JOB_ID}"}}]
    assert container["resources"]["limits"]["memory"] == "2Gi"
    assert pod["volumes"] == [{"name": "scratch", "emptyDir": {"sizeLimit": "10Gi"}}]


def test_job_manifest_off_spot_has_no_scheduling_constraints() -> None:
    pod = job_manifest(spec(), TargetProfile.load("nrp"), IMAGE)["spec"]["template"]
    assert "nodeSelector" not in pod["spec"]
    assert "tolerations" not in pod["spec"]


def test_secret_carries_the_apis_credentials_and_the_address() -> None:
    profile = TargetProfile.load("gke-josh")
    secret = secret_manifest(spec(email="someone@example.org"), profile, API_ENV, "u1")
    assert secret["metadata"]["name"] == f"sev-creds-{JOB_ID}"
    assert secret["metadata"]["ownerReferences"] == [
        {"apiVersion": "batch/v1", "kind": "Job", "name": job_name(JOB_ID), "uid": "u1"}
    ]
    assert secret["stringData"] == {
        "S3_ENDPOINT": "storage.googleapis.com",
        "S3_BUCKET": "dse-nps-fire-severity-storage",
        "S3_ACCESS_KEY_ID": "key",
        "S3_SECRET_ACCESS_KEY": "secret",
        "SMTP_USER": "sender@example.org",
        "NOTIFY_EMAIL": "someone@example.org",
    }


def test_secret_points_pods_at_the_profiles_storage_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEVERITY_TARGETS_DIR", str(CI_TARGETS))
    profile = TargetProfile.load("ci-kind")
    data = secret_manifest(spec(), profile, API_ENV, "u1")["stringData"]
    assert data["S3_ENDPOINT"] == "rustfs.fire-severity-dev.svc.cluster.local:9000"
    assert "NOTIFY_EMAIL" not in data


def v1job(
    conditions: Optional[List[V1JobCondition]] = None, active: int = 0, ready: int = 0
) -> V1Job:
    return V1Job(status=V1JobStatus(conditions=conditions, active=active, ready=ready))


@pytest.mark.parametrize(
    "job, expected",
    [
        (V1Job(status=None), JobStatus("pending")),
        (v1job(active=1, ready=0), JobStatus("pending")),
        (v1job(active=1, ready=1), JobStatus("running")),
        (
            v1job([V1JobCondition(type="Complete", status="True")]),
            JobStatus("succeeded"),
        ),
        (
            v1job(
                [
                    V1JobCondition(
                        type="Failed",
                        status="True",
                        reason="DeadlineExceeded",
                        message="Job was active longer than specified deadline",
                    )
                ]
            ),
            JobStatus(
                "failed",
                "DeadlineExceeded: Job was active longer than specified deadline",
            ),
        ),
        (
            v1job([V1JobCondition(type="Failed", status="False")], active=1, ready=1),
            JobStatus("running"),
        ),
    ],
)
def test_job_status(job: V1Job, expected: JobStatus) -> None:
    assert job_status(job) == expected


class FakeBatch:
    def __init__(self) -> None:
        self.jobs: Dict[str, V1Job] = {}
        self.created: List[Dict[str, Any]] = []
        self.deleted: List[str] = []

    def create_namespaced_job(self, namespace: str, body: Dict[str, Any]) -> Any:
        name = body["metadata"]["name"]
        if name in self.jobs:
            raise ApiException(status=409, reason="AlreadyExists")
        self.created.append(body)
        self.jobs[name] = v1job(active=1)
        return SimpleNamespace(metadata=SimpleNamespace(uid=f"uid-{len(self.created)}"))

    def read_namespaced_job(self, name: str, namespace: str) -> V1Job:
        if name not in self.jobs:
            raise ApiException(status=404, reason="NotFound")
        return self.jobs[name]

    def delete_namespaced_job(
        self, name: str, namespace: str, propagation_policy: str
    ) -> None:
        assert propagation_policy == "Background"  # dependents go too
        if name not in self.jobs:
            raise ApiException(status=404, reason="NotFound")
        self.deleted.append(name)
        del self.jobs[name]

    def list_namespaced_job(self, namespace: str, label_selector: str) -> Any:
        assert label_selector == "app=severity-v2"
        return SimpleNamespace(items=list(self.jobs.values()))


class FakeCore:
    def __init__(self, reason: Optional[str] = None, fail_secrets: bool = False):
        self.reason = reason
        self.fail_secrets = fail_secrets
        self.secrets: Dict[str, Dict[str, Any]] = {}
        self.replaced: List[str] = []

    def create_namespaced_secret(self, namespace: str, body: Dict[str, Any]) -> None:
        if self.fail_secrets:
            raise ApiException(status=403, reason="Forbidden")
        name = body["metadata"]["name"]
        if name in self.secrets:
            raise ApiException(status=409, reason="AlreadyExists")
        self.secrets[name] = body

    def replace_namespaced_secret(
        self, name: str, namespace: str, body: Dict[str, Any]
    ) -> None:
        self.replaced.append(name)
        self.secrets[name] = body

    def list_namespaced_pod(self, namespace: str, label_selector: str) -> Any:
        terminated = SimpleNamespace(reason=self.reason)
        state = SimpleNamespace(terminated=terminated, waiting=None)
        container = SimpleNamespace(state=state)
        return SimpleNamespace(
            items=[
                SimpleNamespace(status=SimpleNamespace(container_statuses=[container]))
            ]
        )


def launcher(core: FakeCore, batch: Optional[FakeBatch] = None) -> KubernetesLauncher:
    return KubernetesLauncher(
        TargetProfile.load("gke-josh"), IMAGE, batch or FakeBatch(), core, API_ENV
    )


def test_kubernetes_launcher() -> None:
    batch, core = FakeBatch(), FakeCore("OOMKilled")
    jobs = launcher(core, batch)
    assert jobs.status(JOB_ID) == JobStatus("unknown")
    assert jobs.start(spec(email="someone@example.org"))
    assert batch.created[0]["metadata"]["name"] == job_name(JOB_ID)
    secret = core.secrets[secret_name(JOB_ID)]
    assert secret["metadata"]["ownerReferences"][0]["uid"] == "uid-1"
    assert secret["stringData"]["NOTIFY_EMAIL"] == "someone@example.org"
    assert not jobs.start(spec())  # 409: already exists, its Secret untouched
    assert core.replaced == []
    assert jobs.status(JOB_ID) == JobStatus("pending")
    assert jobs.active_count() == 1

    batch.jobs[job_name(JOB_ID)] = v1job(
        [V1JobCondition(type="Failed", status="True", reason="BackoffLimitExceeded")]
    )
    status = jobs.status(JOB_ID)
    assert status.phase == "failed"
    assert status.reason == "BackoffLimitExceeded (pod: OOMKilled)"
    assert jobs.active_count() == 0

    jobs.delete(JOB_ID)
    jobs.delete(JOB_ID)  # already gone: fine
    assert batch.deleted == [job_name(JOB_ID)]


def test_a_restart_takes_over_the_previous_jobs_secret() -> None:
    batch, core = FakeBatch(), FakeCore()
    jobs = launcher(core, batch)
    assert jobs.start(spec())
    jobs.delete(JOB_ID)  # the old Secret is collected later, not at once
    assert jobs.start(spec())
    assert core.replaced == [secret_name(JOB_ID)]
    owner = core.secrets[secret_name(JOB_ID)]["metadata"]["ownerReferences"][0]
    assert owner["uid"] == "uid-2"


def test_a_job_without_its_secret_is_removed() -> None:
    batch = FakeBatch()
    with pytest.raises(DispatchUnavailable, match="credentials"):
        launcher(FakeCore(fail_secrets=True), batch).start(spec())
    assert batch.deleted == [job_name(JOB_ID)]
    assert batch.jobs == {}


def test_missing_context_or_image_is_dispatch_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    kubeconfig = tmp_path / "config"
    kubeconfig.write_text(
        json.dumps(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [{"name": "c", "cluster": {"server": "https://x"}}],
                "users": [{"name": "u", "user": {"token": "t"}}],
                "contexts": [
                    {"name": "other", "context": {"cluster": "c", "user": "u"}}
                ],
                "current-context": "other",
            }
        )
    )
    monkeypatch.setenv("KUBECONFIG", str(kubeconfig))
    with pytest.raises(DispatchUnavailable, match="kubeconfig context 'nautilus'"):
        KubernetesLauncher(TargetProfile.load("nrp"), IMAGE).status(JOB_ID)
    with pytest.raises(DispatchUnavailable, match="SEVERITY_IMAGE"):
        KubernetesLauncher(
            TargetProfile.load("gke-josh"), None, FakeBatch(), FakeCore()
        ).start(spec())
