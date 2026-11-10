"""The Job spec, status mapping and Kubernetes launcher, against a fake API."""

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest
from kubernetes.client import V1Job, V1JobCondition, V1JobStatus
from kubernetes.client.exceptions import ApiException

from src.v2.k8s import (
    KubernetesLauncher,
    TargetProfile,
    job_manifest,
    job_name,
    job_status,
)
from src.v2.launcher import DispatchUnavailable, JobSpec, JobStatus

JOB_ID = "bbf1eb5e-fbf0-527a-a51a-b5575941fbb0"
IMAGE = "ghcr.io/schmidtdse/fire-recovery-backend@sha256:abc"


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
    assert profile.secret_name == "severity-v2"
    assert profile.resources["requests"] == profile.resources["limits"]
    assert profile.active_deadline_seconds == 6 * 3600


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
    assert pod["nodeSelector"] == {"cloud.google.com/gke-spot": "true"}
    (container,) = pod["containers"]
    assert container["image"] == IMAGE
    assert container["args"] == ["python", "-m", "src.v2.run"]
    env = {item["name"]: item["value"] for item in container["env"]}
    assert env["SEVERITY_REQUEST"] == '{"operation":"fire_severity_v2"}'
    assert env["JOB_ID"] == JOB_ID
    assert env["NOTIFY_EMAIL"] == "someone@example.org"
    assert env["SEVERITY_IMAGE"] == IMAGE
    assert env["SEVERITY_WORKDIR"] == "/scratch"
    assert container["envFrom"] == [{"secretRef": {"name": "severity-v2"}}]
    assert container["resources"]["limits"]["memory"] == "2Gi"
    assert pod["volumes"] == [{"name": "scratch", "emptyDir": {"sizeLimit": "10Gi"}}]


def test_job_manifest_without_email_has_no_address() -> None:
    manifest = job_manifest(spec(), TargetProfile.load("nrp"), IMAGE)
    names = {
        item["name"]
        for item in manifest["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert "NOTIFY_EMAIL" not in names
    assert "nodeSelector" not in manifest["spec"]["template"]["spec"]


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

    def create_namespaced_job(self, namespace: str, body: Dict[str, Any]) -> None:
        name = body["metadata"]["name"]
        if name in self.jobs:
            raise ApiException(status=409, reason="AlreadyExists")
        self.created.append(body)
        self.jobs[name] = v1job(active=1)

    def read_namespaced_job_status(self, name: str, namespace: str) -> V1Job:
        if name not in self.jobs:
            raise ApiException(status=404, reason="NotFound")
        return self.jobs[name]

    def delete_namespaced_job(
        self, name: str, namespace: str, propagation_policy: str
    ) -> None:
        if name not in self.jobs:
            raise ApiException(status=404, reason="NotFound")
        self.deleted.append(name)
        del self.jobs[name]

    def list_namespaced_job(self, namespace: str, label_selector: str) -> Any:
        assert label_selector == "app=severity-v2"
        return SimpleNamespace(items=list(self.jobs.values()))


class FakeCore:
    def __init__(self, reason: Optional[str] = None) -> None:
        self.reason = reason

    def list_namespaced_pod(self, namespace: str, label_selector: str) -> Any:
        terminated = SimpleNamespace(reason=self.reason)
        container = SimpleNamespace(state=SimpleNamespace(terminated=terminated))
        return SimpleNamespace(
            items=[
                SimpleNamespace(status=SimpleNamespace(container_statuses=[container]))
            ]
        )


def test_kubernetes_launcher(monkeypatch: pytest.MonkeyPatch) -> None:
    batch = FakeBatch()
    launcher = KubernetesLauncher(
        TargetProfile.load("gke-josh"), IMAGE, batch, FakeCore("OOMKilled")
    )
    assert launcher.status(JOB_ID) == JobStatus("unknown")
    assert launcher.start(spec())
    assert batch.created[0]["metadata"]["name"] == job_name(JOB_ID)
    assert not launcher.start(spec())  # 409: already exists
    assert launcher.status(JOB_ID) == JobStatus("pending")
    assert launcher.active_count() == 1

    batch.jobs[job_name(JOB_ID)] = v1job(
        [V1JobCondition(type="Failed", status="True", reason="BackoffLimitExceeded")]
    )
    status = launcher.status(JOB_ID)
    assert status.phase == "failed"
    assert status.reason == "BackoffLimitExceeded (pod: OOMKilled)"
    assert launcher.active_count() == 0

    launcher.delete(JOB_ID)
    launcher.delete(JOB_ID)  # already gone: fine
    assert batch.deleted == [job_name(JOB_ID)]


def test_missing_credentials_or_image_is_dispatch_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("K8S_KUBECONFIG_GKE_JOSH", raising=False)
    with pytest.raises(DispatchUnavailable, match="K8S_KUBECONFIG_GKE_JOSH"):
        KubernetesLauncher(TargetProfile.load("gke-josh"), IMAGE).status(JOB_ID)
    with pytest.raises(DispatchUnavailable, match="SEVERITY_IMAGE"):
        KubernetesLauncher(
            TargetProfile.load("gke-josh"), None, FakeBatch(), FakeCore()
        ).start(spec())
