"""Analysis jobs as Kubernetes Jobs, on any cluster a target profile describes.

The Job spec (design §1) retries only what the platform did to a pod: an
evicted or preempted pod (condition DisruptionTarget) is rescheduled, and any
failure of ours is final (backoffLimit 0). A deadline caps the whole run,
rescheduling included. Everything cluster-specific lives in the profile:
namespace, credentials, scheduling, resources.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from kubernetes import client as k8s_client
from kubernetes.client.exceptions import ApiException
from kubernetes.config import new_client_from_config_dict

from src.v2.constants import ENV_IMAGE
from src.v2.launcher import DispatchUnavailable, JobSpec, JobStatus

logger = logging.getLogger(__name__)

PROFILES_DIR = Path(__file__).resolve().parents[2] / "config" / "targets"
APP_LABEL = "severity-v2"
JOB_ID_LABEL = "severity-v2/job-id"
JOB_NAME_PREFIX = "sev-"
CONTAINER_NAME = "run"
SCRATCH_PATH = "/scratch"
HTTP_NOT_FOUND = 404
HTTP_CONFLICT = 409


def job_name(job_id: str) -> str:
    """Deterministic, so a duplicate create returns 409."""
    return f"{JOB_NAME_PREFIX}{job_id}"


@dataclass(frozen=True)
class TargetProfile:
    """One cluster: where Jobs go, how the API reaches it, how pods are scheduled."""

    name: str
    namespace: str
    kubeconfig_env: str
    secret_name: str = "severity-v2"
    node_selector: Dict[str, str] = field(default_factory=dict)
    tolerations: List[Dict[str, Any]] = field(default_factory=list)
    resources: Dict[str, Dict[str, str]] = field(default_factory=dict)
    threads: int = 2
    scratch_size: str = "10Gi"
    active_deadline_seconds: int = 21600
    ttl_seconds_after_finished: int = 86400

    @classmethod
    def load(cls, name: str, directory: Path = PROFILES_DIR) -> "TargetProfile":
        """Read ``config/targets/{name}.yaml``.

        Raises:
            FileNotFoundError: if there is no such profile.
        """
        data = yaml.safe_load((directory / f"{name}.yaml").read_text())
        return cls(name=name, **data)


def job_manifest(spec: JobSpec, profile: TargetProfile, image: str) -> Dict[str, Any]:
    """The batch/v1 Job for one analysis."""
    labels = {"app": APP_LABEL, JOB_ID_LABEL: spec.job_id}
    env = {
        **spec.environment(),
        ENV_IMAGE: image,
        "SEVERITY_WORKDIR": SCRATCH_PATH,
        "SEVERITY_THREADS": str(profile.threads),
    }
    container: Dict[str, Any] = {
        "name": CONTAINER_NAME,
        "image": image,
        "args": ["python", "-m", "src.v2.run"],
        "env": [{"name": key, "value": value} for key, value in env.items()],
        "envFrom": [{"secretRef": {"name": profile.secret_name}}],
        "volumeMounts": [{"name": "scratch", "mountPath": SCRATCH_PATH}],
    }
    if profile.resources:
        container["resources"] = profile.resources
    pod_spec: Dict[str, Any] = {
        # Required by podFailurePolicy.
        "restartPolicy": "Never",
        # The analysis never calls the cluster API, so it gets no token.
        "automountServiceAccountToken": False,
        "containers": [container],
        "volumes": [
            {"name": "scratch", "emptyDir": {"sizeLimit": profile.scratch_size}}
        ],
    }
    if profile.node_selector:
        pod_spec["nodeSelector"] = profile.node_selector
    if profile.tolerations:
        pod_spec["tolerations"] = profile.tolerations
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name(spec.job_id), "labels": labels},
        "spec": {
            # A failure of ours is never retried...
            "backoffLimit": 0,
            # ...but an evicted or preempted pod is rescheduled.
            "podFailurePolicy": {
                "rules": [
                    {
                        "action": "Ignore",
                        "onPodConditions": [{"type": "DisruptionTarget"}],
                    }
                ]
            },
            "activeDeadlineSeconds": profile.active_deadline_seconds,
            "ttlSecondsAfterFinished": profile.ttl_seconds_after_finished,
            "template": {"metadata": {"labels": labels}, "spec": pod_spec},
        },
    }


def job_status(job: Any) -> JobStatus:
    """A V1Job's phase: its Complete or Failed condition, else active and ready pods."""
    status = job.status
    for condition in (status.conditions if status else None) or []:
        if condition.status != "True":
            continue
        if condition.type == "Complete":
            return JobStatus("succeeded")
        if condition.type == "Failed":
            detail = f": {condition.message}" if condition.message else ""
            return JobStatus("failed", reason=f"{condition.reason}{detail}")
    if status and (status.ready or 0) > 0:
        return JobStatus("running")
    return JobStatus("pending")


class KubernetesLauncher:
    """Creates, reads, counts and deletes Jobs in the profile's namespace."""

    def __init__(
        self,
        profile: TargetProfile,
        image: Optional[str],
        batch: Optional[Any] = None,
        core: Optional[Any] = None,
    ):
        self.profile = profile
        self.image = image
        self._batch = batch
        self._core = core

    def _apis(self) -> tuple[Any, Any]:
        if self._batch is None or self._core is None:
            text = os.environ.get(self.profile.kubeconfig_env)
            if not text:
                raise DispatchUnavailable(
                    f"{self.profile.kubeconfig_env} is not set: no credentials for "
                    f"target {self.profile.name!r}"
                )
            api_client = new_client_from_config_dict(yaml.safe_load(text))
            self._batch = k8s_client.BatchV1Api(api_client)
            self._core = k8s_client.CoreV1Api(api_client)
        return self._batch, self._core

    def start(self, spec: JobSpec) -> bool:
        if not self.image:
            raise DispatchUnavailable(f"{ENV_IMAGE} is not set: no image to run")
        batch, _ = self._apis()
        try:
            batch.create_namespaced_job(
                self.profile.namespace, job_manifest(spec, self.profile, self.image)
            )
        except ApiException as e:
            if e.status == HTTP_CONFLICT:
                return False
            raise DispatchUnavailable(f"Creating the Job failed: {e.reason}") from e
        logger.info(f"Created Job {job_name(spec.job_id)} in {self.profile.namespace}")
        return True

    def status(self, job_id: str) -> JobStatus:
        batch, _ = self._apis()
        try:
            job = batch.read_namespaced_job_status(
                job_name(job_id), self.profile.namespace
            )
        except ApiException as e:
            if e.status == HTTP_NOT_FOUND:
                return JobStatus("unknown")
            raise DispatchUnavailable(f"Reading the Job failed: {e.reason}") from e
        status = job_status(job)
        if status.phase == "failed":
            pod_reason = self._pod_failure(job_id)
            if pod_reason:
                return JobStatus(
                    "failed", reason=f"{status.reason} (pod: {pod_reason})"
                )
        return status

    def _pod_failure(self, job_id: str) -> Optional[str]:
        """Why the Job's pod stopped, e.g. OOMKilled: the Job only says it failed."""
        _, core = self._apis()
        try:
            pods = core.list_namespaced_pod(
                self.profile.namespace, label_selector=f"job-name={job_name(job_id)}"
            )
        except ApiException:
            return None
        for pod in pods.items:
            for container in (
                (pod.status.container_statuses or []) if pod.status else []
            ):
                terminated = container.state.terminated if container.state else None
                if terminated is not None and terminated.reason:
                    return str(terminated.reason)
        return None

    def delete(self, job_id: str) -> None:
        batch, _ = self._apis()
        try:
            batch.delete_namespaced_job(
                job_name(job_id),
                self.profile.namespace,
                propagation_policy="Background",
            )
        except ApiException as e:
            if e.status != HTTP_NOT_FOUND:
                raise DispatchUnavailable(f"Deleting the Job failed: {e.reason}") from e

    def active_count(self) -> int:
        batch, _ = self._apis()
        try:
            jobs = batch.list_namespaced_job(
                self.profile.namespace, label_selector=f"app={APP_LABEL}"
            )
        except ApiException as e:
            raise DispatchUnavailable(f"Listing Jobs failed: {e.reason}") from e
        return sum(1 for job in jobs.items if job_status(job).active)
