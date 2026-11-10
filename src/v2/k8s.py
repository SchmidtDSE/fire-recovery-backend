"""Analysis jobs as Kubernetes Jobs, on any cluster a target profile describes.

Target profiles use josh's format (SchmidtDSE/josh, BATCH_REMOTE.md): JSON with
``"type": "kubernetes"`` and a ``kubernetes`` block naming a kubeconfig context,
a namespace, an image, resources and scheduling. The API reaches the cluster
through that context, read from ``KUBECONFIG`` or ``~/.kube/config``, as josh's
dispatcher does. Each Job gets a Secret holding the storage and mail settings
from the API's own environment, owned by the Job: credentials never appear in a
Job spec, no cluster needs a credential Secret made by hand, and the Secret goes
when the Job does.

The Job spec (design §1) retries only what the platform did to a pod: an
evicted or preempted pod (condition DisruptionTarget) is rescheduled, and any
failure of ours is final (backoffLimit 0). A deadline caps the whole run,
rescheduling included.
"""

import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from kubernetes import client as k8s_client
from kubernetes.client.exceptions import ApiException
from kubernetes.config import new_client_from_config

from src.v2.constants import ENV_IMAGE
from src.v2.launcher import ENV_NOTIFY_EMAIL, DispatchUnavailable, JobSpec, JobStatus

logger = logging.getLogger(__name__)

ENV_TARGETS_DIR = "SEVERITY_TARGETS_DIR"
DEFAULT_TARGETS_DIR = Path(__file__).resolve().parents[2] / "config" / "targets"
TARGET_NAME = re.compile(r"^[A-Za-z0-9_-]+$")
APP_LABEL = "severity-v2"
JOB_ID_LABEL = "severity-v2/job-id"
JOB_NAME_PREFIX = "sev-"
SECRET_NAME_PREFIX = "sev-creds-"
CONTAINER_NAME = "run"
SCRATCH_PATH = "/scratch"
SPOT_LABEL = "cloud.google.com/gke-spot"
HTTP_NOT_FOUND = 404
HTTP_CONFLICT = 409

# What a job needs from the API's environment: storage, and mail if configured.
CREDENTIAL_ENV = (
    "S3_ENDPOINT",
    "S3_SECURE",
    "S3_BUCKET",
    "S3_REGION",
    "S3_PUBLIC_BASE_URL",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
    "SMTP_HOST",
    "SMTP_PORT",
    "SMTP_USER",
    "SMTP_PASSWORD",
    "SMTP_FROM",
    "OPERATOR_EMAIL",
)


def job_name(job_id: str) -> str:
    """Deterministic, so a duplicate create returns 409."""
    return f"{JOB_NAME_PREFIX}{job_id}"


def secret_name(job_id: str) -> str:
    return f"{SECRET_NAME_PREFIX}{job_id}"


@dataclass(frozen=True)
class TargetProfile:
    """One cluster, in josh's profile format: how to reach it and how pods run."""

    name: str
    namespace: str
    # A kubeconfig context; None means the kubeconfig's current context.
    context: Optional[str] = None
    # Used when SEVERITY_IMAGE is unset; the deploy pins a digest instead.
    image: Optional[str] = None
    resources: Dict[str, Dict[str, str]] = field(default_factory=dict)
    timeout_seconds: int = 21600
    ttl_seconds_after_finished: int = 86400
    spot: bool = False
    node_selector: Dict[str, str] = field(default_factory=dict)
    # Where pods reach storage, when not where the API does (in CI, a service
    # inside the cluster).
    pod_s3_endpoint: Optional[str] = None
    threads: int = 2
    scratch_size: str = "10Gi"

    @classmethod
    def load(cls, name: str, directory: Optional[Path] = None) -> "TargetProfile":
        """Read ``{name}.json`` from SEVERITY_TARGETS_DIR, by default config/targets/.

        Raises:
            FileNotFoundError: if there is no such profile.
            ValueError: if the name or the profile is invalid.
        """
        if not TARGET_NAME.match(name):
            raise ValueError(f"Invalid target name {name!r}")
        root = directory or Path(os.environ.get(ENV_TARGETS_DIR) or DEFAULT_TARGETS_DIR)
        data = json.loads((root / f"{name}.json").read_text())
        k8s = data.get("kubernetes")
        if data.get("type") != "kubernetes" or not isinstance(k8s, dict):
            raise ValueError(f"Target {name!r} is not a Kubernetes profile")
        if not k8s.get("namespace"):
            raise ValueError(f"Target {name!r} names no namespace")
        return cls(
            name=name,
            namespace=k8s["namespace"],
            context=k8s.get("context"),
            image=k8s.get("image"),
            resources=k8s.get("resources") or {},
            timeout_seconds=int(k8s.get("timeoutSeconds", 21600)),
            ttl_seconds_after_finished=int(k8s.get("ttlSecondsAfterFinished", 86400)),
            spot=bool(k8s.get("spot", False)),
            node_selector=k8s.get("nodeSelector") or {},
            pod_s3_endpoint=k8s.get("pod_s3_endpoint"),
            threads=int(k8s.get("threads", 2)),
            scratch_size=str(k8s.get("scratchSize", "10Gi")),
        )


def labels(job_id: str) -> Dict[str, str]:
    return {"app": APP_LABEL, JOB_ID_LABEL: job_id}


def job_manifest(spec: JobSpec, profile: TargetProfile, image: str) -> Dict[str, Any]:
    """The batch/v1 Job for one analysis."""
    # The requester's address goes in the Job's Secret, not in its spec.
    env = {k: v for k, v in spec.environment().items() if k != ENV_NOTIFY_EMAIL}
    env.update(
        {
            ENV_IMAGE: image,
            "SEVERITY_WORKDIR": SCRATCH_PATH,
            "SEVERITY_THREADS": str(profile.threads),
        }
    )
    container: Dict[str, Any] = {
        "name": CONTAINER_NAME,
        "image": image,
        "args": ["python", "-m", "src.v2.run"],
        "env": [{"name": key, "value": value} for key, value in env.items()],
        "envFrom": [{"secretRef": {"name": secret_name(spec.job_id)}}],
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
    node_selector = dict(profile.node_selector)
    tolerations: List[Dict[str, str]] = []
    if profile.spot:
        # As josh does: spot nodes, and the taint GKE gives them.
        node_selector[SPOT_LABEL] = "true"
        tolerations.append(
            {
                "key": SPOT_LABEL,
                "operator": "Equal",
                "value": "true",
                "effect": "NoSchedule",
            }
        )
    if node_selector:
        pod_spec["nodeSelector"] = node_selector
    if tolerations:
        pod_spec["tolerations"] = tolerations
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name(spec.job_id), "labels": labels(spec.job_id)},
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
            "activeDeadlineSeconds": profile.timeout_seconds,
            "ttlSecondsAfterFinished": profile.ttl_seconds_after_finished,
            "template": {"metadata": {"labels": labels(spec.job_id)}, "spec": pod_spec},
        },
    }


def job_credentials(
    spec: JobSpec, profile: TargetProfile, env: Mapping[str, str]
) -> Dict[str, str]:
    """The Job's Secret: the API's storage and mail settings, and who to tell."""
    data = {key: env[key] for key in CREDENTIAL_ENV if env.get(key)}
    if profile.pod_s3_endpoint:
        data["S3_ENDPOINT"] = profile.pod_s3_endpoint
    if spec.notify_email:
        data[ENV_NOTIFY_EMAIL] = spec.notify_email
    return data


def secret_manifest(
    spec: JobSpec, profile: TargetProfile, env: Mapping[str, str], owner_uid: str
) -> Dict[str, Any]:
    """The v1 Secret a Job reads, deleted with the Job (by its TTL or a restart)."""
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": secret_name(spec.job_id),
            "labels": labels(spec.job_id),
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": job_name(spec.job_id),
                    "uid": owner_uid,
                }
            ],
        },
        "stringData": job_credentials(spec, profile, env),
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
        env: Optional[Mapping[str, str]] = None,
    ):
        self.profile = profile
        self.image = image
        self._batch = batch
        self._core = core
        self._env = env

    def _apis(self) -> tuple[Any, Any]:
        if self._batch is None or self._core is None:
            try:
                api_client = new_client_from_config(
                    context=self.profile.context, persist_config=False
                )
            except Exception as e:  # no kubeconfig, no such context, a failing plugin
                raise DispatchUnavailable(
                    f"No usable kubeconfig context {self.profile.context!r} for target "
                    f"{self.profile.name!r} (KUBECONFIG or ~/.kube/config): {e}"
                ) from e
            self._batch = k8s_client.BatchV1Api(api_client)
            self._core = k8s_client.CoreV1Api(api_client)
        return self._batch, self._core

    def start(self, spec: JobSpec) -> bool:
        if not self.image:
            raise DispatchUnavailable(
                f"{ENV_IMAGE} is not set and target {self.profile.name!r} names no image"
            )
        batch, core = self._apis()
        try:
            job = batch.create_namespaced_job(
                self.profile.namespace, job_manifest(spec, self.profile, self.image)
            )
        except ApiException as e:
            if e.status == HTTP_CONFLICT:
                return False
            raise DispatchUnavailable(f"Creating the Job failed: {e.reason}") from e
        # Created after the Job, so it can name the Job as its owner; the pod
        # waits for it.
        env = os.environ if self._env is None else self._env
        try:
            self._put_secret(
                core, secret_manifest(spec, self.profile, env, job.metadata.uid)
            )
        except ApiException as e:
            # Without its credentials the pod could only wait out its deadline.
            self.delete(spec.job_id)
            raise DispatchUnavailable(
                f"Creating the Job's credentials failed: {e.reason}"
            ) from e
        logger.info(f"Created Job {job_name(spec.job_id)} in {self.profile.namespace}")
        return True

    def _put_secret(self, core: Any, secret: Dict[str, Any]) -> None:
        name, namespace = secret["metadata"]["name"], self.profile.namespace
        try:
            core.create_namespaced_secret(namespace, secret)
            return
        except ApiException as e:
            if e.status != HTTP_CONFLICT:
                raise
        # Left by the Job this one replaces, and not yet collected.
        try:
            core.replace_namespaced_secret(name, namespace, secret)
        except ApiException as e:
            if e.status != HTTP_NOT_FOUND:
                raise
            core.create_namespaced_secret(namespace, secret)

    def status(self, job_id: str) -> JobStatus:
        batch, _ = self._apis()
        try:
            job = batch.read_namespaced_job(job_name(job_id), self.profile.namespace)
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
                state = container.state
                terminated = state.terminated if state else None
                if terminated is not None and terminated.reason:
                    return str(terminated.reason)
                waiting = state.waiting if state else None
                if waiting is not None and waiting.reason:
                    return str(waiting.reason)
        return None

    def delete(self, job_id: str) -> None:
        """Delete the Job; its pods and Secret follow, as its dependents."""
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
