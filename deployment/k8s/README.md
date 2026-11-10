# Provisioning a cluster for severity v2

Analysis Jobs run on Kubernetes (design §1). The API creates one Job per analysis in a target
cluster and reads its status. Targets work as josh's do (SchmidtDSE/josh, `BATCH_REMOTE.md`):

- A JSON profile in `config/targets/` names a kubeconfig **context**, a namespace, the image,
  resources and spot scheduling. `SEVERITY_TARGET` picks the profile.
- The API reaches the cluster through that context, from `KUBECONFIG` or `~/.kube/config`.
- Each Job gets its own Secret with the storage and mail settings from the API's environment,
  owned by the Job, so no cluster needs a credential Secret made by hand.

Josh's cluster, namespace, bucket and storage identity are OpenTofu in
SchmidtDSE/fire-recovery-iac (`environments/josh-k8s/`, PR #4). Severity's belong beside
them. Until they're there, the manifests here describe the cluster side:

| | josh | severity |
|---|---|---|
| Cluster | `josh-k8s-gke` (`dse-nps`, `us-west1`) | the same cluster |
| Namespace | `joshsim` | `fire-severity-dev` (`namespace.yaml`) |
| Dispatcher role | Jobs, Secrets, pods | the same, plus `delete` and `update` for restarts (`rbac.yaml`) |
| Pod egress | DNS, HTTPS | DNS, HTTPS, SMTP (`networkpolicy.yaml`) |
| Bucket | `dse-nps-josh-batch-storage` | `dse-nps-fire-severity-storage`, public read |
| Storage identity | `josh-k8s-gcs-sa`: `storage.objectAdmin` and `storage.bucketViewer`; HMAC key in Secret Manager as `josh-k8s-minio-{access,secret}-key` | `fire-severity-k8s-gcs-sa`, the same roles; `fire-severity-k8s-minio-{access,secret}-key` |
| Image | `ghcr.io/schmidtdse/josh/joshsim-batch` | `ghcr.io/schmidtdse/fire-recovery-backend/severity-batch` |

The bucket is public-read because the API hands out links to the catalogs in it. The result
records beside them hold no personal data.

## 1. Check the cluster

`podFailurePolicy`, which keeps a preempted pod from counting as a failure, needs Kubernetes
1.26 or later:

```bash
kubectl version
```

## 2. Namespace, dispatcher role, egress

```bash
kubectl apply -f deployment/k8s/namespace.yaml   # on NRP, request the namespace in the portal
kubectl apply -f deployment/k8s/rbac.yaml
kubectl apply -f deployment/k8s/networkpolicy.yaml
```

## 3. Contexts

**People** use their own identity, as with josh:

```bash
# GKE: context gke_dse-nps_us-west1_josh-k8s-gke (needs gke-gcloud-auth-plugin, and a
# RoleBinding to the dispatcher role for your Google identity, as josh-k8s binds its users)
gcloud container clusters get-credentials josh-k8s-gke --region us-west1 --project dse-nps
# NRP: the kubeconfig from the portal, context nautilus
```

**The API on Cloud Run** has no Google login to run the GKE plugin with, so it uses a
kubeconfig whose contexts carry the same names, with the dispatcher's token. One file holds
every target's context:

```bash
NS=fire-severity-dev
CONTEXT=gke_dse-nps_us-west1_josh-k8s-gke   # the profile's context
TOKEN=$(kubectl -n $NS get secret severity-v2-dispatcher-token -o jsonpath='{.data.token}' | base64 -d)
SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
CA=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
cat > /tmp/severity-kubeconfig.yaml <<EOF
apiVersion: v1
kind: Config
clusters: [{name: $CONTEXT, cluster: {server: $SERVER, certificate-authority-data: $CA}}]
users: [{name: $CONTEXT-dispatcher, user: {token: $TOKEN}}]
contexts: [{name: $CONTEXT, context: {cluster: $CONTEXT, user: $CONTEXT-dispatcher, namespace: $NS}}]
current-context: $CONTEXT
EOF
kubectl --kubeconfig /tmp/severity-kubeconfig.yaml -n $NS auth can-i create jobs   # yes
gcloud secrets create severity-v2-kubeconfig --project dse-nps \
  --data-file=/tmp/severity-kubeconfig.yaml
rm /tmp/severity-kubeconfig.yaml
```

The dev deploy mounts it at `/var/secrets/kube/config` and points `KUBECONFIG` there. To add
NRP, add its context to the same file and add a version of the secret.

## 4. Storage

Create the bucket, its service account with `storage.objectAdmin` and `storage.bucketViewer`,
and an HMAC key in Secret Manager, as `environments/josh-k8s/` does for josh. Then grant
`allUsers` `storage.objectViewer` on the bucket. The dev deploy maps the HMAC secrets to
`S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` and passes them on to each Job.

## 5. API keys

Programmatic clients send `X-API-Key`; the API stores only SHA-256 digests, comma-separated.
Create the secret even with no keys yet, because the deploy maps it:

```bash
printf '%s' "$(printf '%s' "$KEY" | sha256sum | cut -d' ' -f1)" | \
  gcloud secrets create severity-v2-api-key-hashes --project dse-nps --data-file=-
```

Grant the Cloud Run service account (`github-actions-sa`) `roles/secretmanager.secretAccessor`
on `severity-v2-kubeconfig`, `severity-v2-api-key-hashes` and the two HMAC secrets.

## 6. The image

As josh does with `joshsim-batch`, CI pushes `ghcr.io/schmidtdse/fire-recovery-backend/severity-batch`:
`{sha}` and `latest` on every deploy to dev, and `{sha}` for each PR from this repository, so a
cluster can run a branch before it merges. Make the GHCR package public once, in the
organization's package settings, so any cluster can pull it without credentials.

## 7. Monitoring

Jobs that fail inside Python write a failed record and email the operator. Platform failures,
such as an OOM kill, the deadline or a bad image, don't reach that handler: the status URL
shows them, and each cluster's own monitoring should alert on them.

On josh's GKE cluster, a log-based alert on Job failure events does this. A starting filter,
to confirm against the cluster's event logs:

```text
resource.type="k8s_cluster"
logName="projects/dse-nps/logs/events"
jsonPayload.involvedObject.kind="Job"
jsonPayload.involvedObject.namespace="fire-severity-dev"
jsonPayload.reason=("BackoffLimitExceeded" OR "DeadlineExceeded" OR "PodFailurePolicy")
```

On NRP, use its own monitoring.

## Switching targets

`SEVERITY_TARGET` on the Cloud Run service names the profile (`gke-josh` or `nrp`). Job names
are unique per cluster, not across clusters, so switch only when no Jobs are running.
