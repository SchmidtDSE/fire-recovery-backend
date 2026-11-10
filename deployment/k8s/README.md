# Provisioning a cluster for severity v2

Analysis Jobs run on Kubernetes (design §1). The API, on Cloud Run, creates one Job per
analysis in a target cluster's namespace and reads its status. These steps set up one target.
Run them by hand with credentials for the cluster; CI never applies anything here.

The two targets are josh's GKE Autopilot cluster (`config/targets/gke-josh.yaml`) and NRP
Nautilus (`config/targets/nrp.yaml`). Both use the namespace `fire-severity-dev`.

## 1. Check the cluster

`podFailurePolicy`, which keeps a preempted pod from counting as a failure, needs Kubernetes
1.26 or later:

```bash
kubectl version
```

## 2. Namespace, dispatcher identity, Job credentials

```bash
kubectl apply -f deployment/k8s/namespace.yaml   # on NRP, request the namespace in the portal
kubectl apply -f deployment/k8s/rbac.yaml
cp deployment/k8s/secret.example.yaml /tmp/severity-v2-secret.yaml   # fill it in, outside the repo
kubectl apply -f /tmp/severity-v2-secret.yaml && rm /tmp/severity-v2-secret.yaml
```

`rbac.yaml` creates `severity-v2-dispatcher`, which may only create, read, list and delete
Jobs and read pods in the namespace. The `severity-v2` Secret holds the storage HMAC keys and
the optional SMTP account every Job reads.

## 3. A kubeconfig for the API

The API reaches the cluster with the dispatcher's token. A token kubeconfig works the same on
GKE and NRP:

```bash
NS=fire-severity-dev
TOKEN=$(kubectl -n $NS get secret severity-v2-dispatcher-token -o jsonpath='{.data.token}' | base64 -d)
SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
CA=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')
cat > /tmp/severity-kubeconfig.yaml <<EOF
apiVersion: v1
kind: Config
clusters: [{name: target, cluster: {server: $SERVER, certificate-authority-data: $CA}}]
users: [{name: severity-v2-dispatcher, user: {token: $TOKEN}}]
contexts: [{name: target, context: {cluster: target, user: severity-v2-dispatcher, namespace: $NS}}]
current-context: target
EOF
kubectl --kubeconfig /tmp/severity-kubeconfig.yaml -n $NS auth can-i create jobs   # yes
```

Store it in Secret Manager, which the dev deploy maps to the profile's `kubeconfig_env`, then
delete the file:

```bash
gcloud secrets create severity-v2-kubeconfig-gke-josh --project dse-nps \
  --data-file=/tmp/severity-kubeconfig.yaml
rm /tmp/severity-kubeconfig.yaml
```

For NRP the secret is `severity-v2-kubeconfig-nrp`, and the API reads it as
`K8S_KUBECONFIG_NRP`.

## 4. API keys

Programmatic clients send `X-API-Key`; the API stores only SHA-256 digests, comma-separated.
Create the secret even with no keys yet, because the deploy maps it:

```bash
printf '%s' "$(printf '%s' "$KEY" | sha256sum | cut -d' ' -f1)" | \
  gcloud secrets create severity-v2-api-key-hashes --project dse-nps --data-file=-
```

Grant the Cloud Run service account (`github-actions-sa`) `roles/secretmanager.secretAccessor`
on both secrets.

## 5. The image

CI pushes `ghcr.io/schmidtdse/fire-recovery-backend` on every deploy to dev. Make the GHCR
package public once, in the organization's package settings, so any cluster can pull it without
credentials.

## 6. Monitoring

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
