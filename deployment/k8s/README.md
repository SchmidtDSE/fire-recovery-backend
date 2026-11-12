# Provisioning a cluster for severity v2

Analysis Jobs run on Kubernetes (design §1). The API, a Cloud Run service, is the one thing
that dispatches: it validates requests, applies API keys and rate limits, then creates one Job
per analysis and reads its status. Targets work as josh's do (SchmidtDSE/josh,
`BATCH_REMOTE.md`):

- A JSON profile in `config/targets/` names the cluster, a namespace, the image, resources and
  spot scheduling. `SEVERITY_TARGET` picks the profile.
- **No keys.** The API reaches josh's GKE cluster as the Cloud Run service's own Google
  identity, from the metadata server, as josh's machines and its Tofu reach it. A RoleBinding
  in the namespace grants that identity the dispatcher role.
- Each Job gets its own Secret with the storage and mail settings from the API's environment,
  owned by the Job, so no cluster needs a credential Secret made by hand.

## Who sets up what

- **On josh's GKE cluster, OpenTofu does.** SchmidtDSE/fire-recovery-iac
  `environments/fire-severity-k8s/` (PR #5), beside josh's `environments/josh-k8s/`, creates:
  - the namespace, the dispatcher role and its bindings, egress and a quota
  - the bucket
  - both service accounts
  - the secrets

  A person applies it. Don't apply the manifests here to that cluster, because Tofu owns the
  same objects.
- **The manifests here** are for clusters Tofu doesn't manage: CI's Kind cluster and NRP. They
  also show what Tofu creates.
- **This repository's CI owns the Cloud Run revisions.** `gcloud run deploy` sets the image,
  environment, secret references, runtime account, scaling and traffic on every push to `dev`
  or `main`, and Tofu ignores those fields. The runtime account belongs with CI because it
  changes in the same revision as the secret references, as it does at the switch to v2.

| | josh | severity |
|---|---|---|
| Cluster | `josh-k8s-gke` (`dse-nps`, `us-west1`) | the same cluster |
| Namespace | `joshsim` | `fire-severity-dev` (`namespace.yaml`) |
| Who dispatches | people and dev VMs, as their Google identity: `container.clusterViewer` plus a RoleBinding | the API's Cloud Run account, `fire-severity-k8s-api-sa`, the same way (`rbac.yaml`) |
| Dispatcher role | Jobs, Secrets, pods | the same, plus `delete` and `update` for restarts |
| Pod egress | DNS, HTTPS | DNS, HTTPS, SMTP, for every pod in the namespace (`networkpolicy.yaml`) |
| Quota | none | four Jobs' requests, from Tofu only (§1) |
| Bucket | `dse-nps-josh-batch-storage` | `dse-nps-fire-severity-storage`, public read, with CORS for browsers |
| Storage identity | `josh-k8s-gcs-sa`: `storage.objectAdmin`, `storage.bucketViewer`; HMAC key in Secret Manager as `josh-k8s-minio-{access,secret}-key` | `fire-severity-k8s-gcs-sa`, the same roles; `fire-severity-k8s-minio-{access,secret}-key` |
| Image | `ghcr.io/schmidtdse/josh/joshsim-batch` | `ghcr.io/schmidtdse/fire-recovery-backend/severity-batch` |

The bucket is public-read because the API hands out links to the catalogs in it. The result
records beside them hold no personal data.

## 1. Check the cluster

`podFailurePolicy`, which keeps a preempted pod from counting as a failure, needs Kubernetes
1.26 or later. Josh's cluster runs 1.35:

```bash
kubectl version
```

On josh's cluster, Tofu's `severity-v2-quota` admits exactly `MAX_ACTIVE_JOBS`
(`src/v2/constants.py`) Jobs at the `gke-josh` profile's requests. If you change either one,
change the quota too.

## 2. Namespace, dispatcher role, egress

Tofu creates these on josh's cluster. Elsewhere:

```bash
kubectl apply -f deployment/k8s/namespace.yaml   # on NRP, request the namespace in the portal
kubectl apply -f deployment/k8s/rbac.yaml
kubectl apply -f deployment/k8s/networkpolicy.yaml
```

## 3. Who may dispatch

- **The API.** The Cloud Run service runs as `fire-severity-k8s-api-sa`. It has
  `roles/container.clusterViewer` on the project, to read the cluster's endpoint and CA, and the
  `severity-v2-api` RoleBinding in the namespace. No key exists for the account.

  The API asks for the `userinfo.email` scope on its token, so GKE identifies it by its email,
  the subject in `rbac.yaml`. Tofu also binds the account's numeric unique ID, which is how GKE
  identifies a token without that scope.
- **People, to test.** Either list yourself in the Tofu environment's
  `dispatcher_user_emails`, which grants `clusterViewer` and a RoleBinding, or act as the API's
  account with Token Creator on it. Acting as the account tests exactly its permissions.
  `docs/DEVELOPING.md` shows both, with `pixi run gke-api`. For kubectl, also run
  `gcloud container clusters get-credentials josh-k8s-gke --region us-west1 --project dse-nps`.
- **NRP** has no Google identities. Dispatching there from Cloud Run would need a kubeconfig
  with a token for the `severity-v2-dispatcher` ServiceAccount. That is the one credential this
  setup can't avoid, so it waits until NRP is a target.

## 4. Storage

Tofu creates the bucket and grants `allUsers` `storage.objectViewer` on it. It creates
`fire-severity-k8s-gcs-sa` with `storage.objectAdmin` and `storage.bucketViewer` on the bucket,
and stores an HMAC key for that account in Secret Manager, as `environments/josh-k8s/` does for
josh. The dev deploy maps the HMAC secrets to `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`, and
the API passes them on to each Job.

## 5. API keys

Programmatic clients send `X-API-Key`. The API stores only SHA-256 digests, comma-separated, in
the secret `severity-v2-api-key-hashes`. Tofu creates the secret and grants the API's account
access to it, but sets no value.

Add a version before the first deploy to dev, which maps the secret, and again whenever the keys
change. Each version replaces the whole list, so include every key's digest:

```bash
printf '%s' "$(printf '%s' "$KEY" | sha256sum | cut -d' ' -f1)" | \
  gcloud secrets versions add severity-v2-api-key-hashes --project dse-nps --data-file=-
```

With no keys yet, store a lone comma (`printf ','`). The API ignores blank entries.

## 6. Mail

Jobs email the requester when an analysis finishes, and the operator when one fails. They send
through a Namecheap Private Email mailbox, `noreply@dse-disturbance-toolbox.org`, over SSL on port
465. The dev deploy sets the host, port and user; the password and the operator's address are
secrets. Tofu (`environments/fire-severity-k8s/secrets.tf`) creates `severity-v2-smtp-password` and
`severity-v2-operator-email` and grants the API's account access to each, but sets no value.

Add both values before the first deploy that maps them:

```bash
read -rs P && printf '%s' "$P" | gcloud secrets versions add severity-v2-smtp-password --project dse-nps --data-file=-; unset P
read -r E && printf '%s' "$E" | gcloud secrets versions add severity-v2-operator-email --project dse-nps --data-file=-; unset E
```

- **Test a new password locally first,** with the same code the Jobs run. `send` never raises,
  so logging shows why a send failed:

  ```bash
  read -rs P; SMTP_USER=noreply@dse-disturbance-toolbox.org SMTP_PASSWORD="$P" pixi run python -c \
    'import logging; logging.basicConfig(); from src.v2.job.notify import Mailer, SmtpSettings; print(Mailer(SmtpSettings.from_env()).send("you@example.org", "severity v2 mail test", "test"))'; unset P
  ```

- **A new version reaches the service only with a new revision.** Cloud Run reads `latest`
  when an instance starts, and dev keeps one warm. Redeploy, or run
  `gcloud run services update fire-recovery-backend-dev --region us-central1`.
- **The domain needs SPF, DKIM and DMARC records** for the mail to land in inboxes. Gmail's
  "Show original" reports all three.
- **Keep the mailbox for sending only.** Each Job's Secret carries the password into the
  cluster, and anyone who can read Secrets in the namespace can read it.

## 7. The image

As josh does with `joshsim-batch`, CI pushes `ghcr.io/schmidtdse/fire-recovery-backend/severity-batch`:
`{sha}` and `latest` on every deploy to dev, and `{sha}` for each PR from this repository, so a
cluster can run a branch before it merges. The package took the repository's public visibility
on its first push, so any cluster pulls it without credentials.

## 8. Monitoring

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
