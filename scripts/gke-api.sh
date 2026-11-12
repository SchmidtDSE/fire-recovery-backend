#!/usr/bin/env bash
# The API on :8000 dispatching to josh's GKE cluster, standing in for Cloud Run.
#
# The API reaches the cluster as your Application Default Credentials. To act as
# the API's own account, with exactly its permissions, sign in with:
#   gcloud auth application-default login \
#     --impersonate-service-account=fire-severity-k8s-api-sa@dse-nps.iam.gserviceaccount.com
#
# The API copies its storage and mail settings into each Job's Secret on the
# cluster, so every one is fixed here. Nothing falls back to .devcontainer/.env,
# whose keys are v1's. The bucket key comes from Secret Manager, as on Cloud Run,
# and mail is off.
set -euo pipefail

project=dse-nps
bucket=dse-nps-fire-severity-storage

# PR builds push an image for each pushed head, so default to this branch's last push.
image=${SEVERITY_IMAGE:-ghcr.io/schmidtdse/fire-recovery-backend/severity-batch:$(git rev-parse '@{upstream}')}

# Without its image, a Job would wait on the pull until its deadline, hours away.
if [[ $image == *@* ]]; then name=${image%@*} ref=${image#*@}; else name=${image%:*} ref=${image##*:}; fi
if [[ $name == ghcr.io/* ]]; then
  path=${name#ghcr.io/}
  token=$(curl -fsS "https://ghcr.io/token?scope=repository:$path:pull" |
    python -c 'import json, sys; print(json.load(sys.stdin)["token"])')
  if ! curl -fsS -o /dev/null -H "Authorization: Bearer $token" \
    -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json' \
    "https://ghcr.io/v2/$path/manifests/$ref"; then
    echo "GHCR has no public $image. Push the branch and wait for its Build Image check, or set SEVERITY_IMAGE." >&2
    exit 1
  fi
fi

secret() { gcloud secrets versions access latest --secret="$1" --project="$project"; }
S3_ACCESS_KEY_ID=$(secret fire-severity-k8s-minio-access-key)
S3_SECRET_ACCESS_KEY=$(secret fire-severity-k8s-minio-secret-key)

export SEVERITY_TARGET=gke-josh SEVERITY_IMAGE="$image"
export S3_ENDPOINT=storage.googleapis.com S3_SECURE=True S3_BUCKET="$bucket" S3_REGION=auto
export S3_PUBLIC_BASE_URL="https://storage.googleapis.com/$bucket"
export S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY
# Set but empty, so .devcontainer/.env can't fill them in.
export SMTP_HOST= SMTP_PORT= SMTP_USER= SMTP_PASSWORD= SMTP_FROM= OPERATOR_EMAIL=

echo "Dispatching to gke-josh with $image" >&2
exec uvicorn src.app:app --port 8000
