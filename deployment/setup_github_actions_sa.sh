#!/bin/bash
set -e

# Configuration
PROJECT_ID="dse-nps"
SA_NAME="github-actions-sa"
SA_DISPLAY_NAME="GitHub Actions Service Account"
KEY_FILE="github-actions-key.json"

echo "Creating service account for GitHub Actions..."
# Create service account if it doesn't exist
if ! gcloud iam service-accounts describe ${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com &>/dev/null; then
  gcloud iam service-accounts create ${SA_NAME} \
    --display-name="${SA_DISPLAY_NAME}"
  echo "Service account created: ${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
else
  echo "Service account already exists."
fi

echo "Granting necessary permissions to GitHub Actions service account..."
# Grant permissions to the GitHub Actions service account
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/cloudbuild.builds.builder"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/run.admin"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountUser"

echo "Getting Cloud Build service account details..."
# Get the Cloud Build service account
CLOUDBUILD_SA=$(gcloud projects describe ${PROJECT_ID} --format='value(projectNumber)')@cloudbuild.gserviceaccount.com
echo "Cloud Build service account: ${CLOUDBUILD_SA}"

echo "Granting necessary permissions to Cloud Build service account..."
# Grant missing permissions to the Cloud Build service account
gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${CLOUDBUILD_SA}" \
  --role="roles/pubsub.publisher"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${CLOUDBUILD_SA}" \
  --role="roles/source.reader"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${CLOUDBUILD_SA}" \
  --role="roles/logging.logWriter"

echo "Creating service account key..."
# Create key file for GitHub Actions
gcloud iam service-accounts keys create ${KEY_FILE} \
  --iam-account=${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com

echo "Service account key created: ${KEY_FILE}"
echo ""
echo "To add this key to your GitHub repository secrets:"
echo "1. Copy the contents of the ${KEY_FILE} file"
echo "2. Create a new GitHub secret named GCP_SA_KEY with the contents"
echo "3. Delete the key file from your local machine after adding it to GitHub"
echo ""
echo "You can also use the GitHub CLI to add the secret:"
echo "cat ${KEY_FILE} | gh secret set GCP_SA_KEY -R <your-username>/fire-recovery-backend"
echo ""
echo "Done!"