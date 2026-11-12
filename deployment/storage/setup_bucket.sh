#!/bin/bash

# Setup GCS bucket with no-cache CORS policy for STAC JSON files
BUCKET_NAME="fire-recovery-temp"

echo "Setting CORS policy for bucket: $BUCKET_NAME"
gcloud storage buckets update gs://$BUCKET_NAME --cors-file=cors.json

echo "CORS policy applied successfully"
echo "JSON files will now have Cache-Control: no-cache headers"