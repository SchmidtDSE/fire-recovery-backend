gcloud CLI Method:

# Set bucket-level metadata for JSON files
```
gcloud storage buckets update gs://fire-recovery-temp \
    --metadata-from-file=cloud_bucket_cache_control.yaml

```