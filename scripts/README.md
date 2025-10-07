# Scripts

Utility and maintenance scripts for the fire-recovery-backend project.

## init_stac_catalog.py

Initializes a STAC catalog in S3-compatible storage (MinIO or GCS).

**Usage:**
```bash
export S3_ENDPOINT=localhost:9000
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin
export S3_SECURE=False
export S3_BUCKET=test-bucket

pixi run python scripts/init_stac_catalog.py
```

**Purpose:**
Used in CI/CD workflows to ensure a STAC catalog exists before running tests.
Uses the application's own `STACCatalogManager` to create the catalog structure.
