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

## validate_frontend_examples.py

Validates frontend API example payloads against the backend OpenAPI schema.

**Usage:**
```bash
# Start backend server
pixi run python -m src.app

# In another terminal, extract schema and validate examples
curl http://localhost:8000/openapi.json > openapi.json

pixi run python scripts/validate_frontend_examples.py \
  --schema openapi.json \
  --examples-dir ../fire-recovery-frontend/tests/api_examples/
```

**Purpose:**
Catches frontend/backend API drift before deployment by validating that frontend example payloads match the backend OpenAPI schema. Used in CI/CD workflows to fail builds if drift is detected.

**Features:**
- Validates JSON example files against OpenAPI schema
- Resolves $ref references in schemas
- Provides colored terminal output with clear error messages
- Shows exact field paths for validation errors
- Exit code 0 for success, 1 for failures
