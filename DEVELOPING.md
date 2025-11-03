### Pixi (Python)

Python and its dependencies are managed by `pixi` - you will need to activate pixi to use python, either by activating using `pixi shell`, or by using `pixi run`. 

```bash
pixi shell
python3 -m src.app
```

Or, optionally, as a one-liner:

```bash
pixi run python3 -m src.app
```

Additionally, by inspecting `pixi.toml`, you can see particular pre-configured tasks such as:

```
pixi run pytest
pixi run ruff-fix
```

### Mypy

This repo strives to adhere to type-checking using mypy. All code should pass static type checking without errors or warnings.

```bash
pixi run mypy
```

### Code Formatting and Linting

Code formatting and linting is handled by Ruff:

```bash
# Format code
pixi run ruff format

# Check for linting issues
pixi run ruff check

# Auto-fix linting issues
pixi run ruff-fix
```

### Testing

Tests are run using pytest:

```bash
# Run all tests
pixi run pytest

# Run with coverage
pixi run pytest --cov

# Run specific test file
pixi run pytest tests/test_example.py
```

### Contract Testing

Contract tests ensure API stability and prevent breaking changes between backend and frontend:

```bash
# Run all contract tests
pixi run pytest tests/contract/

# Run specific contract test suite
pixi run pytest tests/contract/test_openapi_schema.py
pixi run pytest tests/contract/test_schema_stability.py

# Generate baseline schema (after intentional API changes)
pixi run python -m scripts.generate_baseline_schema
```

**When to update the baseline**:
- After adding new endpoints
- After modifying request/response models
- After intentional breaking changes (coordinate with frontend)

**Never update the baseline without**:
1. Reviewing the schema diff
2. Confirming changes are intentional
3. Coordinating with frontend team if breaking

## Environment Variables & Secrets

### Overview

The fire-recovery-backend uses **S3-compatible storage abstraction** (via `MinioCloudStorage`) that works with:
- **Google Cloud Storage (GCS)** in production (Cloud Run)
- **MinIO** for local development and testing

### S3_* Naming Convention

All environment variables use the **S3_* naming convention** to reflect that both GCS and MinIO use S3-compatible protocols.

### Environment Variable Inventory

#### Storage Credentials (S3_*)

| Variable | Context | Required | Description | Example Value |
|----------|---------|----------|-------------|---------------|
| `S3_ACCESS_KEY_ID` | All | Yes | S3-compatible access key for storage | `minioadmin` (local), `GOOG1E...` (GCS) |
| `S3_SECRET_ACCESS_KEY` | All | Yes | S3-compatible secret key for storage | `minioadmin` (local), `abc123...` (GCS) |

#### Storage Configuration (S3_*)

| Variable | Context | Required | Description | Example Value |
|----------|---------|----------|-------------|---------------|
| `S3_ENDPOINT` | All | Yes | S3-compatible endpoint URL | `localhost:9000` (local), `storage.googleapis.com` (GCS) |
| `S3_SECURE` | All | Yes | Use HTTPS for connections | `False` (local MinIO), `True` (GCS) |
| `S3_BUCKET` | All | Yes | Bucket name for storage | `test-bucket` (local), `fire-recovery-temp` (GCS) |

#### Application Configuration

| Variable | Context | Required | Description | Example Value |
|----------|---------|----------|-------------|---------------|
| `RUN_LOCAL` | Local Development | Optional | Use local MinIO for satellite data processing | `True` |

### Setup Instructions by Environment

#### Local Development (Running the App)

For local development, you need credentials to connect to **either** local MinIO **or** Google Cloud Storage.

**Option 1: Using Local MinIO (Recommended for Development)**

1. Start MinIO locally using Docker:
   ```bash
   docker run -d \
     -p 9000:9000 -p 9001:9001 \
     -e "MINIO_ROOT_USER=minioadmin" \
     -e "MINIO_ROOT_PASSWORD=minioadmin" \
     -v /tmp/minio-data:/data \
     quay.io/minio/minio server /data --console-address ":9001"
   ```

2. Create `.devcontainer/.env`:
   ```dotenv
   # S3-compatible storage credentials
   S3_ACCESS_KEY_ID=minioadmin
   S3_SECRET_ACCESS_KEY=minioadmin
   RUN_LOCAL=True
   ```

   **Note:** The `RUN_LOCAL=True` setting tells the spectral index processor to use local MinIO.

**Option 2: Using Google Cloud Storage**

1. Create GCS HMAC credentials:
   - Go to [Google Cloud Console > Cloud Storage > Settings > Interoperability](https://console.cloud.google.com/storage/settings;tab=interoperability)
   - Create a new HMAC key for your service account
   - Save the Access Key and Secret

2. Create `.devcontainer/.env`:
   ```dotenv
   # S3-compatible storage credentials
   S3_ACCESS_KEY_ID=GOOG1E...your-access-key
   S3_SECRET_ACCESS_KEY=...your-secret-key
   RUN_LOCAL=False
   ```

#### Testing (pytest)

Tests use a separate configuration file to isolate test credentials from app credentials.

**For Local Testing with MinIO:**

1. Ensure MinIO is running (see Local Development setup above)

2. Create `.devcontainer/test.env`:
   ```dotenv
   # S3-compatible storage configuration
   S3_ENDPOINT=localhost:9000
   S3_ACCESS_KEY_ID=minioadmin
   S3_SECRET_ACCESS_KEY=minioadmin
   S3_SECURE=False
   S3_BUCKET=test-bucket
   ```

3. Run tests:
   ```bash
   pixi run pytest
   ```

**For Testing Against GCS:**

If you want to run tests against Google Cloud Storage instead of local MinIO:

1. Create `.devcontainer/test.env`:
   ```dotenv
   # S3-compatible storage configuration
   S3_ENDPOINT=storage.googleapis.com
   S3_ACCESS_KEY_ID=GOOG1E...your-access-key
   S3_SECRET_ACCESS_KEY=...your-secret-key
   S3_SECURE=True
   S3_BUCKET=fire-recovery-temp
   ```

   **Warning:** Running tests against GCS will incur storage costs and may leave test artifacts in your bucket.

#### CI/CD (GitHub Actions)

The GitHub Actions workflow (`.github/workflows/pytest.yml`) automatically:

1. Starts a MinIO service container
2. Creates both `.env` and `test.env` files with S3_* credentials
3. Sets environment variables for the test run

**Environment variables set in CI:**

```yaml
# S3-compatible storage configuration
S3_ENDPOINT=localhost:9000
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin
S3_SECURE=False
S3_BUCKET=test-bucket
RUN_LOCAL=True
```

No configuration needed - this happens automatically in CI.

#### Production (Cloud Run)

Production deployments use Google Cloud Storage with HMAC credentials.

**Required Secret Manager secrets:**

1. `S3_ACCESS_KEY_ID` - GCS HMAC access key
2. `S3_SECRET_ACCESS_KEY` - GCS HMAC secret key

These are injected into the Cloud Run service as environment variables. See your Cloud Run service configuration or deployment scripts for details.

### Credential Resolution Flow

When `MinioCloudStorage` is instantiated, it resolves credentials in this order:

```python
# 1. Constructor arguments (highest priority)
storage = MinioCloudStorage(
    access_key="explicit_key",
    secret_key="explicit_secret"
)

# 2. S3_* environment variables
# Checks: S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
```

**Example resolution in testing:**
- Test creates `MinioCloudStorage` with explicit credentials as arguments
- Constructor arguments take precedence, so test credentials are used

**Example resolution in production:**
- App creates `MinioCloudStorage` without explicit credentials
- Falls back to `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` environment variables
- Production S3-compatible credentials are used

### Security Best Practices

#### Local Development

1. **Never commit `.env` or `test.env` files**
   - These are already in `.gitignore` as `.devcontainer/.env` and `.devcontainer/test.env`
   - Always verify with `git status` before committing

2. **Use local MinIO for development when possible**
   - Avoids exposing real cloud credentials
   - Prevents accidental costs or data exposure
   - Faster iteration without network latency

3. **Store real credentials securely**
   - Use a password manager for GCS HMAC keys
   - Don't store in plain text files outside the project
   - Don't share credentials via chat/email

#### Credential Rotation

If credentials are accidentally exposed (committed to git, shared publicly, etc.):

**For local MinIO credentials:**
1. No action needed - these are local-only mock credentials
2. Restart MinIO with new credentials if concerned

**For GCS HMAC credentials:**
1. **Immediately** delete the exposed HMAC key in [Google Cloud Console](https://console.cloud.google.com/storage/settings;tab=interoperability)
2. Generate a new HMAC key
3. Update secrets in:
   - Local `.devcontainer/.env` file
   - Cloud Run Secret Manager secrets
   - Any CI/CD secrets (if used)
4. If committed to git:
   - Rotate credentials BEFORE removing from git history
   - Use tools like `git-filter-repo` to purge from history
   - Force push to remote (coordinate with team)
   - Consider the repository compromised; may need to regenerate all credentials

#### What's in .gitignore

The following files are excluded from version control for security:

```
.devcontainer/.env          # Local app credentials
.devcontainer/test.env      # Test credentials
github-actions-key.json     # Service account keys (if used)
```

Always check `.gitignore` before adding new credential files.

### Troubleshooting

#### "S3 credentials must be provided" Error

**Cause:** `MinioCloudStorage` cannot find S3_* credentials.

**Solution:**
- For app startup: Ensure `.devcontainer/.env` has `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`
- For tests: Ensure `.devcontainer/test.env` has `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY`

#### Tests Skip with "S3-compatible storage credentials not available"

**Cause:** The `minio_available` fixture checks for required environment variables and doesn't find them.

**Solution:**
1. Verify `.devcontainer/test.env` exists with all required S3_* variables
2. Restart your development container to reload environment variables
3. Check that pytest is loading the test environment (see `tests/conftest.py`)

#### MinIO Connection Refused in CI

**Cause:** MinIO service container isn't ready before tests run.

**Solution:**
- The workflow includes a health check and readiness verification
- If this persists, increase wait time in `.github/workflows/pytest.yml`
- Check GitHub Actions logs for MinIO startup errors

#### Production App Can't Access GCS

**Cause:** GCS HMAC credentials are missing or invalid in Cloud Run environment.

**Solution:**
1. Verify secrets exist in Secret Manager: `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`
2. Verify Cloud Run service is configured to inject these as environment variables
3. Check Cloud Run logs for authentication errors
4. Test credentials locally by temporarily setting them in `.devcontainer/.env`

### Development Container

This project uses VS Code devcontainers for consistent development environments. The container includes:

- Python runtime and dependencies
- Pre-configured tools (mypy, ruff, pytest)
- GitHub CLI
- Common Linux utilities

You will need:

- Docker (the Docker engine and optionally Docker desktop for convenience)
- VSCode with the `Dev Containers` extension installed (or another IDE / library that respects the `devcontainer.json` standard)

To use the devcontainer:
1. Open the project in VS Code
2. Use `Cmd + Shift + P` -> `Dev Containers: Reopen in Container`
3. All tools are pre-installed and configured

To rebuild the devcontainer (for reproducibility, or when making / validating environment changes)
- `Cmd + Shift + P` -> `Dev Containers: Rebuild Container`

The above will reuse unchanged docker build layers, to speed up the build (changing only what Docker views as new, and anything _lower_ in the build instructions). It will take longer, but if you want to be _really_ sure you are getting a totally clean and reproducible environment, you can run:
- `Cmd + Shift + P` -> `Dev Containers: Rebuild Container Without Cache`


### Pre-commit Workflow

Before committing code, ensure:

```bash
pixi run ruff-fix    # Fix formatting and auto-fixable issues
pixi run mypy        # Verify type checking passes
pixi run pytest     # Ensure all tests pass
```