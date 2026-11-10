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

### CI/CD Pipeline

The repository uses a unified CI/CD pipeline defined in `.github/workflows/ci-cd.yml`.

**Pipeline Stages:**

1. **Quality Checks** (parallel execution, on pushes and on PRs against any branch, so every
   PR in a stack is checked):
   - MyPy type checking
   - Ruff linting and formatting
   - Pytest (all tests including contract tests)
   - A build of `deployment/fastapi.Dockerfile`, not pushed

2. **Schema Publishing** (after quality checks pass):
   - Generates OpenAPI schema
   - Uploads to GCS for frontend consumption
   - Only runs on `dev` and `main` branches

3. **Deployment** (after schema publishing):
   - `dev`: builds the image once and pushes it to GHCR, where the analysis Jobs pull it, and
     to Artifact Registry. Cloud Run deploys it by digest, and the API starts Jobs with the
     same digest (`SEVERITY_IMAGE`).
   - `main`: builds via Cloud Build (unchanged until prod moves to v2)
   - Deploys to Cloud Run
   - `dev` branch: Auto-deploys to dev environment
   - `main` branch: Requires manual approval for production
   - One deploy per branch runs at a time; a later push waits for the earlier one

**Security:**
- Pull requests only run quality checks (no deployment, no secrets)
- Secrets are isolated using GitHub environments
- Failed tests automatically block deployment

**GitHub Environments:**
- `dev` environment: Auto-deploy, no approval required
- `main` environment: Requires 1+ approvals for production deployment

**Triggering Deployments:**
- Push to `dev` branch: Automatically deploys to dev after tests pass
- Push to `main` branch: Automatically publishes schema, waits for approval to deploy to production
- Pull requests: Only run quality checks

### Running v2 locally

The API and the analysis job are the same code. Locally, the API starts each job as a child
process instead of a Kubernetes Job (`SEVERITY_TARGET=local`, the default).

1. Start RustFS and create a bucket (see [Local storage with RustFS](#local-storage-with-rustfs)
   below), and put its `S3_*` settings in `.devcontainer/.env`.
2. Run the API:
   ```bash
   pixi run uvicorn src.app:app --port 8000
   ```
3. Submit an analysis (see the README), then poll the `status_url` it returns. The job reads
   real imagery from Element 84 or Planetary Computer and publishes to your bucket.

To run one job without the API, give it a canonical request:

```bash
pixi run python -m src.v2.run --request-file request.json
```

`request.json` is the canonical form (`src/v2/job_id.py`), for example a `canonical` entry
from `tests/fixtures/v2_job_id_vectors.json`. Set `SMTP_*` and `OPERATOR_EMAIL` to test mail;
without them the job skips it.

## Environment Variables & Secrets

### Overview

Storage is reached over the S3 protocol, so any S3-compatible endpoint works
(`src/v2/store.py`; `portolan push` uses the same settings):

- **Google Cloud Storage** through its S3 endpoint with HMAC keys, in production (Cloud Run and
  the analysis Jobs)
- **RustFS** in CI and for local development (it replaces MinIO, whose community image is
  deprecated)

### Storage variables (S3_*)

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `S3_ACCESS_KEY_ID` | Yes | Access key (an HMAC key on GCS) | — |
| `S3_SECRET_ACCESS_KEY` | Yes | Secret key | — |
| `S3_ENDPOINT` | No | Host and port, no scheme | `storage.googleapis.com` |
| `S3_SECURE` | No | `True` for HTTPS, `False` for a local HTTP endpoint | `True` |
| `S3_BUCKET` | No | Bucket | `fire-recovery-store` |
| `S3_REGION` | No | Region used to sign requests | `auto` |
| `S3_PUBLIC_BASE_URL` | No | Base of the public URLs in API responses | `{scheme}://{endpoint}/{bucket}` |

The defaults point at the **production** bucket. Set `S3_ENDPOINT` and `S3_BUCKET` explicitly
for anything else.

### Local storage with RustFS

Pixi's `s3` environment has RustFS from conda-forge, at the version CI runs, and the AWS CLI.
It is separate from the default environment, so neither reaches the image.

1. Start RustFS on `127.0.0.1:9000` and leave it running. Its data goes in `.rustfs/`.
   ```bash
   pixi run -e s3 s3-server
   ```

2. Create `dev-bucket`. Running it again does nothing.
   ```bash
   pixi run -e s3 s3-bucket
   ```

3. Point the app and the job at it, in `.devcontainer/.env`:
   ```dotenv
   S3_ENDPOINT=127.0.0.1:9000
   S3_SECURE=False
   S3_BUCKET=dev-bucket
   S3_REGION=us-east-1
   S3_ACCESS_KEY_ID=dev-access-key
   S3_SECRET_ACCESS_KEY=dev-secret-key
   ```

### Tests

`pixi run pytest` needs no storage. Two groups of tests are opt-in:

- **Network tests** read real STAC APIs and imagery: `pixi run pytest -m network`.
- **Storage tests** push a catalog with a COG over 5 MiB to a real S3-compatible store. They
  run only with `RUN_STORAGE_TESTS=1` and an explicit `S3_ENDPOINT`, and refuse the production
  bucket. With RustFS running and `dev-bucket` created (above), this sets both and runs them:
  ```bash
  pixi run storage-test
  ```

pytest loads `.devcontainer/test.env` and `.devcontainer/.env` if they exist (`pytest.ini`).

### CI/CD (GitHub Actions)

The `pytest` job runs RustFS as a service container, creates `test-bucket` with the AWS CLI,
and sets the `S3_*` variables and `RUN_STORAGE_TESTS=1` for the whole job, so the storage tests
run on every PR.

### Production (Cloud Run)

The API reads the result records with GCS HMAC credentials, injected as `S3_ACCESS_KEY_ID` and
`S3_SECRET_ACCESS_KEY` from Secret Manager. Analysis Jobs get the same variables from the
`severity-v2` Secret in their namespace.

### Security Best Practices

1. **Never commit `.env` or `test.env` files.** `.devcontainer/.env` and
   `.devcontainer/test.env` are in `.gitignore`; check `git status` before committing.
2. **Develop against RustFS, not the production bucket.** Local credentials meant for
   production are dangerous with the production defaults above; the storage tests refuse the
   production bucket for that reason.
3. **Store real credentials securely,** in a password manager, never in chat or email.

#### Credential Rotation

**Local RustFS credentials** are fixed in `pixi.toml` and local-only: RustFS listens on
`127.0.0.1`.

**GCS HMAC credentials**, if exposed:
1. **Immediately** delete the exposed HMAC key in the
   [Google Cloud Console](https://console.cloud.google.com/storage/settings;tab=interoperability).
2. Generate a new HMAC key.
3. Update it in your local `.devcontainer/.env`, in Secret Manager for Cloud Run, and in each
   cluster's `severity-v2` Secret.
4. If it was committed to git, rotate before purging history (for example with
   `git-filter-repo`), and treat the repository as compromised.

### Troubleshooting

- **"Set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY"**: the process has no storage
  credentials. Add them to `.devcontainer/.env` locally, or check the Secret in the cluster.
- **Storage tests skip**: set `RUN_STORAGE_TESTS=1` and `S3_ENDPOINT`, and use a bucket other
  than `fire-recovery-store`.
- **RustFS not ready in CI**: the workflow waits up to a minute for it to answer; check the
  service container's log in the run.

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