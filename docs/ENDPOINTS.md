# Fire Recovery Backend API Endpoints

The generated OpenAPI schema is the API contract: `GET /openapi.json`, or the interactive docs
at `/docs`. This page summarizes it. For development setup, see [DEVELOPING.md](DEVELOPING.md).
The design behind the API is [severity v2](../smrt-design/severity-v2-2026-11.md).

## Base URL

- **Local development:** `http://localhost:8000`
- **Dev:** `https://fire-recovery-backend-dev-113009620257.us-central1.run.app`

## GET /healthz

Liveness. It touches no storage and no cluster, so it answers whenever the process is up.

```json
{"status": "ok"}
```

## GET /version

What this deployment runs. It does no I/O.

```json
{"pipeline_version": 1, "git_sha": "<commit>", "image": "ghcr.io/schmidtdse/fire-recovery-backend@sha256:<digest>"}
```

`pipeline_version` is part of every job id. Record it beside results for provenance.
`git_sha` and `image` are `null` when the deployment doesn't set them.
