# Fire Recovery Backend API Endpoints

The generated OpenAPI schema is the API contract: `GET /openapi.json`, or the interactive docs
at `/docs`. This page summarizes it. For development setup, see [DEVELOPING.md](DEVELOPING.md).
The design behind the API is [severity v2](../smrt-design/severity-v2-2026-11.md).

## Base URL

- **Local development:** `http://localhost:8000`
- **Dev:** `https://fire-recovery-backend-dev-113009620257.us-central1.run.app`

## GET /health

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

## POST /v2/severity

Submit an analysis: a fire perimeter, two date windows and, optionally, an email address.

```json
{
  "fire_event_name": "Geology_Fire",
  "geometry": {"type": "Polygon", "coordinates": [[[-116.098, 33.930], [-116.098, 33.881], [-116.019, 33.881], [-116.019, 33.930], [-116.098, 33.930]]]},
  "prefire_window": ["2023-06-01", "2023-06-09"],
  "postfire_window": ["2023-06-17", "2023-06-22"],
  "sensor": "sentinel-2",
  "email": "optional@example.org"
}
```

- `fire_event_name` matches `^[A-Za-z0-9_-]+$`, at most 128 characters.
- Each window is `[start, end]`; the end date includes its whole day. Windows are at most 90
  days, and the prefire window ends before the postfire window starts.
- `geometry` is a Polygon, a MultiPolygon or a Feature. Only its bounds are used: they are
  buffered and snapped to WebMercatorQuad tiles, and requests that snap to the same tiles (with
  the same name, windows and sensor) share one job id and one analysis.

Responses:

| Status | Meaning |
|---|---|
| `200` | Already computed: `{job_id, state: "succeeded", grid, catalog_url, assets}` |
| `202` | Queued or running: `{job_id, state, grid, status_url}`. Poll `status_url`. |
| `422` | Invalid request, or the buffered area exceeds 100 Mpx at the sensor's resolution |
| `429` | Too many analyses running, or too many new ones from this address; see `Retry-After`. An `X-API-Key` lifts the per-address limit. |
| `503` | Storage or job dispatch unavailable |

The email, if given, goes to the requester whose submission starts the job. It is never stored.

## GET /v2/severity/{job_id}

The analysis's `state` (`pending`, `running`, `succeeded` or `failed`), its `catalog_url` and
`assets` when done, its `scene_counts`, and its `error` when failed. A failure reason may come
from the job itself or from the platform (for example `OOMKilled`, or a deadline). Submitting
the same request again reruns a failed analysis. `404` means no such analysis.

Each analysis is a self-contained Portolan catalog under `v2/fires/{fire_id}/` in the bucket.
