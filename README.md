# Fire Recovery Backend

A FastAPI service for wildfire burn severity analysis from Sentinel-2 or Landsat imagery.
Submit a fire perimeter and two date windows; get back a self-contained Portolan catalog of
dNBR, RdNBR and RBR.

Part of the [DSE Disturbance Toolbox](https://dse-disturbance-toolbox.org/tools/disturbance-severity/methodology/).

## What It Does

1. **Validates and keys the request.** The perimeter's buffered bounds snap to WebMercatorQuad
   tiles, so requests that snap to the same tiles (with the same name, windows and sensor)
   share one job id and one analysis.
2. **Runs one bounded Kubernetes Job per analysis**, on josh's GKE cluster or NRP Nautilus. It
   composites each window's cloud-masked scenes, using each product's own quality flags, and
   computes every index in blocks, so memory stays flat however large the fire.
3. **Publishes a Portolan catalog** per analysis, with the scenes, windows, grid and H3 cells it
   covers, validated by rashid. It writes a result record and, optionally, emails the
   requester.

### Why RBR?

The API calculates multiple severity metrics, but **RBR (Relativized Burn Ratio)** is
recommended for most use cases, especially in low-biomass environments like deserts. A fire
that completely consumes sparse desert vegetation registers as "moderate" on dNBR scales
calibrated for forests, but correctly registers as "high severity" on RBR.

## Quick Start

```bash
# Submit an analysis: 202 with a status URL, or 200 with links if it already exists
curl -X POST "http://localhost:8000/v2/severity" \
  -H "Content-Type: application/json" \
  -d '{
    "fire_event_name": "Geology_Fire",
    "geometry": {"type": "Polygon", "coordinates": [[[-116.098276, 33.929925], [-116.098276, 33.880794], [-116.019318, 33.880794], [-116.019318, 33.929925], [-116.098276, 33.929925]]]},
    "prefire_window": ["2023-06-01", "2023-06-09"],
    "postfire_window": ["2023-06-17", "2023-06-22"]
  }'

# Poll the status URL from the response
curl "http://localhost:8000/v2/severity/{job_id}"
```

## API

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v2/severity` | POST | Submit an analysis, or get the existing one for the same key |
| `/v2/severity/{job_id}` | GET | State, links when done, error when failed |
| `/healthz` | GET | Liveness, no I/O |
| `/version` | GET | Pipeline version, commit and Job image, no I/O |

The generated OpenAPI schema is the contract: `/openapi.json`, or the interactive docs at
`/docs`. [docs/ENDPOINTS.md](docs/ENDPOINTS.md) summarizes it.

## Documentation

| Document | Description |
|----------|-------------|
| [smrt-design/severity-v2-2026-11.md](smrt-design/severity-v2-2026-11.md) | The v2 design |
| [docs/ENDPOINTS.md](docs/ENDPOINTS.md) | API summary |
| [docs/DEVELOPING.md](docs/DEVELOPING.md) | Development setup, storage, testing and CI/CD |
| [deployment/k8s/README.md](deployment/k8s/README.md) | Provisioning a cluster for the analysis Jobs |

## Output Products

Each analysis is published under `v2/fires/{fire_id}/` as a Portolan catalog:

- **Cloud Optimized GeoTIFFs**: dNBR, RdNBR and RBR on the sensor's native UTM grid, with
  overviews and embedded statistics
- **STAC metadata**: the scenes, windows, grid, mask, provider and H3 cells, plus an
  `items.parquet` mirror for querying across fires
- **A thumbnail**: RBR on a fixed 0 to 0.5 ramp

## Technology Stack

- **FastAPI** on Cloud Run for the API; **Kubernetes Jobs** for the analyses
- **Sentinel-2 L2A and Landsat C2 L2** via Element 84 and Microsoft Planetary Computer, read
  with odc-stac
- **portolan-cli and rashid** to build and validate the catalogs
- **S3-compatible storage**: Google Cloud Storage in production, RustFS in CI and local
  development

## Development

See [docs/DEVELOPING.md](docs/DEVELOPING.md) for the Pixi environment, running the API and a
job locally, storage, tests and CI/CD.

## License

[Add license information]
