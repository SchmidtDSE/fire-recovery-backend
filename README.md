# Fire Recovery Backend

A FastAPI service for fire severity analysis, boundary refinement, and vegetation impact assessment using Sentinel-2 satellite imagery.

Part of the [DSE Disturbance Toolbox](https://dse-disturbance-toolbox.org/tools/disturbance-severity/methodology/).

## What It Does

This API processes satellite imagery to help assess wildfire impacts:

1. **Fire Severity Analysis** - Calculates burn severity indices (NBR, dNBR, RdNBR, RBR) from Sentinel-2 imagery
2. **Boundary Refinement** - Crops analysis to user-drawn boundaries for precise impact areas
3. **Vegetation Impact** - Generates statistics on fire effects across vegetation communities

### Why RBR?

The API calculates multiple severity metrics, but **RBR (Relativized Burn Ratio)** is recommended for most use cases, especially in low-biomass environments like deserts. A fire that completely consumes sparse desert vegetation registers as "moderate" on dNBR scales calibrated for forests, but correctly registers as "high severity" on RBR.

## Quick Start

```bash
# Fire severity analysis
curl -X POST "http://localhost:8000/fire-recovery/process/analyze_fire_severity" \
  -H "Content-Type: application/json" \
  -d '{
    "fire_event_name": "Geology_Fire",
    "coarse_geojson": {"type": "Polygon", "coordinates": [[[-116.098276, 33.929925], [-116.098276, 33.880794], [-116.019318, 33.880794], [-116.019318, 33.929925], [-116.098276, 33.929925]]]},
    "prefire_date_range": ["2023-06-01", "2023-06-09"],
    "postfire_date_range": ["2023-06-17", "2023-06-22"]
  }'

# Poll for results
curl "http://localhost:8000/fire-recovery/result/analyze_fire_severity/Geology_Fire/{job_id}"
```

## Documentation

| Document | Description |
|----------|-------------|
| [docs/ENDPOINTS.md](docs/ENDPOINTS.md) | Complete API reference with curl examples |
| [docs/DESIGN.md](docs/DESIGN.md) | Architecture and design patterns |
| [docs/DEVELOPING.md](docs/DEVELOPING.md) | Development setup, testing, and CI/CD |

## API Overview

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/fire-recovery/process/analyze_fire_severity` | POST | Start fire severity analysis |
| `/fire-recovery/result/analyze_fire_severity/{name}/{job_id}` | GET | Get severity analysis results |
| `/fire-recovery/process/refine` | POST | Refine boundary and crop COGs |
| `/fire-recovery/result/refine/{name}/{job_id}` | GET | Get refinement results |
| `/fire-recovery/process/resolve_against_veg_map` | POST | Analyze vegetation impacts |
| `/fire-recovery/result/resolve_against_veg_map/{name}/{job_id}` | GET | Get vegetation analysis results |
| `/fire-recovery/upload/geojson` | POST | Upload GeoJSON boundary |
| `/fire-recovery/upload/shapefile` | POST | Upload zipped shapefile |
| `/fire-recovery/healthz` | GET | Health check |

Interactive API docs available at `/docs` (Swagger UI) or `/redoc` when running locally.

## Typical Workflow

```
1. POST /process/analyze_fire_severity  →  Get coarse severity COGs
2. [User views COGs, draws refined boundary]
3. POST /process/refine                 →  Get refined severity COGs
4. POST /process/resolve_against_veg_map →  Get vegetation impact statistics
```

## Output Products

- **Cloud Optimized GeoTIFFs (COGs)**: Pre/post NBR, dNBR, RdNBR, RBR rasters for web visualization
- **Boundary GeoJSON**: Coarse and refined fire perimeters
- **Vegetation Matrix**: CSV/JSON with hectares affected by severity class per vegetation type
- **STAC Metadata**: Catalog entries for all assets with spatial/temporal metadata

## Technology Stack

- **FastAPI** - Async Python web framework
- **Sentinel-2 L2A** - Satellite imagery via Microsoft Planetary Computer
- **Cloud Optimized GeoTIFF** - Web-optimized raster format
- **STAC** - SpatioTemporal Asset Catalog for metadata
- **Google Cloud Storage** - Asset storage (S3-compatible)

## Development

See [docs/DEVELOPING.md](docs/DEVELOPING.md) for:
- Pixi environment setup
- Running locally with MinIO
- Testing (pytest, contract tests)
- CI/CD pipeline
- Environment variables and secrets

## License

[Add license information]
