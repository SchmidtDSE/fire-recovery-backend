"""The provenance sidecar the portolan plugin copies into the STAC (design §5).

The job writes one JSON file per analysis, outside the catalog directory, and
names it in FIRE_SEVERITY_SIDECAR. Every value in it is a function of the
cache key or of what the job computed, never of the submitted perimeter.
"""

import json
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import h3
import rasterio

from src.models.provenance import Provenance
from src.util.date_windows import window_bounds
from src.util.geo_ops import Bounds
from src.v2.identity.grid import Grid, metric_size
from src.v2.identity.job_id import CanonicalRequest
from src.v2.sensors import SENSOR_MASKS

SIDECAR_ENV = "FIRE_SEVERITY_SIDECAR"

# The H3 level is the finest at which this many cells' average area covers the
# raster, so a typical analysis lists 15-25 cells whatever its size.
H3_TARGET_CELLS = 25
H3_MAX_RESOLUTION = 15

COMPOSITE = "median"
STATISTICS = ("minimum", "maximum", "mean", "stddev", "valid_percent")


def h3_cover(footprint: Bounds) -> Tuple[int, List[str]]:
    """The H3 level and the sorted cells overlapping an EPSG:4326 box."""
    width, height = metric_size(footprint)
    area = width * height
    resolution = max(
        (
            r
            for r in range(H3_MAX_RESOLUTION + 1)
            if h3.average_hexagon_area(r, "m^2") * H3_TARGET_CELLS >= area
        ),
        default=0,
    )
    west, south, east, north = footprint
    polygon = h3.LatLngPoly(
        [(south, west), (south, east), (north, east), (north, west)]
    )
    cells = h3.h3shape_to_cells_experimental(polygon, resolution, contain="overlap")
    return resolution, sorted(cells)


def band_metadata(cog: Path) -> Dict[str, Any]:
    """Data type, nodata and the statistics GDAL embedded in a COG."""
    with rasterio.open(cog) as ds:
        tags = ds.tags(1)
        return {
            "data_type": ds.dtypes[0],
            "nodata": ds.nodata,
            "statistics": {
                name: float(tags[f"STATISTICS_{name.upper()}"]) for name in STATISTICS
            },
        }


def utc_text(value: Any) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_sidecar(
    canonical: CanonicalRequest,
    job_id: str,
    grid: Grid,
    provenance: Provenance,
    cogs: Dict[str, Path],
) -> Dict[str, Any]:
    """Item properties and per-band metadata for one analysis."""
    start, _ = window_bounds(*canonical.prefire_window)
    _, end = window_bounds(*canonical.postfire_window)
    resolution, cells = h3_cover(grid.footprint())
    mask = SENSOR_MASKS[canonical.sensor]
    properties: Dict[str, Any] = {
        "start_datetime": utc_text(start),
        "end_datetime": utc_text(end - timedelta(seconds=1)),
        "fire:event_name": canonical.fire_event_name,
        "fire:job_id": job_id,
        "fire:pipeline_version": canonical.pipeline_version,
        "fire:sensor": canonical.sensor,
        "fire:composite": COMPOSITE,
        "fire:prefire_window": list(canonical.prefire_window),
        "fire:postfire_window": list(canonical.postfire_window),
        "fire:grid": {**grid.model_dump(), "tiles": canonical.tiles.model_dump()},
        "fire:h3": {"res": resolution, "cells": cells},
        "fire:mask": {
            "band": provenance.source.bands["mask"],
            "kind": mask.kind,
            "values": list(mask.values),
            "description": mask.description,
        },
        **provenance.to_stac_properties(),
    }
    return {
        "properties": properties,
        "bands": {name: [band_metadata(path)] for name, path in cogs.items()},
    }


def write_sidecar(sidecar: Dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sidecar, indent=1))
    return path
