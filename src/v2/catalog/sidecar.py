"""The provenance sidecar the portolan plugin copies into the STAC (design §5).

The job writes one JSON file per analysis, outside the catalog directory, and
names it in FIRE_SEVERITY_SIDECAR. Every value in it is a function of the
cache key or of what the job computed, never of the submitted perimeter.
"""

import json
from datetime import timedelta
from functools import lru_cache
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import h3
import rasterio
import yaml

from src.models.provenance import Provenance
from src.util.date_windows import window_bounds
from src.util.geo_ops import Bounds
from src.v2.identity.grid import Grid, metric_size
from src.v2.identity.job_id import CanonicalRequest
from src.v2.sensors import SENSOR_MASKS
from src.v2.templates import render_paragraph

SIDECAR_ENV = "FIRE_SEVERITY_SIDECAR"

# The H3 level is the finest at which this many cells' average area covers the
# raster, so a typical analysis lists 15-25 cells whatever its size.
H3_TARGET_CELLS = 25
H3_MAX_RESOLUTION = 15

COMPOSITE = "median"
RESAMPLING = "nearest"
# The method in words: config/method.yaml (descriptions and citations) and
# config/templates/lineage.txt.j2 (the method statement).
METHOD_FILE = Path(__file__).resolve().parents[3] / "config" / "method.yaml"
LINEAGE_TEMPLATE = "lineage.txt.j2"

# Established STAC extensions for the method record (design §5). Scientific is
# in the Portolan profile's extension registry and rashid validates it offline;
# Processing isn't registered yet, so rashid notes it (PTL-EXT-002) without
# validating it.
PROCESSING_EXTENSION = "https://stac-extensions.github.io/processing/v1.2.0/schema.json"
SCIENTIFIC_EXTENSION = "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"

# The method record every published item must carry. The build checks it, so
# a change that drops one fails before anything is published.
METHOD_PROPERTIES = (
    "processing:lineage",
    "processing:software",
    "processing:version",
    "sci:publications",
    "fire:pipeline_version",
    "fire:composite",
    "fire:scene_selection",
    "fire:scaling",
    "fire:resampling",
    "fire:mask",
    "fire:grid",
    "fire:source",
    "fire:prefire_scenes",
    "fire:postfire_scenes",
)
SOFTWARE_PACKAGES = ("odc-stac", "numpy", "rasterio", "portolan-cli", "rashid")
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


def software_versions(git_sha: Optional[str]) -> Dict[str, str]:
    """This pipeline's commit and the versions of the libraries that shape the output."""
    versions = {"fire-recovery-backend": git_sha or "unknown"}
    for package in SOFTWARE_PACKAGES:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            continue
    versions["GDAL"] = rasterio.__gdal_version__
    return versions


@lru_cache(maxsize=1)
def method() -> Dict[str, Any]:
    """config/method.yaml: the descriptions and citations every item carries."""
    loaded: Dict[str, Any] = yaml.safe_load(METHOD_FILE.read_text())
    return loaded


def lineage(canonical: CanonicalRequest, grid: Grid, provenance: Provenance) -> str:
    """The method in plain language: processing:lineage and the catalog README."""
    source = provenance.source
    return render_paragraph(
        LINEAGE_TEMPLATE,
        sensor=canonical.sensor,
        provider=source.provider,
        collection=source.collection,
        prefire=canonical.prefire_window,
        postfire=canonical.postfire_window,
        bands=source.bands,
        mask_description=SENSOR_MASKS[canonical.sensor].description,
        composite=COMPOSITE,
        resampling=RESAMPLING,
        resolution=grid.resolution,
        epsg=grid.epsg,
        pipeline_version=canonical.pipeline_version,
    )


def missing_method_properties(properties: Dict[str, Any]) -> List[str]:
    """The METHOD_PROPERTIES absent or empty in an item's properties."""
    return [key for key in METHOD_PROPERTIES if not properties.get(key)]


def build_sidecar(
    canonical: CanonicalRequest,
    job_id: str,
    grid: Grid,
    provenance: Provenance,
    cogs: Dict[str, Path],
    git_sha: Optional[str] = None,
) -> Dict[str, Any]:
    """Item properties, per-band metadata and the extensions they use, for one
    analysis.

    Raises:
        ValueError: if the method record is incomplete.
    """
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
        "fire:scene_selection": method()["scene_selection"],
        "fire:scaling": method()["scaling"],
        "fire:resampling": RESAMPLING,
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
        "processing:lineage": lineage(canonical, grid, provenance),
        "processing:software": software_versions(git_sha),
        "processing:version": str(canonical.pipeline_version),
        "sci:publications": method()["publications"],
    }
    missing = missing_method_properties(properties)
    if missing:
        raise ValueError(f"The method record lacks {missing}")
    return {
        "properties": properties,
        "bands": {name: [band_metadata(path)] for name, path in cogs.items()},
        "stac_extensions": [PROCESSING_EXTENSION, SCIENTIFIC_EXTENSION],
    }


def write_sidecar(sidecar: Dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sidecar, indent=1))
    return path
