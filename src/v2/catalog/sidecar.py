"""The provenance sidecar the portolan plugin copies into the STAC (design §5).

The job writes one JSON file per analysis, outside the catalog directory, and
names it in FIRE_SEVERITY_SIDECAR. Every value in it is a function of the
cache key or of what the job computed, never of the submitted perimeter.
"""

import json
from datetime import timedelta
from importlib import metadata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
RESAMPLING = "nearest"
SCENE_SELECTION = (
    "Every scene in each window; where a provider lists one Sentinel-2 acquisition "
    "(mission, sensing start, orbit, tile) under several processings, only the "
    "newest processing baseline"
)
SCALING = (
    "Per scene: reflectance = DN * scale + offset, with the scene's own published "
    "raster:bands scale and offset (else the product's documented rule), before "
    "compositing"
)

# Established STAC extensions for the method record (design §5). Scientific is
# in the Portolan profile's extension registry and rashid validates it offline;
# Processing isn't registered yet, so rashid notes it (PTL-EXT-002) without
# validating it.
PROCESSING_EXTENSION = "https://stac-extensions.github.io/processing/v1.2.0/schema.json"
SCIENTIFIC_EXTENSION = "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"

PUBLICATIONS = [
    {
        "citation": (
            "Key, C.H., Benson, N.C. (2006). Landscape Assessment (LA): Sampling and "
            "analysis methods. In: Lutes, D.C., et al., FIREMON: Fire Effects "
            "Monitoring and Inventory System. Gen. Tech. Rep. RMRS-GTR-164-CD. USDA "
            "Forest Service, Rocky Mountain Research Station. LA-1-55. (dNBR)"
        ),
        "doi": "10.2737/RMRS-GTR-164",
    },
    {
        "citation": (
            "Miller, J.D., Thode, A.E. (2007). Quantifying burn severity in a "
            "heterogeneous landscape with a relative version of the delta "
            "Normalized Burn Ratio (dNBR). Remote Sensing of Environment 109, "
            "66-80. (RdNBR)"
        ),
        "doi": "10.1016/j.rse.2006.12.006",
    },
    {
        "citation": (
            "Parks, S.A., Dillon, G.K., Miller, C. (2014). A New Metric for "
            "Quantifying Burn Severity: The Relativized Burn Ratio. Remote Sensing "
            "6(3), 1827-1844. (RBR)"
        ),
        "doi": "10.3390/rs6031827",
    },
]

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


def lineage(canonical: CanonicalRequest, grid: Grid, provenance: Provenance) -> str:
    """The method in plain language: processing:lineage and the catalog README."""
    source = provenance.source
    mask = SENSOR_MASKS[canonical.sensor]
    pre, post = canonical.prefire_window, canonical.postfire_window
    return (
        f"Burn severity from {canonical.sensor} surface reflectance served by "
        f"{source.provider} (STAC collection {source.collection}). For each window "
        f"(prefire {pre[0]} to {pre[1]}, postfire {post[0]} to {post[1]}), every "
        f"scene covering the area is used, except that where a provider lists one "
        f"Sentinel-2 acquisition under several processings, only the newest "
        f"processing baseline is kept. Each scene's NIR ({source.bands['nir']}) and "
        f"SWIR ({source.bands['swir']}) digital numbers are converted to reflectance "
        f"with that scene's own published scale and offset (reflectance = DN x scale "
        f"+ offset). Observations with a zero digital number, or flagged in the "
        f"{source.bands['mask']} band ({mask.description}), are excluded, and the "
        f"per-pixel {COMPOSITE} of the remaining observations forms each window's "
        f"composite. Scenes are read with {RESAMPLING}-neighbour resampling onto a "
        f"{grid.resolution:g} m grid in EPSG:{grid.epsg}, aligned to the sensor's "
        f"native pixels. NBR = (NIR - SWIR) / (NIR + SWIR) per composite; dNBR, "
        f"RdNBR and RBR follow, each asset carrying its formula in "
        f"processing:expression. Pixels with no clear observation in either window "
        f"are nodata (-9999). The scenes used, with their processing baselines and "
        f"offsets, are listed in fire:prefire_scenes and fire:postfire_scenes. "
        f"Pipeline version {canonical.pipeline_version}."
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
        "fire:scene_selection": SCENE_SELECTION,
        "fire:scaling": SCALING,
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
        "sci:publications": PUBLICATIONS,
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
