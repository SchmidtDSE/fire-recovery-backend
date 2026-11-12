"""Search, composite and COGs for one canonical request (design §2, steps 1-7)."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import odc.stac

from src.models.provenance import Provenance, Scene, SourceData
from src.stac.stac_endpoint_handler import StacEndpointHandler
from src.v2.raster.cog import write_cog
from src.v2.raster.compute import WindowScenes, compute_rasters
from src.v2.identity.grid import Grid, raster_grid
from src.v2.identity.job_id import CanonicalRequest
from src.v2.raster.load import window_scaling

logger = logging.getLogger(__name__)

# GDAL retries transient HTTP failures (429, 5xx) on each read, a bounded
# number of times; anything else fails the job.
GDAL_HTTP_MAX_RETRY = 5
GDAL_HTTP_RETRY_DELAY_SECONDS = 2


@dataclass(frozen=True)
class ComputeResult:
    """The output grid, one COG per index, and what they were computed from."""

    grid: Grid
    cogs: Dict[str, Path]
    provenance: Provenance


def compute(
    canonical: CanonicalRequest,
    workdir: Path,
    threads: int,
    handler: Optional[StacEndpointHandler] = None,
) -> ComputeResult:
    """Compute every index for a canonical request into COGs under ``workdir``.

    Raises:
        RuntimeError: if no provider has scenes in both windows.
        ValueError: if a window mixes reflectance scalings.
    """
    grid = raster_grid(canonical.tiles, canonical.sensor)
    handler = handler or StacEndpointHandler()
    search = handler.search_windows(
        grid.footprint(),
        [list(canonical.prefire_window), list(canonical.postfire_window)],
        canonical.sensor,
    )
    pre_items, post_items = search.windows
    prefire = WindowScenes(
        pre_items, *window_scaling(pre_items, search.provider, canonical.sensor)
    )
    postfire = WindowScenes(
        post_items, *window_scaling(post_items, search.provider, canonical.sensor)
    )
    odc.stac.configure_rio(
        cloud_defaults=True,
        GDAL_HTTP_MAX_RETRY=GDAL_HTTP_MAX_RETRY,
        GDAL_HTTP_RETRY_DELAY=GDAL_HTTP_RETRY_DELAY_SECONDS,
    )
    raws = compute_rasters(
        prefire,
        postfire,
        search.provider,
        canonical.sensor,
        grid,
        workdir / "raw",
        threads,
    )
    cog_dir = workdir / "cogs"
    cog_dir.mkdir(parents=True, exist_ok=True)
    cogs = {}
    for name, raw in raws.items():
        cogs[name] = write_cog(raw, cog_dir / f"{name}.tif")
        raw.unlink()
    provenance = Provenance(
        source=SourceData.from_mapping(search.provider, canonical.sensor),
        prefire_scenes=[Scene.from_item(item) for item in pre_items],
        postfire_scenes=[Scene.from_item(item) for item in post_items],
    )
    logger.info(
        f"Computed {sorted(cogs)} on a {grid.width}x{grid.height} grid from "
        f"{len(pre_items)} prefire and {len(post_items)} postfire scenes"
    )
    return ComputeResult(grid=grid, cogs=cogs, provenance=provenance)
