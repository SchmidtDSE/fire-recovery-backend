"""Cloud-optimized GeoTIFFs from the intermediate rasters (design §2, step 7)."""

import math
from pathlib import Path

import rasterio
from rasterio.shutil import copy as rio_copy

# GDAL's block cache while building, in MiB: building reads from disk, so a
# small cache keeps memory flat at a modest cost in speed.
GDAL_CACHE_MB = 64
COG_BLOCK = 512


def overview_count(height: int, width: int, block: int = COG_BLOCK) -> int:
    """Overview levels until the longer side fits one block.

    Sized from the longer side, so a raster whose short side is under one
    block still gets overviews.
    """
    return max(0, math.ceil(math.log2(max(height, width) / block)))


def write_cog(source: Path, destination: Path) -> Path:
    """Convert a tiled GeoTIFF to a COG.

    DEFLATE with the floating-point predictor, 512 px blocks, AVERAGE
    overviews, and embedded statistics (min, max, mean, stddev, valid percent)
    that portolan copies into the STAC.
    """
    with rasterio.open(source) as src:
        levels = overview_count(src.height, src.width)
    with rasterio.Env(GDAL_CACHEMAX=GDAL_CACHE_MB):
        rio_copy(
            source,
            destination,
            driver="COG",
            COMPRESS="DEFLATE",
            PREDICTOR="YES",
            BLOCKSIZE=COG_BLOCK,
            RESAMPLING="AVERAGE",
            OVERVIEW_COUNT=levels,
            STATISTICS="YES",
        )
    return destination
