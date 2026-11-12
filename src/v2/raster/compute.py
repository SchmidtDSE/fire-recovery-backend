"""The bounded compute: per block, composite both windows and write every index.

Memory stays flat however large the fire or long the scene list (design §2).
The output grid is processed in square blocks sized so that one band of one
window, every day included, fits a fixed budget. Each block reads the prefire
scenes, takes the median of the clear observations, frees the reads, does the
same for the postfire scenes, computes every registered index, and writes it
into one tiled GeoTIFF per index on disk. Disk, not memory, holds the result.
"""

import logging
import math
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence

import numpy as np
import rasterio
from odc.geo.geobox import GeoBox
from pystac import Item
from rasterio.windows import Window

from src.stac.stac_endpoint_handler import StacMapping
from src.v2.identity.grid import Grid
from src.v2.raster.indices import INDICES, Composites, Index
from src.v2.raster.load import ScaledScenes, geobox, load_block
from src.v2.raster.mask import clear
from src.v2.sensors import SENSOR_MASKS

logger = logging.getLogger(__name__)

NODATA = np.float32(-9999.0)
# One band of one window over one block, every day included, at uint16.
# Measured on real Sentinel-2 data (4.6 Mpx, 45-day windows, 2 threads): 32 MiB
# peaks at about 0.5 GiB of process memory; 64 MiB peaked at 0.84 GiB.
BLOCK_BUDGET_BYTES = 32 * 2**20
BYTES_PER_SAMPLE = 2
BLOCK_STEP = 256
MIN_BLOCK = 256
MAX_BLOCK = 2048
# Rows per strip when taking medians: bounds the float32 copy and the sort.
MEDIAN_STRIP_ROWS = 128
# Internal tiling of the intermediate GeoTIFFs and of the final COGs.
TIFF_BLOCK = 512


@dataclass(frozen=True)
class Observations:
    """One scaling group's reads over a block: (time, y, x) DNs and clear flags."""

    nir: np.ndarray
    swir: np.ndarray
    ok: np.ndarray
    scale: float
    offset: float
    # Each time step's acquisition date, or None when the load gives no times.
    days: Optional[List[Any]]

    def reflectances(self, rows: slice) -> tuple[np.ndarray, np.ndarray]:
        """Float32 NIR and SWIR reflectance of a strip of rows, NaN where not clear.

        Clear means the flags are clear and both reflectances are above 0
        (design §2), so a band never enters a composite without the other.
        Nodata (DN 0) is 0 or below under either convention. Under the -0.1
        offset, DN 1 to 1000 are 0 or below too: dark pixels that would push
        NBR outside [-1, 1].
        """
        nir = self.nir[:, rows].astype(np.float32) * np.float32(self.scale)
        nir += np.float32(self.offset)
        swir = self.swir[:, rows].astype(np.float32) * np.float32(self.scale)
        swir += np.float32(self.offset)
        with np.errstate(invalid="ignore"):
            unclear = ~self.ok[:, rows] | ~(nir > 0) | ~(swir > 0)
        nir[unclear] = np.nan
        swir[unclear] = np.nan
        return nir, swir


@dataclass(frozen=True)
class Block:
    """A rectangle of output pixels: its offsets and size."""

    row: int
    col: int
    height: int
    width: int

    def window(self) -> Window:
        return Window(self.col, self.row, self.width, self.height)


def block_side(days: int) -> int:
    """The largest block side, in pixels, that keeps one band within budget."""
    side = math.sqrt(BLOCK_BUDGET_BYTES / (max(days, 1) * BYTES_PER_SAMPLE))
    return int(max(MIN_BLOCK, min(MAX_BLOCK, side // BLOCK_STEP * BLOCK_STEP)))


def acquisition_days(items: Sequence[Item]) -> int:
    """Distinct acquisition dates: the time steps a load groups the items into."""
    return len({item.datetime.date() for item in items if item.datetime is not None})


def loaded_days(window: Sequence[ScaledScenes]) -> int:
    """Time steps one band of a window holds in memory: each group loads its own."""
    return sum(acquisition_days(group.items) for group in window)


def blocks(height: int, width: int, side: int) -> Iterator[Block]:
    """The grid's blocks, row by row; edge blocks are clipped."""
    for row in range(0, height, side):
        for col in range(0, width, side):
            yield Block(row, col, min(side, height - row), min(side, width - col))


def window_medians(
    window: Sequence[ScaledScenes],
    provider: StacMapping,
    sensor: str,
    block: GeoBox,
    threads: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Median NIR and SWIR reflectance of the clear observations over a block.

    Each scaling group is read and converted with its own scale and offset.
    NaN where a pixel has no clear observation in the window.
    """
    shape = (block.shape.y, block.shape.x)
    observations = []
    for group in window:
        loaded = load_block(group.items, provider, sensor, block, threads)
        if loaded is None or loaded.sizes.get("time", 0) == 0:
            continue
        nir = loaded[provider.nir_band].values
        swir = loaded[provider.swir_band].values
        ok = clear(nir, swir, loaded[provider.mask_band].values, SENSOR_MASKS[sensor])
        days = (
            list(loaded["time"].values.astype("datetime64[D]"))
            if "time" in loaded.coords
            else None
        )
        del loaded
        observations.append(
            Observations(nir, swir, ok, group.scale, group.offset, days)
        )
    if not observations:
        empty = np.full(shape, np.nan, dtype=np.float32)
        return empty, empty.copy()
    return clear_median(observations)


def same_day_merged(
    observations: Sequence[Observations], rows: slice
) -> tuple[np.ndarray, np.ndarray]:
    """Every group's NIR and SWIR reflectance for a strip, one time step per
    acquisition date.

    One group passes through as loaded. When groups share a date (one overpass,
    adjacent tiles published under different scalings), their steps merge as
    odc-stac's solar-day grouping merges tiles: the first clear observation
    wins, in group order. A tile overlap then counts once, not twice.
    """
    if len(observations) == 1:
        return observations[0].reflectances(rows)
    by_day: Dict[Any, tuple[np.ndarray, np.ndarray]] = {}
    for number, obs in enumerate(observations):
        nir, swir = obs.reflectances(rows)
        for step in range(nir.shape[0]):
            day = obs.days[step] if obs.days is not None else (number, step)
            if day in by_day:
                merged_nir, merged_swir = by_day[day]
                # Both bands are NaN together, so NIR marks the gaps.
                gap = np.isnan(merged_nir)
                merged_nir[gap] = nir[step][gap]
                merged_swir[gap] = swir[step][gap]
            else:
                by_day[day] = (nir[step], swir[step])
    steps = list(by_day.values())
    return np.stack([s[0] for s in steps]), np.stack([s[1] for s in steps])


def clear_median(
    observations: Sequence[Observations],
) -> tuple[np.ndarray, np.ndarray]:
    """Per-pixel median NIR and SWIR reflectance over time of the clear
    observations.

    Works through rows in strips, so the float32 copies and the median's sort
    stay small however large the block. NaN where a pixel has no clear
    observation.
    """
    _, height, width = observations[0].nir.shape
    nir_out = np.empty((height, width), dtype=np.float32)
    swir_out = np.empty((height, width), dtype=np.float32)
    for top in range(0, height, MEDIAN_STRIP_ROWS):
        rows = slice(top, top + MEDIAN_STRIP_ROWS)
        nir, swir = same_day_merged(observations, rows)
        with warnings.catch_warnings():
            # An all-NaN pixel is a pixel with no clear observation.
            warnings.simplefilter("ignore", RuntimeWarning)
            nir_out[rows] = np.nanmedian(nir, axis=0)
            swir_out[rows] = np.nanmedian(swir, axis=0)
    return nir_out, swir_out


def compute_rasters(
    prefire: Sequence[ScaledScenes],
    postfire: Sequence[ScaledScenes],
    provider: StacMapping,
    sensor: str,
    grid: Grid,
    out_dir: Path,
    threads: int,
    indices: Sequence[Index] = INDICES,
) -> Dict[str, Path]:
    """Write one float32 GeoTIFF per index over the grid, block by block.

    NaN becomes NODATA, so the mask and the nodata value agree. Returns the
    paths by index name.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    full = geobox(grid)
    side = block_side(max(loaded_days(prefire), loaded_days(postfire)))
    profile = {
        "driver": "GTiff",
        "width": grid.width,
        "height": grid.height,
        "count": 1,
        "dtype": "float32",
        "nodata": float(NODATA),
        "crs": f"EPSG:{grid.epsg}",
        "transform": grid.transform(),
        "tiled": True,
        "blockxsize": TIFF_BLOCK,
        "blockysize": TIFF_BLOCK,
        "compress": "zstd",
    }
    paths = {index.name: out_dir / f"{index.name}_raw.tif" for index in indices}
    sinks = {name: rasterio.open(path, "w", **profile) for name, path in paths.items()}
    try:
        for index in indices:
            sinks[index.name].set_band_description(1, index.name)
        all_blocks = list(blocks(grid.height, grid.width, side))
        logger.info(
            f"Computing {grid.width}x{grid.height} px in {len(all_blocks)} block(s) "
            f"of up to {side} px"
        )
        for number, block in enumerate(all_blocks, start=1):
            sub = full[
                block.row : block.row + block.height,
                block.col : block.col + block.width,
            ]
            pre_nir, pre_swir = window_medians(prefire, provider, sensor, sub, threads)
            post_nir, post_swir = window_medians(
                postfire, provider, sensor, sub, threads
            )
            composites = Composites.from_medians(pre_nir, pre_swir, post_nir, post_swir)
            for index in indices:
                values = index.compute(composites)
                values[~np.isfinite(values)] = NODATA
                sinks[index.name].write(values, 1, window=block.window())
            logger.debug(f"Block {number}/{len(all_blocks)} written")
    finally:
        for sink in sinks.values():
            sink.close()
    return paths
