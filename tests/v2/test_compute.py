"""The block loop matches whole-array compute, block sizes stay in budget."""

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import numpy as np
import pytest
import rasterio
import xarray as xr
from odc.geo.geobox import GeoBox

from src.stac.stac_endpoint_handler import StacProviderConfig
from src.v2.compute import (
    BLOCK_BUDGET_BYTES,
    BYTES_PER_SAMPLE,
    NODATA,
    WindowScenes,
    block_side,
    blocks,
    compute_rasters,
)
from src.v2.grid import Grid
from src.v2.indices import INDICES, Composites
from src.v2.load import geobox

PROVIDER = StacProviderConfig.load_from_file().get_providers("sentinel-2")[0]
GRID = Grid(epsg=32611, resolution=20, bounds=(580000, 3750000, 583400, 3753000))
SCALE, OFFSET = 1e-4, -0.1


@pytest.mark.parametrize("days, side", [(1, 2048), (11, 1024), (48, 512), (5000, 256)])
def test_block_side(days: int, side: int) -> None:
    assert block_side(days) == side
    if side > 256:
        assert days * side**2 * BYTES_PER_SAMPLE <= BLOCK_BUDGET_BYTES


def test_blocks_tile_the_grid_exactly_once() -> None:
    covered = np.zeros((300, 470), dtype=int)
    for b in blocks(300, 470, 128):
        covered[b.row : b.row + b.height, b.col : b.col + b.width] += 1
    assert (covered == 1).all()


def synthetic_window(days: int, seed: int) -> Dict[str, np.ndarray]:
    """A stack over the full grid with cloud, nodata and an all-cloud corner."""
    rng = np.random.default_rng(seed)
    shape = (days, GRID.height, GRID.width)
    nir = rng.integers(1500, 4500, shape).astype(np.uint16)
    swir = rng.integers(800, 3500, shape).astype(np.uint16)
    scl = np.full(shape, 4, dtype=np.uint8)
    scl[rng.random(shape) < 0.3] = 9  # cloud
    nir[rng.random(shape) < 0.05] = 0  # nodata
    scl[:, :10, :12] = 8  # never clear
    return {"nir08": nir, "swir22": swir, "scl": scl}


WINDOWS = {"pre": synthetic_window(5, 1), "post": synthetic_window(7, 2)}


def fake_loader(window: str) -> Any:
    full = geobox(GRID)

    def load(
        items: Any, provider: Any, sensor: str, block: GeoBox, threads: int
    ) -> Optional[xr.Dataset]:
        # Locate the block in the full grid from its top-left corner.
        col = int((block.affine.c - full.affine.c) / GRID.resolution)
        row = int((full.affine.f - block.affine.f) / GRID.resolution)
        rows, cols = slice(row, row + block.shape.y), slice(col, col + block.shape.x)
        return xr.Dataset(
            {
                k: (("time", "y", "x"), v[:, rows, cols])
                for k, v in WINDOWS[window].items()
            }
        )

    return load


def fake_items(window: str, days: int) -> List[Any]:
    return [
        SimpleNamespace(
            id=window, datetime=datetime(2023, 6, 1 + d, tzinfo=timezone.utc)
        )
        for d in range(days)
    ]


def whole_array_medians(window: str) -> tuple[np.ndarray, np.ndarray]:
    data = WINDOWS[window]
    ok = (
        (data["nir08"] > 0)
        & (data["swir22"] > 0)
        & ~np.isin(data["scl"], [0, 1, 3, 8, 9, 10, 11])
    )
    out = []
    for band in ("nir08", "swir22"):
        refl = data[band].astype(np.float32) * np.float32(SCALE) + np.float32(OFFSET)
        refl[~ok] = np.nan
        with np.errstate(all="ignore"), pytest.warns(RuntimeWarning):
            out.append(np.nanmedian(refl, axis=0).astype(np.float32))
    return out[0], out[1]


def test_block_loop_matches_whole_array(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"n": 0}
    loaders = {"pre": fake_loader("pre"), "post": fake_loader("post")}

    def load_block(items: Any, *args: Any) -> Any:
        calls["n"] += 1
        return loaders[items[0].id](items, *args)

    monkeypatch.setattr("src.v2.compute.load_block", load_block)
    monkeypatch.setattr("src.v2.compute.block_side", lambda days: 64)
    pre = WindowScenes(items=fake_items("pre", 5), scale=SCALE, offset=OFFSET)
    post = WindowScenes(items=fake_items("post", 7), scale=SCALE, offset=OFFSET)
    paths = compute_rasters(
        pre, post, PROVIDER, "sentinel-2", GRID, tmp_path, threads=1
    )

    expected = Composites.from_medians(
        *whole_array_medians("pre"), *whole_array_medians("post")
    )
    n_blocks = len(list(blocks(GRID.height, GRID.width, 64)))
    assert calls["n"] == 2 * n_blocks
    for index in INDICES:
        with rasterio.open(paths[index.name]) as ds:
            assert ds.descriptions == (index.name,)
            assert ds.nodata == NODATA
            assert ds.transform == GRID.transform()
            written = ds.read(1)
        want = index.compute(expected)
        want[~np.isfinite(want)] = NODATA
        np.testing.assert_allclose(written, want, rtol=1e-6)
        assert (written[:10, :12] == NODATA).all()


def test_block_without_scenes_is_nodata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("src.v2.compute.load_block", lambda *args: None)
    scenes = WindowScenes(items=[], scale=SCALE, offset=OFFSET)
    paths = compute_rasters(
        scenes, scenes, PROVIDER, "sentinel-2", GRID, tmp_path, threads=1
    )
    with rasterio.open(paths["rbr"]) as ds:
        assert (ds.read(1) == NODATA).all()
