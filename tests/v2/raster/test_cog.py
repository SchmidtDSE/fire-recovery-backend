"""COGs carry overviews, statistics and nodata."""

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from src.v2.raster.cog import overview_count, write_cog


@pytest.mark.parametrize(
    "height, width, levels",
    [(512, 512, 0), (410, 513, 1), (100, 3000, 3), (2048, 1024, 2)],
)
def test_overview_count_uses_the_longer_side(
    height: int, width: int, levels: int
) -> None:
    assert overview_count(height, width) == levels


def test_write_cog(tmp_path: Path) -> None:
    raw = tmp_path / "rbr_raw.tif"
    data = np.random.default_rng(0).normal(0.1, 0.05, (300, 1100)).astype(np.float32)
    data[:50] = -9999.0
    with rasterio.open(
        raw,
        "w",
        driver="GTiff",
        width=1100,
        height=300,
        count=1,
        dtype="float32",
        nodata=-9999.0,
        crs="EPSG:32611",
        transform=from_origin(580000, 3753000, 20, 20),
        tiled=True,
        blockxsize=512,
        blockysize=512,
    ) as dst:
        dst.write(data, 1)
        dst.set_band_description(1, "rbr")

    cog = write_cog(raw, tmp_path / "rbr.tif")
    with rasterio.open(cog) as ds:
        assert ds.tags(ns="IMAGE_STRUCTURE").get("LAYOUT") == "COG"
        assert ds.block_shapes[0] == (512, 512)
        assert len(ds.overviews(1)) == 2
        assert ds.nodata == -9999.0
        assert ds.descriptions == ("rbr",)
        tags = ds.tags(1)
        assert float(tags["STATISTICS_VALID_PERCENT"]) == pytest.approx(
            83.333, abs=0.01
        )
        assert float(tags["STATISTICS_MEAN"]) == pytest.approx(0.1, abs=0.01)
