"""The fixed-ramp index thumbnails."""

from pathlib import Path
from typing import Dict

import numpy as np
import rasterio

from matplotlib import colormaps

from src.v2.catalog.portolan_plugin import RENDER_COLORMAP
from src.v2.raster.indices import INDICES
from src.v2.catalog.thumbnail import COLORMAP, MAX_SIZE, colorize, render_thumbnail


def test_colorize_ramp_ends_and_nodata() -> None:
    rgb = colorize(
        np.array([[-0.2, 0.0, 0.5, 0.9, np.nan]], dtype=np.float32), 0.0, 0.5
    )
    assert rgb.shape == (3, 1, 5) and rgb.dtype == np.uint8
    first = colormaps[COLORMAP](0.0, bytes=True)[:3]
    last = colormaps[COLORMAP](1.0, bytes=True)[:3]
    assert (rgb[:, 0, 0] == first).all() and (rgb[:, 0, 1] == first).all()
    assert (rgb[:, 0, 2] == last).all() and (rgb[:, 0, 3] == last).all()
    assert (rgb[:, 0, 4] == 255).all()


def test_thumbnails_use_the_ramp_the_map_renders_with() -> None:
    # rio-tiler (TiTiler) names matplotlib's colormaps in lower case.
    assert COLORMAP.lower() == RENDER_COLORMAP


def test_render_thumbnail(tmp_path: Path, index_cogs: Dict[str, Path]) -> None:
    out = render_thumbnail(index_cogs["rbr"], tmp_path / "rbr.thumb.jpg", (0.0, 0.5))
    with rasterio.open(out) as ds:
        assert ds.driver == "JPEG"
        assert ds.count == 3
        assert max(ds.width, ds.height) <= MAX_SIZE
    assert not (tmp_path / "rbr.thumb.jpg.aux.xml").exists()


def test_every_index_has_an_increasing_display_range() -> None:
    for index in INDICES:
        low, high = index.display_range
        assert low < high, index.name
