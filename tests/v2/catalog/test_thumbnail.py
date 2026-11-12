"""The fixed-ramp RBR thumbnail."""

from pathlib import Path
from typing import Dict

import numpy as np
import rasterio

from src.v2.catalog.thumbnail import MAX_SIZE, RAMP_COLORS, colorize, render_thumbnail


def test_colorize_ramp_ends_and_nodata() -> None:
    rgb = colorize(np.array([[-0.2, 0.0, 0.5, 0.9, np.nan]], dtype=np.float32))
    assert rgb.shape == (3, 1, 5) and rgb.dtype == np.uint8
    first, last = RAMP_COLORS[0].astype(np.uint8), RAMP_COLORS[-1].astype(np.uint8)
    assert (rgb[:, 0, 0] == first).all() and (rgb[:, 0, 1] == first).all()
    assert (rgb[:, 0, 2] == last).all() and (rgb[:, 0, 3] == last).all()
    assert (rgb[:, 0, 4] == 255).all()


def test_render_thumbnail(tmp_path: Path, index_cogs: Dict[str, Path]) -> None:
    out = render_thumbnail(index_cogs["rbr"], tmp_path / "rbr.thumb.jpg")
    with rasterio.open(out) as ds:
        assert ds.driver == "JPEG"
        assert ds.count == 3
        assert max(ds.width, ds.height) <= MAX_SIZE
    assert not (tmp_path / "rbr.thumb.jpg.aux.xml").exists()
