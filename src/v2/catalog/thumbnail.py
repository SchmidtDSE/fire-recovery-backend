"""Each index's thumbnail, on a fixed ramp (design §5, decision 11).

portolan draws a grayscale thumbnail per COG with a percentile stretch, which
makes an unburned scene look as dramatic as a burned one. A fixed ramp (the
index's ``display_range``) keeps thumbnails comparable across analyses.
portolan keeps a thumbnail that already exists, so these are drawn before
``portolan add`` runs.
"""

import warnings
from pathlib import Path
from typing import Tuple

import numpy as np
import rasterio
from matplotlib import colormaps
from matplotlib.colors import Normalize
from rasterio.enums import Resampling
from rasterio.errors import NotGeoreferencedWarning

MAX_SIZE = 512
JPEG_QUALITY = 85
# matplotlib's YlOrRd: the ramp the collection's renders name for TiTiler
# (portolan_plugin.RENDER_COLORMAP), so a thumbnail matches the map.
COLORMAP = "YlOrRd"
NODATA_COLOR = "white"


def colorize(values: np.ndarray, low: float, high: float) -> np.ndarray:
    """(3, h, w) uint8 RGB for values stretched from low to high; NaN is white."""
    ramp = colormaps[COLORMAP].with_extremes(bad=NODATA_COLOR)
    rgba = ramp(Normalize(low, high, clip=True)(values), bytes=True)
    return np.moveaxis(rgba[..., :3], -1, 0)


def render_thumbnail(
    cog: Path, destination: Path, display_range: Tuple[float, float]
) -> Path:
    """Draw a COG at most MAX_SIZE pixels on its longer side as a JPEG."""
    with rasterio.open(cog) as src:
        scale = min(1.0, MAX_SIZE / max(src.width, src.height))
        height = max(1, round(src.height * scale))
        width = max(1, round(src.width * scale))
        data = src.read(
            1, out_shape=(height, width), resampling=Resampling.average, masked=True
        )
    values = data.astype(np.float32).filled(np.nan)
    profile = {
        "driver": "JPEG",
        "width": width,
        "height": height,
        "count": 3,
        "dtype": "uint8",
        "quality": JPEG_QUALITY,
    }
    # A thumbnail is a picture, not a map; PAM off so no .aux.xml lands in
    # the catalog beside it.
    with warnings.catch_warnings(), rasterio.Env(GDAL_PAM_ENABLED="NO"):
        warnings.simplefilter("ignore", NotGeoreferencedWarning)
        with rasterio.open(destination, "w", **profile) as dst:
            dst.write(colorize(values, *display_range))
    return destination
