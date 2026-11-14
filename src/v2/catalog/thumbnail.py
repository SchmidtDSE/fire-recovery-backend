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
from rasterio.enums import Resampling
from rasterio.errors import NotGeoreferencedWarning

MAX_SIZE = 512
JPEG_QUALITY = 85
# ColorBrewer YlOrRd (5 classes): pale yellow at the low end, dark red at the high.
RAMP_COLORS = np.array(
    [[255, 255, 178], [254, 204, 92], [253, 141, 60], [240, 59, 32], [189, 0, 38]],
    dtype=np.float32,
)
NODATA_COLOR = np.array([255, 255, 255], dtype=np.float32)


def colorize(values: np.ndarray, low: float, high: float) -> np.ndarray:
    """(3, h, w) uint8 RGB for values stretched from low to high; NaN is white."""
    position = np.clip((values - low) / (high - low), 0.0, 1.0)
    stops = np.linspace(0.0, 1.0, len(RAMP_COLORS))
    filled = np.nan_to_num(position, nan=0.0)
    rgb = np.stack(
        [np.interp(filled, stops, RAMP_COLORS[:, channel]) for channel in range(3)]
    )
    rgb[:, np.isnan(values)] = NODATA_COLOR[:, None]
    return np.round(rgb).astype(np.uint8)


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
