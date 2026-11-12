"""Clear-observation masks from SCL and QA_PIXEL."""

import numpy as np
import pytest

from src.v2.raster.mask import clear, flagged
from src.v2.sensors import SENSOR_MASKS

S2 = SENSOR_MASKS["sentinel-2"]
LANDSAT = SENSOR_MASKS["landsat"]


@pytest.mark.parametrize("scl", range(12))
def test_scl_classes(scl: int) -> None:
    # 2 dark area, 4 vegetation, 5 bare soil, 6 water, 7 unclassified stay.
    expected = scl in (0, 1, 3, 8, 9, 10, 11)
    assert bool(flagged(np.array([scl], dtype=np.uint8), S2)[0]) is expected


@pytest.mark.parametrize(
    "qa, masked",
    [
        (21824, False),  # clear land: clear (6), low confidences (8, 10, 12, 14)
        (21952, False),  # water (7) and clear
        (1, True),  # fill
        (1 << 1, True),  # dilated cloud
        (1 << 2, True),  # cirrus
        (22280, True),  # cloud (3), high cloud confidence
        (1 << 4, True),  # cloud shadow
        (1 << 5, True),  # snow
    ],
)
def test_qa_pixel_bits(qa: int, masked: bool) -> None:
    assert bool(flagged(np.array([qa], dtype=np.uint16), LANDSAT)[0]) is masked


def test_clear_requires_positive_reflectance() -> None:
    nir = np.array([1200, 0, 1200, 1200, np.nan], dtype=np.float32)
    swir = np.array([800, 800, 0, 800, 800], dtype=np.float32)
    scl = np.array([4, 4, 4, 9, 4], dtype=np.uint8)
    assert clear(nir, swir, scl, S2).tolist() == [True, False, False, False, False]
