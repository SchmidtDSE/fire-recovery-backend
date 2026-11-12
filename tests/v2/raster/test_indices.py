"""v2's index registry reproduces v1's calculator outputs."""

import json
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pytest

from src.v2.raster.indices import INDICES, Composites, nbr, required_bands

FIXTURE = json.loads(
    (Path(__file__).parents[2] / "fixtures" / "v1_index_values.json").read_text()
)
PIXELS: List[Dict[str, Any]] = FIXTURE["pixels"]


def obs(values: List[Optional[float]]) -> np.ndarray:
    return np.array([np.nan if v is None else v for v in values], dtype=np.float32)


def median(values: List[Optional[float]]) -> np.float32:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return np.float32(np.nanmedian(obs(values)))


def composites_for(pixel: Dict[str, Any]) -> Composites:
    def one(window: str, band: str) -> np.ndarray:
        return np.array([median(pixel[window][band])], dtype=np.float32)

    return Composites.from_medians(
        one("prefire", "nir"),
        one("prefire", "swir"),
        one("postfire", "nir"),
        one("postfire", "swir"),
    )


def assert_close(actual: float, expected: Optional[float]) -> None:
    if expected is None:
        assert np.isnan(actual)
    else:
        assert actual == pytest.approx(expected, rel=1e-5, abs=1e-6)


@pytest.mark.parametrize("pixel", PIXELS, ids=[p["name"] for p in PIXELS])
def test_reproduces_v1(pixel: Dict[str, Any]) -> None:
    composites = composites_for(pixel)
    expected = pixel["expected"]
    assert_close(float(composites.prefire_nbr[0]), expected["prefire_nbr"])
    assert_close(float(composites.postfire_nbr[0]), expected["postfire_nbr"])
    for index in INDICES:
        assert_close(float(index.compute(composites)[0]), expected[index.name])


def test_indices_are_float32() -> None:
    composites = composites_for(PIXELS[0])
    for index in INDICES:
        assert index.compute(composites).dtype == np.float32


def test_registry() -> None:
    assert [i.name for i in INDICES] == ["dnbr", "rdnbr", "rbr"]
    assert len({i.name for i in INDICES}) == len(INDICES)
    assert required_bands() == ("nir", "swir")
    for index in INDICES:
        assert index.title and index.description


def test_nbr_of_zero_reflectance_is_nan() -> None:
    zero = np.zeros(1, dtype=np.float32)
    assert np.isnan(nbr(zero, zero)[0])
