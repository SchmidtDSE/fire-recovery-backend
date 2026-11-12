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


@pytest.mark.parametrize("index", INDICES, ids=lambda index: index.name)
def test_the_published_expression_computes_the_index(index: Any) -> None:
    # processing:expression must say exactly what the code computes, including
    # RdNBR's zero-denominator rule.
    rng = np.random.default_rng(7)
    nir = rng.uniform(0.01, 0.6, 500).astype(np.float32)
    swir = rng.uniform(0.01, 0.6, 500).astype(np.float32)
    swir[:5] = nir[:5]  # prefire NBR exactly 0
    composites = Composites.from_medians(nir, swir, swir, nir)
    namespace = {"where": np.where, "sqrt": np.sqrt, "abs": np.abs}
    namespace.update(vars(composites))
    with np.errstate(divide="ignore", invalid="ignore"):
        published = eval(index.expression, {"__builtins__": {}}, namespace)
    np.testing.assert_allclose(
        np.asarray(published, dtype=np.float32), index.compute(composites), rtol=1e-6
    )
