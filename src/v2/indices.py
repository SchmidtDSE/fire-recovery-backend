"""The burn severity indices: a registry of pure functions over composites.

v1's calculators were classes that called each other, so NBR was recomputed
for every index. Here each date window is composited once, NBR is computed
once per window, and every registered index reads those composites (design §3).

To add an index on the same inputs, add an entry to INDICES: it gets its own
COG, asset, band statistics and README line. An index that needs another band
names it in ``needs``, and that band must be configured for every provider in
``config/stac_providers.json``. Changing a formula needs a PIPELINE_VERSION bump.
"""

from dataclasses import dataclass
from typing import Callable

import numpy as np

# RdNBR's denominator where sqrt(|prefire NBR|) is zero, as in v1.
RDNBR_ZERO_DENOMINATOR = np.float32(0.001)
# RBR's denominator offset (Parks et al. 2014).
RBR_OFFSET = np.float32(1.001)


def nbr(nir: np.ndarray, swir: np.ndarray) -> np.ndarray:
    """Normalized Burn Ratio, (NIR - SWIR) / (NIR + SWIR), as float32."""
    with np.errstate(divide="ignore", invalid="ignore"):
        return ((nir - swir) / (nir + swir)).astype(np.float32)


@dataclass(frozen=True)
class Composites:
    """Each window's median reflectance and its NBR, for one block of pixels."""

    prefire_nir: np.ndarray
    prefire_swir: np.ndarray
    postfire_nir: np.ndarray
    postfire_swir: np.ndarray
    prefire_nbr: np.ndarray
    postfire_nbr: np.ndarray

    @classmethod
    def from_medians(
        cls,
        prefire_nir: np.ndarray,
        prefire_swir: np.ndarray,
        postfire_nir: np.ndarray,
        postfire_swir: np.ndarray,
    ) -> "Composites":
        return cls(
            prefire_nir=prefire_nir,
            prefire_swir=prefire_swir,
            postfire_nir=postfire_nir,
            postfire_swir=postfire_swir,
            prefire_nbr=nbr(prefire_nir, prefire_swir),
            postfire_nbr=nbr(postfire_nir, postfire_swir),
        )


@dataclass(frozen=True)
class Index:
    """One burn severity index: its asset key, documentation and formula."""

    name: str
    title: str
    description: str
    needs: tuple[str, ...]
    fn: Callable[[Composites], np.ndarray]

    def compute(self, composites: Composites) -> np.ndarray:
        """The index as float32; NaN wherever an input is missing."""
        with np.errstate(divide="ignore", invalid="ignore"):
            return self.fn(composites).astype(np.float32)


def _dnbr(c: Composites) -> np.ndarray:
    return c.prefire_nbr - c.postfire_nbr


def _rdnbr(c: Composites) -> np.ndarray:
    root = np.sqrt(np.abs(c.prefire_nbr))
    return _dnbr(c) / np.where(root != 0, root, RDNBR_ZERO_DENOMINATOR)


def _rbr(c: Composites) -> np.ndarray:
    return _dnbr(c) / (c.prefire_nbr + RBR_OFFSET)


DNBR = Index(
    name="dnbr",
    title="Differenced NBR (dNBR)",
    description="Prefire NBR minus postfire NBR, where NBR = (NIR - SWIR) / (NIR + SWIR).",
    needs=("nir", "swir"),
    fn=_dnbr,
)
RDNBR = Index(
    name="rdnbr",
    title="Relativized dNBR (RdNBR)",
    description=(
        "dNBR / sqrt(|prefire NBR|), with 0.001 in place of a zero denominator "
        "(Miller & Thode 2007)."
    ),
    needs=("nir", "swir"),
    fn=_rdnbr,
)
RBR = Index(
    name="rbr",
    title="Relativized Burn Ratio (RBR)",
    description="dNBR / (prefire NBR + 1.001) (Parks et al. 2014).",
    needs=("nir", "swir"),
    fn=_rbr,
)

INDICES: tuple[Index, ...] = (DNBR, RDNBR, RBR)


def required_bands(indices: tuple[Index, ...] = INDICES) -> tuple[str, ...]:
    """The union of every index's logical input bands, sorted."""
    return tuple(sorted({band for index in indices for band in index.needs}))
