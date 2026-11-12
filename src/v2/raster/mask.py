"""Which observations are clear, from each product's own quality band (design §2)."""

import numpy as np

from src.v2.sensors import MaskSpec


def flagged(flags: np.ndarray, spec: MaskSpec) -> np.ndarray:
    """True where the quality band marks a pixel unusable."""
    if spec.kind == "classes":
        return np.isin(flags, spec.values)
    bitmask = sum(1 << bit for bit in spec.values)
    return (flags.astype(np.uint32) & bitmask) != 0


def clear(
    nir: np.ndarray, swir: np.ndarray, flags: np.ndarray, spec: MaskSpec
) -> np.ndarray:
    """True for observations with data whose flags are clear.

    A digital number of 0 is the products' nodata, and NaN fails the comparison
    too. Reflectance must also be above 0, which needs each scene's scale and
    offset: the compute step checks that (``Observations.reflectances``).
    """
    with np.errstate(invalid="ignore"):
        valid = (nir > 0) & (swir > 0)
    return valid & ~flagged(flags, spec)
