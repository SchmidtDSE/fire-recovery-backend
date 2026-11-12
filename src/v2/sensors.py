"""Per-sensor facts the pipeline depends on: native grids, masks and scaling.

Any change here changes output for identical requests, so it needs a
PIPELINE_VERSION bump.
"""

from dataclasses import dataclass
from typing import Any, Literal, Mapping, cast, get_args

from src.stac.stac_endpoint_handler import DEFAULT_SENSOR

Sensor = Literal["sentinel-2", "landsat"]

SENSORS: tuple[str, ...] = get_args(Sensor)


@dataclass(frozen=True)
class SensorGrid:
    """A sensor's native pixel grid in its UTM zone.

    Pixel edges sit at ``offset + k * resolution`` metres on both axes. Snapping
    the output grid to them makes output pixels coincide with source pixels, so
    nearest-neighbour reads are exact.
    """

    resolution: int
    offset: int


@dataclass(frozen=True)
class MaskSpec:
    """Which values of a product's quality band mark a pixel as unusable.

    ``classes`` lists values of a classification band (any match is masked);
    ``bits`` lists bit positions of a bit-flag band (any set bit is masked).
    """

    kind: Literal["classes", "bits"]
    values: tuple[int, ...]
    description: str


# Sentinel-2 L2A at 20 m (B8A, B12 and SCL are native there): origins are
# multiples of 20 m. Landsat Collection 2 Level-2 at 30 m: origins sit 15 m off
# multiples of 30 m. Both checked on Element 84 items.
SENSOR_GRIDS: dict[str, SensorGrid] = {
    "sentinel-2": SensorGrid(resolution=20, offset=0),
    "landsat": SensorGrid(resolution=30, offset=15),
}

# Each product's own quality flags (design §2, decision 2). No standard smoke
# flag exists: SWIR sees through most smoke, thick plumes are usually flagged as
# cloud, and the median absorbs the rest.
SENSOR_MASKS: dict[str, MaskSpec] = {
    "sentinel-2": MaskSpec(
        kind="classes",
        values=(0, 1, 3, 8, 9, 10, 11),
        description=(
            "SCL: no data, saturated or defective, cloud shadow, medium- and "
            "high-probability cloud, thin cirrus, snow"
        ),
    ),
    "landsat": MaskSpec(
        kind="bits",
        values=(0, 1, 2, 3, 4, 5),
        description="QA_PIXEL: fill, dilated cloud, cirrus, cloud, cloud shadow, snow",
    ),
}

# Reflectance scaling for items that don't publish raster:bands scale and
# offset. Planetary Computer's Sentinel-2 items don't (checked 2026-11); its
# Landsat items and every Element 84 item do. Sentinel-2 L2A gained a -1000 DN
# offset (BOA_ADD_OFFSET) with processing baseline 04.00 in January 2022.
S2_SCALE = 1e-4
S2_BOA_OFFSET = -0.1
S2_OFFSET_FROM_BASELINE = 4.0
LANDSAT_C2_SCALE = 2.75e-5
LANDSAT_C2_OFFSET = -0.2

# Native type and nodata of each logical band. Passed to odc-stac, so loads
# don't depend on whether a provider publishes raster:bands.
SENSOR_BAND_TYPES: dict[str, dict[str, tuple[str, int]]] = {
    "sentinel-2": {"nir": ("uint16", 0), "swir": ("uint16", 0), "mask": ("uint8", 0)},
    "landsat": {"nir": ("uint16", 0), "swir": ("uint16", 0), "mask": ("uint16", 1)},
}

assert set(SENSOR_GRIDS) == set(SENSORS), "every Sensor needs a grid"
assert set(SENSOR_MASKS) == set(SENSORS), "every Sensor needs a mask"
assert set(SENSOR_BAND_TYPES) == set(SENSORS), "every Sensor needs band types"


def as_sensor(value: str) -> Sensor:
    """Narrow a string to a supported Sensor.

    Raises:
        ValueError: if the sensor isn't supported.
    """
    if value not in SENSORS:
        raise ValueError(f"Unsupported sensor {value!r}; expected one of {SENSORS}")
    return cast(Sensor, value)


def fallback_scaling(sensor: str, properties: Mapping[str, Any]) -> tuple[float, float]:
    """(scale, offset) from the product's documented rules, for an item without them.

    Raises:
        ValueError: if a Sentinel-2 item doesn't state its processing baseline.
    """
    if sensor == "landsat":
        return LANDSAT_C2_SCALE, LANDSAT_C2_OFFSET
    baseline = properties.get("s2:processing_baseline")
    if baseline is None:
        raise ValueError(
            "Sentinel-2 item has neither raster:bands scaling nor "
            "s2:processing_baseline, so its reflectance offset is unknown"
        )
    offset = S2_BOA_OFFSET if float(baseline) >= S2_OFFSET_FROM_BASELINE else 0.0
    return S2_SCALE, offset


# The provider chain's default, applied before hashing (D13).
SENSOR_DEFAULT: Sensor = as_sensor(DEFAULT_SENSOR)
