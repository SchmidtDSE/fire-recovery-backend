"""Per-sensor facts the pipeline depends on.

Any change here changes output for identical requests, so it needs a
PIPELINE_VERSION bump.
"""

from dataclasses import dataclass
from typing import Literal, cast, get_args

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


# Sentinel-2 L2A at 20 m (B8A, B12 and SCL are native there): origins are
# multiples of 20 m. Landsat Collection 2 Level-2 at 30 m: origins sit 15 m off
# multiples of 30 m. Both checked on Element 84 items.
SENSOR_GRIDS: dict[str, SensorGrid] = {
    "sentinel-2": SensorGrid(resolution=20, offset=0),
    "landsat": SensorGrid(resolution=30, offset=15),
}

assert set(SENSOR_GRIDS) == set(SENSORS), "every Sensor needs a grid"


def as_sensor(value: str) -> Sensor:
    """Narrow a string to a supported Sensor.

    Raises:
        ValueError: if the sensor isn't supported.
    """
    if value not in SENSORS:
        raise ValueError(f"Unsupported sensor {value!r}; expected one of {SENSORS}")
    return cast(Sensor, value)


# The provider chain's default, applied before hashing (D13).
SENSOR_DEFAULT: Sensor = as_sensor(DEFAULT_SENSOR)
