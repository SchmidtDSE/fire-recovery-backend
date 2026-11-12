"""The POST /v2/severity request: validation and geometry normalization.

The server validates and never transforms what reaches the job id (D8, D20).
The geometry is the exception, by design: it enters the key only through its
bounds (design §6), so it is normalized just far enough to make those bounds
deterministic. Of the deterministic-job-id D10 steps, that means unwrapping a
Feature, dropping Z, and quantizing to integer micro-degrees. Promoting a
Polygon to a MultiPolygon and orienting rings can't move bounds, so they're
skipped.
"""

import math
from datetime import timedelta
from typing import Annotated, Any, Optional, Union

from geojson_pydantic import Feature, MultiPolygon, Polygon
from pydantic import BaseModel, ConfigDict, Field, model_validator

from src.util.date_windows import window_bounds
from src.util.geo_ops import Bounds
from src.v2.constants import FIRE_EVENT_NAME_MAX_LENGTH, MAX_WINDOW_DAYS
from src.v2.grid import check_extent
from src.v2.sensors import SENSOR_DEFAULT, Sensor

FIRE_EVENT_NAME_PATTERN = r"^[A-Za-z0-9_-]+$"
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
EMAIL_MAX_LENGTH = 254
MICRODEGREES_PER_DEGREE = 1_000_000

FireEventName = Annotated[
    str,
    Field(
        pattern=FIRE_EVENT_NAME_PATTERN,
        min_length=1,
        max_length=FIRE_EVENT_NAME_MAX_LENGTH,
        description=(
            "Name of the fire event: letters, digits, '_' and '-' only. Validated, "
            "never transformed; it is part of the job id."
        ),
    ),
]

DateWindow = Annotated[
    list[str],
    Field(
        min_length=2,
        max_length=2,
        description=(
            "[start, end] as ISO dates or datetimes (naive values are UTC). The end "
            "date includes its whole day. Hashed verbatim, so '2023-06-01' and "
            "'2023-06-01T00:00:00' are different requests."
        ),
    ),
]

Position = tuple[float, ...]


def quantize(value: float) -> int:
    """Degrees to integer micro-degrees, rounding half up.

    Written as floor(x + 0.5) on purpose (D10): round() is banker's rounding
    and string formatting differs at ties.
    """
    return math.floor(value * MICRODEGREES_PER_DEGREE + 0.5)


def polygon_rings(
    geometry: Union[Polygon, MultiPolygon, Feature],
) -> list[list[Position]]:
    """Every ring of a polygonal geometry, unwrapping a Feature.

    Raises:
        ValueError: if a Feature has no geometry or a non-polygonal one.
    """
    if isinstance(geometry, Feature):
        inner = geometry.geometry
        if inner is None:
            raise ValueError("The Feature has no geometry")
        if not isinstance(inner, (Polygon, MultiPolygon)):
            raise ValueError(
                f"The Feature's geometry must be a Polygon or MultiPolygon, "
                f"not {inner.type}"
            )
        geometry = inner
    if isinstance(geometry, Polygon):
        return [[tuple(p) for p in ring] for ring in geometry.coordinates]
    return [[tuple(p) for p in ring] for poly in geometry.coordinates for ring in poly]


def window_days(window: list[str]) -> float:
    """Length of a date window in days, with the end date's whole day included."""
    start, end = window_bounds(window[0], window[1])
    return (end - start) / timedelta(days=1)


class SeverityRequest(BaseModel):
    """Request for a fire severity analysis."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Geology_Fire",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-116.098276, 33.929925],
                            [-116.098276, 33.880794],
                            [-116.019318, 33.880794],
                            [-116.019318, 33.929925],
                            [-116.098276, 33.929925],
                        ]
                    ],
                },
                "prefire_window": ["2023-06-01", "2023-06-09"],
                "postfire_window": ["2023-06-17", "2023-06-22"],
                "sensor": "sentinel-2",
            }
        }
    )

    fire_event_name: FireEventName
    geometry: Union[Polygon, MultiPolygon, Feature] = Field(
        ...,
        description=(
            "Fire perimeter: a Polygon, a MultiPolygon, or a Feature holding one. "
            "Only its bounds are used, and it is not stored."
        ),
    )
    prefire_window: DateWindow
    postfire_window: DateWindow
    sensor: Sensor = Field(
        default=SENSOR_DEFAULT,
        description="Satellite source: 'sentinel-2' (default) or 'landsat'",
    )
    email: Optional[str] = Field(
        default=None,
        pattern=EMAIL_PATTERN,
        max_length=EMAIL_MAX_LENGTH,
        description=(
            "Optional address to email when the analysis finishes. Never stored; "
            "the status URL works without it."
        ),
    )

    @model_validator(mode="after")
    def _check(self) -> "SeverityRequest":
        check_extent(self.bounds())
        for label, window in (
            ("prefire_window", self.prefire_window),
            ("postfire_window", self.postfire_window),
        ):
            days = window_days(window)
            if days > MAX_WINDOW_DAYS:
                raise ValueError(
                    f"{label} spans {days:g} days; the limit is {MAX_WINDOW_DAYS}"
                )
        _, prefire_end = window_bounds(*self.prefire_window)
        postfire_start, _ = window_bounds(*self.postfire_window)
        if prefire_end > postfire_start:
            raise ValueError("prefire_window must end before postfire_window starts")
        return self

    def bounds(self) -> Bounds:
        """Bounds of the geometry after D10 normalization, in degrees.

        Quantizing is monotonic, so quantizing the bounds equals the bounds of
        the quantized coordinates.
        """
        positions = [p for ring in polygon_rings(self.geometry) for p in ring]
        lons = [quantize(p[0]) for p in positions]
        lats = [quantize(p[1]) for p in positions]
        return (
            min(lons) / MICRODEGREES_PER_DEGREE,
            min(lats) / MICRODEGREES_PER_DEGREE,
            max(lons) / MICRODEGREES_PER_DEGREE,
            max(lats) / MICRODEGREES_PER_DEGREE,
        )

    def bounds_geometry(self) -> dict[str, Any]:
        """The normalized bounds as a GeoJSON Polygon."""
        west, south, east, north = self.bounds()
        ring = [
            [west, south],
            [east, south],
            [east, north],
            [west, north],
            [west, south],
        ]
        return {"type": "Polygon", "coordinates": [ring]}
