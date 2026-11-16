"""The POST /v2/severity request: validation and geometry normalization.

The server validates and never transforms what reaches the job id (D8, D20).
The geometry is the exception, by design, and so are the H3 resolutions, a set
whose order and repeats mean nothing: they enter the key sorted and unique. An area of interest enters the key
only through its bounds (design §6); a perimeter to publish enters it whole.
Both go through the same normalization, the deterministic-job-id D10 steps
that matter for either: unwrapping a Feature, dropping Z, and quantizing to
integer micro-degrees. Rings are kept as sent, in their order and orientation:
two files describing one boundary differently are two perimeters.
"""

import math
from datetime import timedelta
from typing import Annotated, Any, Literal, Optional, Union

import numpy as np
from geojson_pydantic import Feature, MultiPolygon, Polygon
from pydantic import BaseModel, ConfigDict, Field, model_validator
from shapely.geometry import MultiPoint, box, mapping

from src.util.date_windows import window_bounds
from src.util.geo_ops import Bounds
from src.v2.constants import (
    FIRE_EVENT_NAME_MAX_LENGTH,
    H3_MAX_RESOLUTION,
    H3_MIN_RESOLUTION,
    MAX_PERIMETER_POSITIONS,
    MAX_WINDOW_DAYS,
)
from src.v2.identity.grid import check_extent
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
Ring = list[Position]
GeometryKind = Literal["perimeter", "aoi"]
# A normalized polygonal geometry: polygons of rings of (lon, lat) in integer
# micro-degrees, a Polygon as one polygon.
QuantizedParts = list[list[list[tuple[int, int]]]]


def quantize(value: float) -> int:
    """Degrees to integer micro-degrees, rounding half up.

    Written as floor(x + 0.5) on purpose (D10): round() is banker's rounding
    and string formatting differs at ties.
    """
    return math.floor(value * MICRODEGREES_PER_DEGREE + 0.5)


def polygon_parts(
    geometry: Union[Polygon, MultiPolygon, Feature],
) -> list[list[Ring]]:
    """Each polygon's rings, exterior first, unwrapping a Feature.

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
    polygons = (
        [geometry.coordinates]
        if isinstance(geometry, Polygon)
        else geometry.coordinates
    )
    return [[[tuple(p) for p in ring] for ring in poly] for poly in polygons]


def polygon_rings(
    geometry: Union[Polygon, MultiPolygon, Feature],
) -> list[Ring]:
    """Every ring of a polygonal geometry, unwrapping a Feature."""
    return [ring for poly in polygon_parts(geometry) for ring in poly]


def quantized_parts(geometry: Union[Polygon, MultiPolygon, Feature]) -> QuantizedParts:
    """The geometry normalized: Z dropped, lon and lat quantized, rings as sent."""
    return [
        [[(quantize(p[0]), quantize(p[1])) for p in ring] for ring in poly]
        for poly in polygon_parts(geometry)
    ]


H3Resolution = Annotated[int, Field(ge=H3_MIN_RESOLUTION, le=H3_MAX_RESOLUTION)]


class SeverityOptions(BaseModel):
    """Optional outputs beyond the index COGs. Each one set is part of the job id."""

    model_config = ConfigDict(extra="forbid")

    h3_resolutions: Optional[list[H3Resolution]] = Field(
        default=None,
        min_length=1,
        description=(
            f"Publish an H3 table of the indices at each of these resolutions "
            f"({H3_MIN_RESOLUTION} to {H3_MAX_RESOLUTION}): each cell holds the "
            f"pixel under its centre. A set: order and repeats don't matter. "
            f"Omit for none."
        ),
    )

    def resolutions(self) -> Optional[tuple[int, ...]]:
        """The requested resolutions as a set: sorted, without repeats."""
        if not self.h3_resolutions:
            return None
        return tuple(sorted(set(self.h3_resolutions)))


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
            "Fire perimeter or area of interest: a Polygon, a MultiPolygon, or a "
            "Feature holding one. Only its bounds place the analysis. It is stored "
            "and published only when geometry_kind is 'perimeter'."
        ),
    )
    geometry_kind: GeometryKind = Field(
        default="aoi",
        description=(
            "'perimeter' if the geometry is a fire perimeter the requester wants "
            "published: it becomes part of the job id and is published in the "
            "analysis's catalog. 'aoi' (default) for an area drawn only to place "
            "the analysis: only its bounds are used, and it is never stored."
        ),
    )
    prefire_window: DateWindow
    postfire_window: DateWindow
    sensor: Sensor = Field(
        default=SENSOR_DEFAULT,
        description="Satellite source: 'sentinel-2' (default) or 'landsat'",
    )
    options: Optional[SeverityOptions] = Field(
        default=None, description="Optional outputs beyond the index COGs"
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
        if self.geometry_kind == "perimeter":
            positions = sum(len(ring) for ring in polygon_rings(self.geometry))
            if positions > MAX_PERIMETER_POSITIONS:
                raise ValueError(
                    f"The perimeter has {positions} vertices; the limit for "
                    f"publishing one is {MAX_PERIMETER_POSITIONS}"
                )
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

    def quantized(self) -> QuantizedParts:
        """The normalized geometry: what the bounds and a perimeter come from."""
        return quantized_parts(self.geometry)

    def bounds(self) -> Bounds:
        """Bounds of the normalized geometry, in degrees."""
        positions = [p for poly in self.quantized() for ring in poly for p in ring]
        degrees = np.asarray(positions, dtype=np.float64) / MICRODEGREES_PER_DEGREE
        west, south, east, north = MultiPoint(degrees).bounds
        return west, south, east, north

    def bounds_geometry(self) -> dict[str, Any]:
        """The normalized bounds as a GeoJSON Polygon."""
        return dict(mapping(box(*self.bounds())))
