"""
Geometry and projection helpers shared across commands.

Buffering an area of interest is the main reason this module exists. Doing it
in degrees does not give a constant distance -- a degree of longitude shrinks
toward the poles -- so distances are applied in an azimuthal equidistant
projection centred on the geometry, where distance from that centre is true in
every direction regardless of latitude.
"""

from typing import Any, Dict, Tuple

from geojson_pydantic import Polygon, MultiPolygon, Feature
from pydantic import BaseModel
from rasterio.crs import CRS
from rasterio.warp import transform_bounds
from shapely.geometry.base import BaseGeometry
from shapely.geometry import shape

WGS84 = CRS.from_epsg(4326)

GeometryLike = Polygon | MultiPolygon | Feature | Dict[str, Any]

Bounds = Tuple[float, float, float, float]

# Buffer policy. The margin around a perimeter absorbs two kinds of slop: the
# imprecision of the perimeter itself, and scene-to-scene registration. A bare
# fraction of the perimeter's size tracks the first reasonably well in the
# middle of the range but fails at both ends -- a fifth of a sub-acre perimeter
# is a few metres, far tighter than anyone can draw, while a fifth of a 16 km
# fire is kilometres of mostly-unburned background that nobody asked for and
# every pixel of which gets downloaded. So the fraction is clamped at both
# ends.
DEFAULT_BUFFER_FRACTION = 0.2
DEFAULT_MIN_BUFFER_METERS = 100.0
DEFAULT_MAX_BUFFER_METERS = 1000.0


def as_geometry_dict(geometry: GeometryLike) -> Dict[str, Any]:
    """Normalise a geojson-pydantic model or raw mapping to a dict."""
    if isinstance(geometry, BaseModel):
        return geometry.model_dump()
    return geometry


def _shapely_geometry(geometry: GeometryLike) -> BaseGeometry:
    """Shapely geometry for a GeoJSON geometry, Feature, or mapping.

    ``shape()`` understands geometries, not Features, so a Feature's geometry
    member is unwrapped first.
    """
    as_dict = as_geometry_dict(geometry)
    if as_dict.get("type") == "Feature":
        if not as_dict.get("geometry"):
            raise ValueError("Feature has no geometry")
        as_dict = as_dict["geometry"]
    return shape(as_dict)


def local_metric_crs(bounds: Bounds) -> CRS:
    """
    An azimuthal equidistant CRS centred on the given bounds.

    Distance from that centre is true in every direction, so metres mean the
    same thing in every direction regardless of latitude.
    """
    minx, miny, maxx, maxy = bounds
    return CRS.from_proj4(
        f"+proj=aeqd +lat_0={(miny + maxy) / 2} +lon_0={(minx + maxx) / 2} "
        f"+datum=WGS84 +units=m +no_defs"
    )


def buffered_bounds(
    geometry: GeometryLike,
    *,
    fraction: float = DEFAULT_BUFFER_FRACTION,
    minimum: float = DEFAULT_MIN_BUFFER_METERS,
    maximum: float = DEFAULT_MAX_BUFFER_METERS,
) -> Tuple[Bounds, float]:
    """
    Expand a geometry's bounds by a metric buffer sized from the geometry.

    The distance is a fraction of the longer side of the bounding box, clamped
    into ``[minimum, maximum]``. Perimeter imprecision is roughly isotropic, so
    that one distance is applied on every side rather than scaling each axis
    independently -- otherwise a long, narrow fire would get a generous margin
    along its length and almost none across it.

    Returns:
        The padded bounds in EPSG:4326 (the CRS stackstac is asked to stack
        in), and the buffer distance in metres that was applied.
    """
    bounds = _shapely_geometry(geometry).bounds
    metric_crs = local_metric_crs(bounds)
    metric_bounds = transform_bounds(WGS84, metric_crs, *bounds)

    longest_side = max(
        metric_bounds[2] - metric_bounds[0],
        metric_bounds[3] - metric_bounds[1],
    )
    buffer_meters = min(max(longest_side * fraction, minimum), maximum)

    padded = (
        metric_bounds[0] - buffer_meters,
        metric_bounds[1] - buffer_meters,
        metric_bounds[2] + buffer_meters,
        metric_bounds[3] + buffer_meters,
    )

    # transform_bounds densifies the edges, so the returned box contains the
    # projected rectangle rather than just its corners.
    return transform_bounds(metric_crs, WGS84, *padded), buffer_meters
