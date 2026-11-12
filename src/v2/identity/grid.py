"""The spatial part of an analysis: the cache-key tiles and the output raster grid.

The buffered box snaps outward to tiles of OGC's WebMercatorQuad tile matrix
set (OGC 17-083r4), the z/x/y grid web maps use, as implemented by morecantile.
The tile range is the spatial part of the cache key (design §6). The output
raster then covers those tiles in the UTM zone of their centre, on the sensor's
native pixel grid (design §2).
"""

import math
from typing import Final, Literal

import morecantile
from affine import Affine
from pydantic import BaseModel, ConfigDict, computed_field
from rasterio.crs import CRS
from rasterio.warp import transform_bounds

from src.util.geo_ops import WGS84, Bounds, local_metric_crs
from src.v2.constants import SNAP_TILES_PER_SIDE
from src.v2.sensors import SENSOR_GRIDS

TMS_NAME: Final = "WebMercatorQuad"
_TMS = morecantile.tms.get(TMS_NAME)

# Length of the equator on the sphere WebMercatorQuad projects onto, in
# metres: the ground width of its single zoom-0 tile.
EQUATOR_METRES = 2 * math.pi * 6378137.0
MAX_ZOOM = 24
# WebMercatorQuad stops at about ±85.05° latitude.
MAX_LATITUDE = 85.0511287798066
# Points per edge when projecting a box, so the result contains its curved image.
DENSIFY_POINTS = 21

UTM_NORTH_EPSG_BASE = 32600
UTM_SOUTH_EPSG_BASE = 32700
UTM_ZONE_WIDTH_DEGREES = 6
UTM_ZONES = 60


class TileRange(BaseModel):
    """An inclusive range of WebMercatorQuad tiles at one zoom: the spatial key."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tms: Literal["WebMercatorQuad"] = TMS_NAME
    z: int
    x: tuple[int, int]
    y: tuple[int, int]


class Grid(BaseModel):
    """The output raster's grid: a UTM CRS, a resolution and pixel-aligned bounds."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    epsg: int
    resolution: int
    bounds: tuple[int, int, int, int]  # left, bottom, right, top, in metres

    @computed_field  # type: ignore[prop-decorator]
    @property
    def width(self) -> int:
        return (self.bounds[2] - self.bounds[0]) // self.resolution

    @computed_field  # type: ignore[prop-decorator]
    @property
    def height(self) -> int:
        return (self.bounds[3] - self.bounds[1]) // self.resolution

    def transform(self) -> Affine:
        """The affine transform of the grid's top-left pixel corner."""
        left, _, _, top = self.bounds
        return Affine(self.resolution, 0, left, 0, -self.resolution, top)

    def footprint(self) -> Bounds:
        """The grid's extent in EPSG:4326, containing its projected outline."""
        return transform_bounds(
            CRS.from_epsg(self.epsg), WGS84, *self.bounds, densify_pts=DENSIFY_POINTS
        )


def check_extent(bounds: Bounds) -> None:
    """Reject boxes the tile grid can't key.

    Raises:
        ValueError: if the box crosses the antimeridian, spans more than half
            the globe, or reaches past WebMercatorQuad's latitude limit.
    """
    west, south, east, north = bounds
    if west > east or east - west > 180:
        raise ValueError("The area crosses the antimeridian or is too wide")
    if south < -MAX_LATITUDE or north > MAX_LATITUDE:
        raise ValueError(
            f"The area reaches past ±{MAX_LATITUDE:.2f}° latitude, "
            f"the limit of the {TMS_NAME} tile grid"
        )


def metric_size(bounds: Bounds) -> tuple[float, float]:
    """Width and height, in metres, of an EPSG:4326 box in a local equidistant CRS."""
    crs = local_metric_crs(bounds)
    left, bottom, right, top = transform_bounds(
        WGS84, crs, *bounds, densify_pts=DENSIFY_POINTS
    )
    return right - left, top - bottom


def snap_tiles(bounds: Bounds) -> TileRange:
    """Snap an EPSG:4326 box outward to WebMercatorQuad tiles.

    The zoom is the one whose tiles are closest to 1/SNAP_TILES_PER_SIDE of the
    box's longer side, at the box's mid-latitude.

    Raises:
        ValueError: see check_extent.
    """
    check_extent(bounds)
    west, south, east, north = bounds
    width, height = metric_size(bounds)
    target_tile_metres = max(width, height) / SNAP_TILES_PER_SIDE
    zoom_zero_metres = EQUATOR_METRES * math.cos(math.radians((south + north) / 2))
    zoom = round(math.log2(zoom_zero_metres / target_tile_metres))
    zoom = min(max(zoom, 0), MAX_ZOOM)
    upper_left = _TMS.tile(west, north, zoom)
    lower_right = _TMS.tile(east, south, zoom)
    return TileRange(
        z=zoom, x=(upper_left.x, lower_right.x), y=(upper_left.y, lower_right.y)
    )


def tile_bounds(tiles: TileRange) -> Bounds:
    """The EPSG:4326 box the tile range covers."""
    upper_left = _TMS.bounds(morecantile.Tile(tiles.x[0], tiles.y[0], tiles.z))
    lower_right = _TMS.bounds(morecantile.Tile(tiles.x[1], tiles.y[1], tiles.z))
    return (upper_left.left, lower_right.bottom, lower_right.right, upper_left.top)


def utm_epsg(lon: float, lat: float) -> int:
    """EPSG code of the standard WGS84 UTM zone containing a point."""
    zone = min(int((lon + 180) // UTM_ZONE_WIDTH_DEGREES) + 1, UTM_ZONES)
    return (UTM_NORTH_EPSG_BASE if lat >= 0 else UTM_SOUTH_EPSG_BASE) + zone


def raster_grid(tiles: TileRange, sensor: str) -> Grid:
    """The output grid covering a tile range, aligned to the sensor's pixels.

    The tiles' box is projected to the UTM zone of its centre, then each edge
    moves outward to the nearest native pixel edge.
    """
    west, south, east, north = tile_bounds(tiles)
    epsg = utm_epsg((west + east) / 2, (south + north) / 2)
    left, bottom, right, top = transform_bounds(
        WGS84, CRS.from_epsg(epsg), west, south, east, north, densify_pts=DENSIFY_POINTS
    )
    native = SENSOR_GRIDS[sensor]

    def outward(value: float, rounding: int) -> int:
        steps = (value - native.offset) / native.resolution
        whole = math.floor(steps) if rounding < 0 else math.ceil(steps)
        return whole * native.resolution + native.offset

    return Grid(
        epsg=epsg,
        resolution=native.resolution,
        bounds=(
            outward(left, -1),
            outward(bottom, -1),
            outward(right, 1),
            outward(top, 1),
        ),
    )


def buffered_megapixels(bounds: Bounds, resolution: int) -> float:
    """Pixels, in millions, an EPSG:4326 box holds at a resolution in metres."""
    width, height = metric_size(bounds)
    return width * height / resolution**2 / 1e6
