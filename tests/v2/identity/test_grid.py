"""The tile snap and the output raster grid."""

import pytest

from src.util.geo_ops import Bounds
from src.v2.constants import SNAP_TILES_PER_SIDE
from src.v2.identity.grid import (
    TileRange,
    buffered_megapixels,
    check_extent,
    metric_size,
    raster_grid,
    snap_tiles,
    tile_bounds,
    utm_epsg,
)

BOXES = {
    "7 km": (-116.11, 33.87, -116.01, 33.94),
    "30 km": (-121.0, 39.0, -120.65, 39.27),
    "130 km": (-122.5, 38.5, -121.0, 39.67),
    "southern": (148.95, -35.42, 149.08, -35.31),
    "high latitude": (-150.2, 64.7, -149.6, 64.95),
}


def contains(outer: Bounds, inner: Bounds) -> bool:
    return (
        outer[0] <= inner[0]
        and outer[1] <= inner[1]
        and outer[2] >= inner[2]
        and outer[3] >= inner[3]
    )


@pytest.mark.parametrize("bounds", BOXES.values(), ids=BOXES.keys())
def test_tiles_cover_the_box(bounds: Bounds) -> None:
    assert contains(tile_bounds(snap_tiles(bounds)), bounds)


@pytest.mark.parametrize("bounds", BOXES.values(), ids=BOXES.keys())
def test_tile_size_is_near_the_target_fraction(bounds: Bounds) -> None:
    tiles = snap_tiles(bounds)
    snapped_width, snapped_height = metric_size(tile_bounds(tiles))
    tile_metres = snapped_width / (tiles.x[1] - tiles.x[0] + 1)
    target = max(metric_size(bounds)) / SNAP_TILES_PER_SIDE
    # The zoom is rounded in log2, so a tile is within a factor of sqrt(2).
    assert target / 2**0.5 <= tile_metres <= target * 2**0.5


@pytest.mark.parametrize("bounds", BOXES.values(), ids=BOXES.keys())
@pytest.mark.parametrize("sensor, offset", [("sentinel-2", 0), ("landsat", 15)])
def test_raster_grid_is_aligned_and_covers_the_tiles(
    bounds: Bounds, sensor: str, offset: int
) -> None:
    tiles = snap_tiles(bounds)
    grid = raster_grid(tiles, sensor)
    for edge in grid.bounds:
        assert (edge - offset) % grid.resolution == 0
    assert grid.width * grid.resolution == grid.bounds[2] - grid.bounds[0]
    assert grid.height * grid.resolution == grid.bounds[3] - grid.bounds[1]
    assert contains(grid.footprint(), tile_bounds(tiles))


def test_grid_transform_starts_at_the_top_left() -> None:
    grid = raster_grid(snap_tiles(BOXES["7 km"]), "sentinel-2")
    transform = grid.transform()
    assert (transform.c, transform.f) == (grid.bounds[0], grid.bounds[3])
    assert (transform.a, transform.e) == (20, -20)


def test_tile_range_round_trips_through_json() -> None:
    tiles = snap_tiles(BOXES["30 km"])
    assert TileRange.model_validate_json(tiles.model_dump_json()) == tiles


@pytest.mark.parametrize(
    "lon, lat, epsg",
    [
        (-116.0, 33.9, 32611),
        (149.0, -35.3, 32755),
        (-180.0, 10.0, 32601),
        (179.99, 10.0, 32660),
        (180.0, -1.0, 32760),
        (0.0, 0.0, 32631),
    ],
)
def test_utm_epsg(lon: float, lat: float, epsg: int) -> None:
    assert utm_epsg(lon, lat) == epsg


@pytest.mark.parametrize(
    "bounds",
    [
        (179.9, 10.0, -179.9, 10.1),
        (-170.0, 0.0, 20.0, 1.0),
        (0.0, 85.0, 0.1, 85.2),
        (0.0, -85.2, 0.1, -85.0),
    ],
)
def test_check_extent_rejects(bounds: Bounds) -> None:
    with pytest.raises(ValueError):
        check_extent(bounds)


def test_buffered_megapixels() -> None:
    # About 9.3 km x 7.5 km at 20 m.
    assert buffered_megapixels(BOXES["7 km"], 20) == pytest.approx(0.18, rel=0.1)
