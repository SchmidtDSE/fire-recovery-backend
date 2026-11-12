"""Tests for the shared geometry/projection helpers."""

from typing import Any, Dict

import pytest
from geojson_pydantic import Polygon
from pyproj import Geod

from src.util.geo_ops import (
    DEFAULT_BUFFER_FRACTION,
    DEFAULT_MAX_BUFFER_METERS,
    DEFAULT_MIN_BUFFER_METERS,
    as_geometry_dict,
    buffered_bounds,
)


def metric_box(width_km: float, height_km: float) -> Dict[str, Any]:
    """A lon/lat box of the given ground size, centred near Los Angeles."""
    geod = Geod(ellps="WGS84")
    centre_lon, centre_lat = -118.7, 34.14
    east, _, _ = geod.fwd(centre_lon, centre_lat, 90, width_km * 500)
    west, _, _ = geod.fwd(centre_lon, centre_lat, 270, width_km * 500)
    _, north, _ = geod.fwd(centre_lon, centre_lat, 0, height_km * 500)
    _, south, _ = geod.fwd(centre_lon, centre_lat, 180, height_km * 500)
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [west, south],
                [east, south],
                [east, north],
                [west, north],
                [west, south],
            ]
        ],
    }


def pinned(geometry: Dict[str, Any], buffer_meters: float):
    """Buffered bounds at an exact distance, with the clamp pinned shut."""
    return buffered_bounds(geometry, minimum=buffer_meters, maximum=buffer_meters)


class TestAsGeometryDict:
    """Normalising the geometry shapes the commands actually hold."""

    def test_passes_through_a_mapping(self) -> None:
        raw = metric_box(1.0, 1.0)
        assert as_geometry_dict(raw) is raw

    def test_unwraps_a_pydantic_geometry(self) -> None:
        raw = metric_box(1.0, 1.0)
        assert as_geometry_dict(Polygon(**raw))["type"] == "Polygon"


class TestBufferedBounds:
    """The padded analysis extent, and the distance it was padded by."""

    def test_returns_four_floats_and_a_distance(self) -> None:
        bounds, buffer_meters = buffered_bounds(
            {
                "type": "Polygon",
                "coordinates": [
                    [[-120, 35], [-119, 35], [-119, 36], [-120, 36], [-120, 35]]
                ],
            }
        )

        assert len(bounds) == 4
        assert all(isinstance(b, float) for b in bounds)
        assert buffer_meters > 0

        minx, miny, maxx, maxy = bounds
        assert minx < -120
        assert miny < 35
        assert maxx > -119
        assert maxy > 36

    @pytest.mark.parametrize(
        "half_extent_deg",
        [
            pytest.param(0.0005, id="sub_acre"),
            pytest.param(0.15, id="large_fire"),
        ],
    )
    def test_buffer_is_the_requested_distance_at_any_fire_size(
        self, half_extent_deg: float
    ) -> None:
        """The buffer is a fixed metric distance, not a share of the extent.

        Regression test: the buffer used to be 20% of the bounding box, so a
        sub-acre perimeter got a few metres while a large fire got kilometres.
        Distances are measured with pyproj here, independently of the
        rasterio reprojection the implementation uses.
        """
        buffer_meters = 250.0
        centre_lon, centre_lat = -118.7, 34.14
        west, east = centre_lon - half_extent_deg, centre_lon + half_extent_deg
        south, north = centre_lat - half_extent_deg, centre_lat + half_extent_deg
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [west, south],
                    [east, south],
                    [east, north],
                    [west, north],
                    [west, south],
                ]
            ],
        }

        (minx, miny, maxx, maxy), applied = pinned(geometry, buffer_meters)
        assert applied == buffer_meters

        geod = Geod(ellps="WGS84")
        # Distance from each original edge out to the buffered edge.
        _, _, west_margin = geod.inv(minx, south, west, south)
        _, _, east_margin = geod.inv(maxx, south, east, south)
        _, _, south_margin = geod.inv(west, miny, west, south)
        _, _, north_margin = geod.inv(west, maxy, west, north)

        # Reprojecting a padded rectangle back to lat/lon densifies its edges,
        # so a wide box can end up slightly over-padded. Overshoot is safe;
        # falling short of the requested buffer is not.
        for margin in (west_margin, east_margin, south_margin, north_margin):
            assert margin >= buffer_meters
            assert margin <= buffer_meters * 1.25

    @pytest.mark.parametrize(
        ("width_km", "height_km", "expected_m", "reason"),
        [
            pytest.param(0.06, 0.06, 100.0, "floor", id="sub_acre_takes_the_floor"),
            pytest.param(3.0, 3.0, 600.0, "fraction", id="mid_size_takes_the_fraction"),
            pytest.param(32.0, 32.0, 1000.0, "ceiling", id="huge_takes_the_ceiling"),
        ],
    )
    def test_buffer_is_a_clamped_fraction_of_perimeter_size(
        self, width_km: float, height_km: float, expected_m: float, reason: str
    ) -> None:
        """A fraction of the perimeter, floored and capped.

        A bare fraction collapses to metres on a sub-acre perimeter and blows
        out to kilometres on a large fire, so it is clamped at both ends.
        """
        _, buffer_meters = buffered_bounds(
            metric_box(width_km, height_km),
            fraction=0.2,
            minimum=100.0,
            maximum=1000.0,
        )

        assert buffer_meters == pytest.approx(expected_m, rel=0.02), reason

    def test_buffer_uses_the_longer_side_of_a_narrow_perimeter(self) -> None:
        """Perimeter imprecision is isotropic, so one distance covers all sides.

        Scaling each axis on its own would give a long, narrow fire a generous
        margin along its length and almost none across it.
        """
        _, buffer_meters = buffered_bounds(
            metric_box(4.0, 0.5), fraction=0.2, minimum=1.0, maximum=100_000.0
        )

        # 20% of the 4 km side, not of the 0.5 km side.
        assert buffer_meters == pytest.approx(800.0, rel=0.02)

    def test_accepts_a_geojson_pydantic_geometry(self) -> None:
        """The commands hold pydantic geometries, not raw mappings."""
        raw = metric_box(3.0, 3.0)

        assert buffered_bounds(Polygon(**raw))[1] == pytest.approx(
            buffered_bounds(raw)[1]
        )

    def test_buffer_policy_defaults_are_coherent(self) -> None:
        """The shipped defaults have to be usable as a clamp."""
        assert 0 < DEFAULT_BUFFER_FRACTION < 1
        assert 0 < DEFAULT_MIN_BUFFER_METERS < DEFAULT_MAX_BUFFER_METERS

    def test_buffer_scales_with_the_requested_distance(self) -> None:
        """A larger buffer distance produces a proportionally larger margin."""
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-118.70, 34.14],
                    [-118.69, 34.14],
                    [-118.69, 34.15],
                    [-118.70, 34.15],
                    [-118.70, 34.14],
                ]
            ],
        }

        geod = Geod(ellps="WGS84")

        def west_margin(buffer_meters: float) -> float:
            minx = pinned(geometry, buffer_meters)[0][0]
            _, _, distance = geod.inv(minx, 34.14, -118.70, 34.14)
            return distance

        assert west_margin(1000.0) == pytest.approx(4 * west_margin(250.0), rel=0.05)


class TestFeatureGeometries:
    """Features carry their geometry one level down."""

    def test_buffers_a_feature_like_its_geometry(self) -> None:
        raw = metric_box(3.0, 3.0)
        feature = {"type": "Feature", "properties": {}, "geometry": raw}

        assert buffered_bounds(feature) == buffered_bounds(raw)

    def test_feature_without_geometry_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="no geometry"):
            buffered_bounds({"type": "Feature", "properties": {}, "geometry": None})
