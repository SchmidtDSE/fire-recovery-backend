"""The library calls reproduce the hand-written spatial code they replaced.

tests/fixtures/v2_spatial_golden.json holds that code's outputs for random
inputs (its "description" says where they came from). Anything that differs
here changes published grids, buffers or cells for an existing job id. Results
are compared exactly, except buffers, which pass through PROJ as floats.
"""

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest
from affine import Affine
from odc.geo.geobox import GeoBox

from src.util.geo_ops import buffered_bounds
from src.v2.catalog.sidecar import h3_cover
from src.v2.identity.grid import TileRange, raster_grid, utm_epsg
from src.v2.identity.request import SeverityRequest
from src.v2.raster.compute import blocks

GOLDEN: Dict[str, List[Dict[str, Any]]] = json.loads(
    (Path(__file__).parents[1] / "fixtures" / "v2_spatial_golden.json").read_text()
)
WINDOWS = {"prefire_window": ["2023-06-01", "2023-06-09"],
           "postfire_window": ["2023-06-17", "2023-06-22"]}  # fmt: skip


def box_polygon(bounds: List[float]) -> Dict[str, Any]:
    west, south, east, north = bounds
    ring = [[west, south], [east, south], [east, north], [west, north], [west, south]]
    return {"type": "Polygon", "coordinates": [ring]}


def test_utm_zones() -> None:
    cases = GOLDEN["utm"]
    assert [utm_epsg(c["lon"], c["lat"]) for c in cases] == [c["epsg"] for c in cases]


def test_raster_grids() -> None:
    for case in GOLDEN["raster_grid"]:
        grid = raster_grid(TileRange.model_validate(case["tiles"]), case["sensor"])
        assert (grid.epsg, grid.resolution, list(grid.bounds)) == (
            case["epsg"],
            case["resolution"],
            case["bounds"],
        ), case


# PROJ's projections differ in the last bits across CPUs and maths libraries
# (seen on CI: 1e-15 degrees). A millimetre is about 1e-8 degrees.
DEGREES = 1e-12
METRES = 1e-6


def test_buffered_bounds() -> None:
    for case in GOLDEN["buffered_bounds"]:
        padded, metres = buffered_bounds(box_polygon(case["bounds"]))
        assert list(padded) == pytest.approx(case["padded"], rel=0, abs=DEGREES), case
        assert metres == pytest.approx(case["buffer_metres"], rel=0, abs=METRES), case


def test_h3_covers() -> None:
    for case in GOLDEN["h3_cover"]:
        resolution, cells = h3_cover(tuple(case["footprint"]))
        assert (resolution, cells) == (case["resolution"], case["cells"]), case


@pytest.mark.parametrize("case", GOLDEN["blocks"])
def test_blocks(case: Dict[str, Any]) -> None:
    full = GeoBox(
        (case["height"], case["width"]), Affine(20, 0, 0, 0, -20, 0), "EPSG:32611"
    )
    windows = [
        [w.row_off, w.col_off, w.height, w.width] for _, w in blocks(full, case["side"])
    ]
    assert windows == case["blocks"]


def test_request_bounds() -> None:
    for case in GOLDEN["request_bounds"]:
        body = {"fire_event_name": "Golden", "geometry": case["geometry"], **WINDOWS}
        request = SeverityRequest.model_validate(body)
        assert list(request.bounds()) == case["bounds"], case
