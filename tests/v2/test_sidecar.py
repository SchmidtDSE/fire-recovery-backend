"""The sidecar: item properties, H3 cells and band statistics."""

from pathlib import Path
from typing import Dict

import h3
import pytest

from src.models.provenance import Provenance
from src.v2.grid import Grid
from src.v2.job_id import CanonicalRequest
from src.v2.sidecar import band_metadata, build_sidecar, h3_cover


@pytest.mark.parametrize(
    "bounds, resolution",
    [
        ((-116.115, 33.870, -116.005, 33.944), 7),  # ~10 km
        ((-121.2, 39.4, -120.05, 40.3), 4),  # ~100 km
    ],
)
def test_h3_cover_scales_with_the_area(bounds: tuple, resolution: int) -> None:
    res, cells = h3_cover(bounds)
    assert res == resolution
    # Levels step 7x in area, so the count varies with where a size falls.
    assert 5 <= len(cells) <= 60
    assert cells == sorted(cells)
    assert all(h3.get_resolution(cell) == res for cell in cells)
    centre = ((bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2)
    assert h3.latlng_to_cell(*centre, res) in cells


def test_band_metadata(index_cogs: Dict[str, Path]) -> None:
    band = band_metadata(index_cogs["rbr"])
    assert band["data_type"] == "float32"
    assert band["nodata"] == -9999.0
    assert set(band["statistics"]) == {
        "minimum",
        "maximum",
        "mean",
        "stddev",
        "valid_percent",
    }
    assert band["statistics"]["mean"] == pytest.approx(0.08, abs=0.01)


def test_build_sidecar(
    canonical: CanonicalRequest,
    grid: Grid,
    provenance: Provenance,
    index_cogs: Dict[str, Path],
) -> None:
    job_id = canonical.job_id()
    sidecar = build_sidecar(canonical, job_id, grid, provenance, index_cogs)
    props = sidecar["properties"]
    assert props["start_datetime"] == "2023-06-01T00:00:00Z"
    assert props["end_datetime"] == "2023-06-22T23:59:59Z"
    assert props["fire:job_id"] == job_id
    assert props["fire:event_name"] == "Geology_Fire"
    assert props["fire:pipeline_version"] == canonical.pipeline_version
    assert props["fire:prefire_window"] == ["2023-06-01", "2023-06-09"]
    assert props["fire:grid"]["epsg"] == grid.epsg
    assert props["fire:grid"]["tiles"] == canonical.tiles.model_dump()
    assert props["fire:mask"]["band"] == "scl"
    assert props["fire:mask"]["values"] == [0, 1, 3, 8, 9, 10, 11]
    assert props["fire:h3"]["cells"]
    assert [s["id"] for s in props["fire:prefire_scenes"]] == [
        "S2A_11SNT_20230602_0_L2A",
        "S2A_11SNT_20230607_0_L2A",
    ]
    assert props["fire:source"]["provider"] == "Element 84"
    assert set(sidecar["bands"]) == {"dnbr", "rdnbr", "rbr"}
    # Nothing in the sidecar depends on the submitted perimeter.
    assert "fire:buffer_m" not in props
