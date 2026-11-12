"""Reflectance scaling, odc-stac band configuration and the per-block item filter."""

import json
from pathlib import Path

import pystac
import pytest

from src.stac.stac_endpoint_handler import StacProviderConfig
from src.v2.identity.grid import Grid
from src.v2.raster.load import (
    asset_scaling,
    geobox,
    intersecting,
    scaling_groups,
    stac_cfg,
)
from src.v2.sensors import fallback_scaling

ITEMS = Path(__file__).parents[2] / "fixtures" / "stac_items"
CONFIG = StacProviderConfig.load_from_file()
E84_S2, MPC_S2 = CONFIG.get_providers("sentinel-2")
MPC_LANDSAT = CONFIG.get_providers("landsat")[0]


def item(name: str) -> pystac.Item:
    return pystac.Item.from_dict(json.loads((ITEMS / f"{name}.json").read_text()))


@pytest.mark.parametrize(
    "name, key, sensor, expected",
    [
        ("e84_s2", "nir08", "sentinel-2", (1e-4, -0.1)),
        ("e84_s2_2021", "swir22", "sentinel-2", (1e-4, 0.0)),
        # Planetary Computer's S2 items publish no raster:bands: the fallback
        # reads s2:processing_baseline (05.10 and 02.12 here).
        ("pc_s2", "B8A", "sentinel-2", (1e-4, -0.1)),
        ("pc_s2_2021", "B12", "sentinel-2", (1e-4, 0.0)),
        ("pc_landsat", "nir08", "landsat", (2.75e-5, -0.2)),
    ],
)
def test_asset_scaling(name: str, key: str, sensor: str, expected: tuple) -> None:
    scale, offset = asset_scaling(item(name), key, sensor)
    assert (scale, offset) == pytest.approx(expected)


def test_one_scaling_is_one_group() -> None:
    (group,) = scaling_groups([item("e84_s2")], E84_S2, "sentinel-2")
    assert (group.scale, group.offset) == pytest.approx((1e-4, -0.1))
    assert [i.id for i in group.items] == ["S2A_11SNT_20230609_0_L2A"]


def test_a_window_across_the_baseline_change_groups_by_scaling() -> None:
    # 2021 (baseline 03.00, offset 0) and 2023 (05.09, offset -0.1): each
    # scene keeps its own pair, newer convention first.
    new, old = scaling_groups(
        [item("e84_s2_2021"), item("e84_s2")], E84_S2, "sentinel-2"
    )
    assert (new.offset, [i.id for i in new.items]) == (
        pytest.approx(-0.1),
        ["S2A_11SNT_20230609_0_L2A"],
    )
    assert (old.offset, [i.id for i in old.items]) == (
        0.0,
        ["S2A_11SNT_20210609_0_L2A"],
    )


def test_a_scene_whose_bands_scale_differently_is_rejected() -> None:
    odd = item("e84_s2")
    odd.assets["swir22"].extra_fields["raster:bands"][0]["offset"] = 0.0
    with pytest.raises(ValueError, match="scales its NIR and SWIR differently"):
        scaling_groups([odd], E84_S2, "sentinel-2")


def test_missing_asset_is_an_error() -> None:
    with pytest.raises(ValueError, match="no 'B8A' asset"):
        asset_scaling(item("e84_s2"), "B8A", "sentinel-2")


def test_sentinel2_fallback_needs_a_baseline() -> None:
    with pytest.raises(ValueError, match="processing_baseline"):
        fallback_scaling("sentinel-2", {})


def test_stac_cfg_sets_native_types_for_every_band() -> None:
    cfg = stac_cfg(MPC_S2, "sentinel-2")
    assert cfg == {
        "sentinel-2-l2a": {
            "assets": {
                "B8A": {"data_type": "uint16", "nodata": 0},
                "B12": {"data_type": "uint16", "nodata": 0},
                "SCL": {"data_type": "uint8", "nodata": 0},
            }
        }
    }
    landsat = stac_cfg(MPC_LANDSAT, "landsat")["landsat-c2-l2"]["assets"]
    assert landsat["qa_pixel"] == {"data_type": "uint16", "nodata": 1}


def test_geobox_matches_the_grid() -> None:
    grid = Grid(epsg=32611, resolution=20, bounds=(581820, 3748140, 592080, 3756340))
    box = geobox(grid)
    assert (box.shape.y, box.shape.x) == (grid.height, grid.width)
    assert box.affine == grid.transform()
    assert box.crs.epsg == 32611


def test_intersecting_filters_by_geometry() -> None:
    s2 = item("e84_s2")  # tile 11SNT
    assert intersecting([s2], (-116.1, 33.8, -116.0, 33.9)) == [s2]
    assert intersecting([s2], (-110.0, 40.0, -109.9, 40.1)) == []
