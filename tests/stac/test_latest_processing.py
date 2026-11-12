"""One item per Sentinel-2 acquisition when a provider lists several processings."""

import copy
import json
from pathlib import Path

import pystac

from src.stac.stac_endpoint_handler import acquisition_key, latest_processing

ITEMS = Path(__file__).parents[1] / "fixtures" / "stac_items"


def item(name: str) -> pystac.Item:
    return pystac.Item.from_dict(json.loads((ITEMS / f"{name}.json").read_text()))


def reprocessed(original: pystac.Item, baseline: str) -> pystac.Item:
    """The same acquisition as ESA reprocessed it, as Element 84 lists it."""
    copy_ = copy.deepcopy(original)
    copy_.id = original.id.replace("_0_", "_1_")
    uri = original.properties["s2:product_uri"].split("_")
    uri[3], uri[6] = f"N{baseline.replace('.', '')}", "20230320T015055.SAFE"
    copy_.properties["s2:product_uri"] = "_".join(uri)
    copy_.properties["s2:processing_baseline"] = baseline
    # The two listings' timestamps differ by a millisecond.
    copy_.datetime = original.datetime.replace(  # type: ignore[union-attr]
        microsecond=original.datetime.microsecond + 1000  # type: ignore[union-attr]
    )
    return copy_


def test_the_newest_processing_of_an_acquisition_wins() -> None:
    original = item("e84_s2_2021")  # baseline 03.00
    newer = reprocessed(original, "05.00")
    assert acquisition_key(original) == acquisition_key(newer)
    assert [i.id for i in latest_processing([original, newer])] == [newer.id]
    assert [i.id for i in latest_processing([newer, original])] == [newer.id]


def test_different_acquisitions_are_all_kept() -> None:
    kept = latest_processing([item("e84_s2"), item("e84_s2_2021")])
    assert [i.id for i in kept] == [
        "S2A_11SNT_20210609_0_L2A",
        "S2A_11SNT_20230609_0_L2A",
    ]


def test_planetary_computer_items_share_the_key() -> None:
    # Planetary Computer ids differ from Element 84's, but the product URI
    # names the same acquisition.
    assert acquisition_key(item("pc_s2")) == (
        "S2A",
        "20230609T181921",
        "R127",
        "T11SNT",
    )
    assert acquisition_key(item("e84_s2")) == acquisition_key(item("pc_s2"))


def test_items_without_a_product_uri_pass_through() -> None:
    landsat = item("pc_landsat")
    assert acquisition_key(landsat) is None
    kept = latest_processing([landsat, copy.deepcopy(landsat)])
    assert [i.id for i in kept] == [landsat.id, landsat.id]
