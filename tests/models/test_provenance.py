"""The provenance record written into each item's properties."""

import json
from pathlib import Path

import pystac

from src.models.provenance import Provenance, Scene, SourceData
from src.stac.stac_endpoint_handler import StacProviderConfig

ITEM = pystac.Item.from_dict(
    json.loads(
        (
            Path(__file__).parent.parent / "fixtures" / "stac_items" / "e84_s2.json"
        ).read_text()
    )
)


def test_scene_from_item() -> None:
    scene = Scene.from_item(ITEM)
    assert scene.id == "S2A_11SNT_20230609_0_L2A"
    assert scene.datetime.startswith("2023-06-09T") and scene.datetime.endswith("Z")
    assert scene.href is not None and scene.href.endswith(ITEM.id)


def test_source_from_mapping() -> None:
    provider = StacProviderConfig.load_from_file().get_providers("sentinel-2")[1]
    source = SourceData.from_mapping(provider, "sentinel-2")
    assert source.provider == "Microsoft Planetary Computer"
    assert source.provider_id == "MICROSOFT_PLANETARY_COMPUTER"
    assert source.api == provider.url
    assert source.collection == "sentinel-2-l2a"
    assert source.bands == {"nir": "B8A", "swir": "B12", "mask": "SCL"}


def test_stac_properties() -> None:
    provider = StacProviderConfig.load_from_file().get_providers("sentinel-2")[0]
    provenance = Provenance(
        source=SourceData.from_mapping(provider, "sentinel-2"),
        prefire_scenes=[Scene.from_item(ITEM)],
        postfire_scenes=[],
    )
    props = provenance.to_stac_properties()
    assert set(props) == {"fire:source", "fire:prefire_scenes", "fire:postfire_scenes"}
    assert props["fire:prefire_scenes"][0]["id"] == ITEM.id
    assert props["fire:source"]["sensor"] == "sentinel-2"
