"""The enrich hooks, run directly on STAC dicts."""

import copy
import json
from pathlib import Path
from typing import Any, Dict

import pytest

from src.v2.catalog.portolan_plugin import (
    INDEX_BY_NAME,
    RENDER_EXTENSION,
    portolan_enrich_collection,
    portolan_enrich_item,
)
from src.v2.catalog.sidecar import SIDECAR_ENV

SIDECAR = {
    "properties": {
        "start_datetime": "2023-06-01T00:00:00Z",
        "end_datetime": "2023-06-22T23:59:59Z",
        "fire:job_id": "bbf1eb5e-fbf0-527a-a51a-b5575941fbb0",
    },
    "bands": {
        name: [{"data_type": "float32", "nodata": -9999.0, "statistics": {"mean": 0.1}}]
        for name in ("dnbr", "rdnbr", "rbr")
    },
}


def item(index: str = "rbr") -> Dict[str, Any]:
    """An item as portolan writes it for one index's folder."""
    return {
        "id": index,
        "properties": {"datetime": "2023-06-22T00:00:00Z"},
        "assets": {
            "data": {"href": f"./{index}.tif", "roles": ["data"]},
            "thumbnail": {"href": f"./{index}.thumb.jpg", "roles": ["thumbnail"]},
        },
    }


@pytest.fixture
def sidecar_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "sidecar.json"
    path.write_text(json.dumps(SIDECAR))
    monkeypatch.setenv(SIDECAR_ENV, str(path))


@pytest.mark.parametrize("name", ["dnbr", "rdnbr", "rbr"])
def test_item_enrichment(sidecar_env: None, name: str) -> None:
    index = INDEX_BY_NAME[name]
    enriched = item(name)
    portolan_enrich_item(enriched, context=None)
    assets = enriched["assets"]
    assert sorted(assets) == sorted([name, "thumbnail"])
    assert assets["thumbnail"]["href"] == f"./{name}.thumb.jpg"
    assert assets["thumbnail"]["title"].startswith(index.title)
    assert assets[name]["title"] == index.title
    assert assets[name]["bands"][0]["name"] == name
    assert assets[name]["bands"][0]["unit"] == "dimensionless"
    properties = enriched["properties"]
    assert properties["title"] == index.title
    assert properties["fire:job_id"] == SIDECAR["properties"]["fire:job_id"]
    assert properties["datetime"] == "2023-06-22T00:00:00Z"


def test_item_not_named_after_an_index_is_left_alone(sidecar_env: None) -> None:
    other = item("rbr")
    other["id"] = "severity"
    portolan_enrich_item(other, context=None)
    assert "data" in other["assets"]
    assert "title" not in other["properties"]


def test_item_enrichment_is_idempotent(sidecar_env: None) -> None:
    once = item()
    portolan_enrich_item(once, context=None)
    twice = copy.deepcopy(once)
    portolan_enrich_item(twice, context=None)
    assert twice == once


def test_collection_enrichment(sidecar_env: None) -> None:
    collection: Dict[str, Any] = {"extent": {"temporal": {"interval": [[None, None]]}}}
    portolan_enrich_collection(collection, context=None)
    assert collection["extent"]["temporal"]["interval"] == [
        ["2023-06-01T00:00:00Z", "2023-06-22T23:59:59Z"]
    ]
    assert collection["stac_extensions"] == [RENDER_EXTENSION]
    assert collection["renders"]["rbr"] == {
        "title": "Relativized Burn Ratio (RBR)",
        "assets": ["rbr"],
        "rescale": [[0.0, 0.5]],
        "colormap_name": "ylorrd",
    }
    # One render per index, each drawing only its own asset.
    assert {name: r["assets"] for name, r in collection["renders"].items()} == {
        name: [name] for name in INDEX_BY_NAME
    }
    again = copy.deepcopy(collection)
    portolan_enrich_collection(again, context=None)
    assert again == collection


def test_hooks_do_nothing_without_a_sidecar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(SIDECAR_ENV, raising=False)
    untouched = item()
    portolan_enrich_item(untouched, context=None)
    assert untouched == item()
