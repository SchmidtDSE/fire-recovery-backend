"""The enrich hooks, run directly on STAC dicts."""

import copy
import json
from pathlib import Path
from typing import Any, Dict

import pytest

from src.v2.portolan_plugin import portolan_enrich_collection, portolan_enrich_item
from src.v2.sidecar import SIDECAR_ENV

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


def item() -> Dict[str, Any]:
    return {
        "properties": {"datetime": "2023-06-22T00:00:00Z"},
        "assets": {
            "data": {"href": "./dnbr.tif", "roles": ["data"]},
            "rbr": {"href": "./rbr.tif", "roles": ["data"]},
            "rdnbr": {"href": "./rdnbr.tif", "roles": ["data"]},
            "thumbnail": {"href": "./dnbr.thumb.jpg", "roles": ["thumbnail"]},
            "rbr.thumb": {"href": "./rbr.thumb.jpg", "roles": ["thumbnail"]},
        },
    }


@pytest.fixture
def sidecar_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "sidecar.json"
    path.write_text(json.dumps(SIDECAR))
    monkeypatch.setenv(SIDECAR_ENV, str(path))


def test_item_enrichment(sidecar_env: None) -> None:
    enriched = item()
    portolan_enrich_item(enriched, context=None)
    assets = enriched["assets"]
    assert sorted(assets) == ["dnbr", "rbr", "rdnbr", "thumbnail"]
    assert assets["thumbnail"]["href"] == "./rbr.thumb.jpg"
    assert assets["dnbr"]["title"] == "Differenced NBR (dNBR)"
    assert assets["rbr"]["bands"][0]["name"] == "rbr"
    assert assets["rbr"]["bands"][0]["unit"] == "dimensionless"
    assert enriched["properties"]["fire:job_id"] == SIDECAR["properties"]["fire:job_id"]
    assert enriched["properties"]["datetime"] == "2023-06-22T00:00:00Z"


def test_item_enrichment_is_idempotent(sidecar_env: None) -> None:
    once = item()
    portolan_enrich_item(once, context=None)
    twice = copy.deepcopy(once)
    portolan_enrich_item(twice, context=None)
    assert twice == once


def test_collection_enrichment(sidecar_env: None) -> None:
    collection = {
        "extent": {"temporal": {"interval": [[None, None]]}},
        "assets": {"thumbnail": {"href": "./severity/dnbr.thumb.jpg"}},
    }
    portolan_enrich_collection(collection, context=None)
    assert collection["extent"]["temporal"]["interval"] == [
        ["2023-06-01T00:00:00Z", "2023-06-22T23:59:59Z"]
    ]
    assert collection["assets"]["thumbnail"]["href"] == "./severity/rbr.thumb.jpg"


def test_hooks_do_nothing_without_a_sidecar(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(SIDECAR_ENV, raising=False)
    untouched = item()
    portolan_enrich_item(untouched, context=None)
    assert untouched == item()
