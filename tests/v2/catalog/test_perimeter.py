"""A submitted perimeter's collection in its analysis's catalog."""

import json
from pathlib import Path
from typing import Any, Dict

import geopandas as gpd
import pytest

from src.models.provenance import Provenance
from src.v2.catalog.build import CatalogResult, build_catalog
from src.v2.catalog.perimeter import (
    PERIMETER_FILE,
    PMTILES_FILE,
    Perimeter,
    collection_id,
    wording,
)
from src.v2.catalog.sidecar import build_sidecar
from src.v2.identity.grid import Grid
from src.v2.identity.job_id import CanonicalRequest, fire_id

RING = [[-116.09, 33.88], [-116.02, 33.88], [-116.02, 33.93], [-116.09, 33.93],
        [-116.09, 33.88]]  # fmt: skip
HOLE = [[-116.08, 33.90], [-116.07, 33.90], [-116.07, 33.91], [-116.08, 33.90]]
GEOMETRY: Dict[str, Any] = {"type": "MultiPolygon", "coordinates": [[RING, HOLE]]}


@pytest.fixture
def built(
    tmp_path: Path,
    canonical: CanonicalRequest,
    grid: Grid,
    provenance: Provenance,
    index_cogs: Dict[str, Path],
) -> CatalogResult:
    job_id = canonical.job_id()
    fid = fire_id(canonical.fire_event_name, job_id)
    return build_catalog(
        tmp_path / "work",
        fid,
        index_cogs,
        build_sidecar(canonical, job_id, grid, provenance, index_cogs),
        title="Geology_Fire burn severity",
        description="dNBR, RdNBR and RBR for Geology_Fire.",
        item_datetime=canonical.postfire_window[1],
        source_url="https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a",
        perimeter=Perimeter(fire_event_name="Geology_Fire", geometry=GEOMETRY),
    )


def test_the_perimeter_is_a_second_collection_and_rashid_passes(
    built: CatalogResult,
) -> None:
    assert built.warnings == 0
    fid = built.root.name
    catalog = json.loads((built.root / "catalog.json").read_text())
    children = sorted(
        link["href"] for link in catalog["links"] if link["rel"] == "child"
    )
    assert children == [
        f"./{fid}-perimeter/collection.json",
        f"./{fid}/collection.json",
    ]

    collection = json.loads(
        (built.root / collection_id(fid) / "collection.json").read_text()
    )
    assert collection["id"] == collection_id(fid)
    assert collection["title"] == "Geology_Fire perimeter"
    assert "requester supplied it" in collection["description"]
    # The analysis's renders and windows are the analysis collection's alone.
    assert "renders" not in collection

    assets = collection["assets"]
    assert assets["perimeter"]["href"] == f"./{PERIMETER_FILE}"
    assert assets["perimeter"]["roles"] == ["data"]
    assert assets["perimeter-tiles"]["href"] == f"./{PMTILES_FILE}"
    assert assets["perimeter-tiles"]["roles"] == ["visual"]
    assert any(link["rel"] == "pmtiles" for link in collection["links"])
    assert [p["roles"] for p in collection["providers"]] == [
        ["producer", "processor", "host"]
    ]
    columns = {c["name"]: c for c in collection["table:columns"]}
    assert set(columns) == set(wording()["columns"])
    assert all(c["description"] for c in columns.values())


def test_each_collection_keeps_its_own_notes_and_source(built: CatalogResult) -> None:
    # The perimeter's .portolan/metadata.yaml overrides the catalog's.
    fid = built.root.name
    analysis = (built.root / fid / "README.md").read_text()
    perimeter = (built.root / collection_id(fid) / "README.md").read_text()
    assert "tippecanoe" in perimeter and "earth-search" not in perimeter
    assert "earth-search" in analysis and "tippecanoe" not in analysis


def test_the_parquet_holds_the_geometry_as_sent(built: CatalogResult) -> None:
    fid = built.root.name
    frame = gpd.read_parquet(built.root / collection_id(fid) / PERIMETER_FILE)
    assert frame.crs.to_epsg() == 4326
    (row,) = frame.itertuples()
    assert row.fire_id == fid
    assert row.fire_event_name == "Geology_Fire"
    assert row.geometry.geom_type == "MultiPolygon"
    (polygon,) = row.geometry.geoms
    assert len(polygon.interiors) == 1
    assert [list(xy) for xy in polygon.exterior.coords] == RING
