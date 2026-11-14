"""An offline catalog build: portolan builds it and rashid passes it."""

import json
from pathlib import Path
from typing import Dict

import pytest

from src.models.provenance import Provenance
from src.v2.catalog.build import CatalogError, build_catalog, validate
from src.v2.catalog.portolan_plugin import INDEX_BY_NAME
from src.v2.identity.grid import Grid
from src.v2.identity.job_id import CanonicalRequest, fire_id
from src.v2.catalog.sidecar import (
    PROCESSING_EXTENSION,
    SCIENTIFIC_EXTENSION,
    build_sidecar,
    missing_method_properties,
)


def test_build_catalog_passes_rashid(
    tmp_path: Path,
    canonical: CanonicalRequest,
    grid: Grid,
    provenance: Provenance,
    index_cogs: Dict[str, Path],
) -> None:
    job_id = canonical.job_id()
    fid = fire_id(canonical.fire_event_name, job_id)
    sidecar = build_sidecar(canonical, job_id, grid, provenance, index_cogs)
    result = build_catalog(
        tmp_path / "work",
        fid,
        index_cogs,
        sidecar,
        title="Geology_Fire burn severity",
        description="dNBR, RdNBR and RBR for Geology_Fire.",
        item_datetime=canonical.postfire_window[1],
        source_url="https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a",
    )
    assert result.warnings == 0

    collection_dir = result.root / fid
    for name, index in INDEX_BY_NAME.items():
        item = json.loads((collection_dir / name / f"{name}.json").read_text())
        assert sorted(item["assets"]) == [name, "thumbnail"]
        assert item["properties"]["title"] == index.title
        thumbnail = item["assets"]["thumbnail"]
        assert thumbnail["href"] == f"./{name}.thumb.jpg"
        assert thumbnail["title"].startswith(index.title)
        assert thumbnail["file:checksum"]
        assert item["properties"]["fire:job_id"] == job_id
        assert item["properties"]["fire:h3"]["cells"]
        # The method record survives portolan on every item: both extensions
        # declared, every method property present, the index's formula.
        assert {PROCESSING_EXTENSION, SCIENTIFIC_EXTENSION} <= set(
            item["stac_extensions"]
        )
        assert missing_method_properties(item["properties"]) == []
        assert item["assets"][name]["processing:expression"]["format"] == "numpy"
        assert item["assets"][name]["bands"][0]["name"] == name
    # Nothing else sits beside the items: no stray thumbnails.
    assert sorted(p.name for p in (collection_dir / "rbr").iterdir()) == [
        "rbr.json",
        "rbr.thumb.jpg",
        "rbr.tif",
    ]
    # rashid validates Scientific; Processing isn't in the Portolan registry,
    # which rashid notes once and doesn't fail.
    notes = {(f["rule_id"], f.get("actual")) for f in result.findings}
    assert ("PTL-EXT-002", PROCESSING_EXTENSION) in notes
    assert not [f for f in result.findings if f["severity"] in ("error", "warning")]
    # The README states the same method as the STAC.
    readme = (result.root / "README.md").read_text()
    assert "newest processing baseline" in readme
    collection = json.loads((collection_dir / "collection.json").read_text())
    assert collection["title"] == "Geology_Fire burn severity"
    assert collection["description"] == "dNBR, RdNBR and RBR for Geology_Fire."
    thumbnail = collection["assets"]["thumbnail"]
    assert thumbnail["href"] == "./thumbnail.jpg"
    assert thumbnail["file:checksum"]
    item_links = {
        link["href"]: link.get("title")
        for link in collection["links"]
        if link["rel"] == "item"
    }
    assert item_links == {
        f"./{name}/{name}.json": index.title for name, index in INDEX_BY_NAME.items()
    }
    assert (result.root / fid / "items.parquet").exists()
    assert (result.root / "README.md").exists()
    # The sidecar stays outside the catalog, so push never uploads it.
    assert not list(result.root.rglob("sidecar.json"))


def test_validate_rejects_a_broken_catalog(tmp_path: Path) -> None:
    (tmp_path / "catalog.json").write_text(json.dumps({"type": "Catalog", "id": "x"}))
    with pytest.raises(CatalogError):
        validate(tmp_path)
