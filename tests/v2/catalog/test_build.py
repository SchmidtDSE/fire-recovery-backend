"""An offline catalog build: portolan builds it and rashid passes it."""

import json
from pathlib import Path
from typing import Dict

import pytest

from src.models.provenance import Provenance
from src.v2.catalog.build import CatalogError, build_catalog, validate
from src.v2.identity.grid import Grid
from src.v2.identity.job_id import CanonicalRequest, fire_id
from src.v2.catalog.sidecar import build_sidecar


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

    item_path = result.root / fid / "severity" / "severity.json"
    item = json.loads(item_path.read_text())
    assert sorted(item["assets"]) == ["dnbr", "rbr", "rdnbr", "thumbnail"]
    assert item["assets"]["thumbnail"]["href"] == "./rbr.thumb.jpg"
    assert item["assets"]["thumbnail"]["file:checksum"]
    assert item["properties"]["fire:job_id"] == job_id
    assert item["properties"]["fire:h3"]["cells"]
    collection = json.loads((result.root / fid / "collection.json").read_text())
    assert collection["assets"]["thumbnail"]["href"] == "./severity/rbr.thumb.jpg"
    assert (result.root / fid / "items.parquet").exists()
    assert (result.root / "README.md").exists()
    # The sidecar stays outside the catalog, so push never uploads it.
    assert not list(result.root.rglob("sidecar.json"))


def test_validate_rejects_a_broken_catalog(tmp_path: Path) -> None:
    (tmp_path / "catalog.json").write_text(json.dumps({"type": "Catalog", "id": "x"}))
    with pytest.raises(CatalogError):
        validate(tmp_path)
