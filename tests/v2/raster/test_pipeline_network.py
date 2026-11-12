"""End to end against real STAC APIs and imagery. Run with ``pytest -m network``."""

import json
from pathlib import Path

import pytest
import rasterio

from src.v2.identity.job_id import canonical_request
from src.v2.raster.pipeline import compute
from src.v2.identity.request import SeverityRequest

pytestmark = pytest.mark.network

VECTORS = Path(__file__).parents[2] / "fixtures" / "v2_job_id_vectors.json"


def test_geology_fire_sentinel2(tmp_path: Path) -> None:
    body = json.loads(VECTORS.read_text())["vectors"][0]["request_body"]
    canonical = canonical_request(SeverityRequest.model_validate(body))
    result = compute(canonical, tmp_path, threads=4)

    assert result.provenance.source.provider == "Element 84"
    assert result.provenance.prefire_scenes and result.provenance.postfire_scenes
    assert sorted(result.cogs) == ["dnbr", "rbr", "rdnbr"]
    for path in result.cogs.values():
        with rasterio.open(path) as ds:
            assert (ds.height, ds.width) == (result.grid.height, result.grid.width)
            assert ds.transform == result.grid.transform()
            assert float(ds.tags(1)["STATISTICS_VALID_PERCENT"]) > 90
