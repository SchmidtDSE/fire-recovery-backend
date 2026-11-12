"""Shared builders for v2 tests: a canonical request, a grid, provenance and COGs."""

import json
from pathlib import Path
from typing import Dict

import numpy as np
import pytest
import rasterio

from src.models.provenance import Provenance, Scene, SourceData
from src.stac.stac_endpoint_handler import StacProviderConfig
from src.v2.cog import write_cog
from src.v2.grid import Grid, raster_grid
from src.v2.job_id import CanonicalRequest, canonical_request
from src.v2.request import SeverityRequest

VECTORS = Path(__file__).parent.parent / "fixtures" / "v2_job_id_vectors.json"


@pytest.fixture
def canonical() -> CanonicalRequest:
    body = json.loads(VECTORS.read_text())["vectors"][0]["request_body"]
    return canonical_request(SeverityRequest.model_validate(body))


@pytest.fixture
def grid(canonical: CanonicalRequest) -> Grid:
    return raster_grid(canonical.tiles, canonical.sensor)


@pytest.fixture
def provenance() -> Provenance:
    provider = StacProviderConfig.load_from_file().get_providers("sentinel-2")[0]

    def scene(day: int) -> Scene:
        item_id = f"S2A_11SNT_202306{day:02d}_0_L2A"
        return Scene(
            id=item_id,
            datetime=f"2023-06-{day:02d}T18:40:00Z",
            href=f"{provider.url}collections/sentinel-2-l2a/items/{item_id}",
        )

    return Provenance(
        source=SourceData.from_mapping(provider, "sentinel-2"),
        prefire_scenes=[scene(2), scene(7)],
        postfire_scenes=[scene(19)],
    )


def write_index_cogs(directory: Path, grid: Grid, seed: int = 0) -> Dict[str, Path]:
    """Three plausible float32 index COGs on ``grid``, with a nodata border."""
    rng = np.random.default_rng(seed)
    directory.mkdir(parents=True, exist_ok=True)
    out = {}
    for name, centre in (("dnbr", 0.1), ("rdnbr", 0.3), ("rbr", 0.08)):
        data = rng.normal(centre, 0.05, (grid.height, grid.width)).astype(np.float32)
        data[:5] = -9999.0
        raw = directory / f"{name}_raw.tif"
        with rasterio.open(
            raw,
            "w",
            driver="GTiff",
            width=grid.width,
            height=grid.height,
            count=1,
            dtype="float32",
            nodata=-9999.0,
            crs=f"EPSG:{grid.epsg}",
            transform=grid.transform(),
            tiled=True,
            blockxsize=512,
            blockysize=512,
        ) as dst:
            dst.write(data, 1)
            dst.set_band_description(1, name)
        out[name] = write_cog(raw, directory / f"{name}.tif")
        raw.unlink()
    return out


@pytest.fixture
def index_cogs(tmp_path: Path, grid: Grid) -> Dict[str, Path]:
    return write_index_cogs(tmp_path / "cogs", grid)
