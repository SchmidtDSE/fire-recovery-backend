"""Publishing to a real S3-compatible store (RustFS in CI).

Opt-in: runs only with RUN_STORAGE_TESTS=1 and an explicit S3_ENDPOINT, and
never against the production bucket, so local credentials meant for
production can't be written with.
"""

import os
from pathlib import Path

import numpy as np
import obstore
import pytest
import rasterio

from src.models.provenance import Provenance
from src.v2.catalog import build_catalog
from src.v2.grid import Grid
from src.v2.job_id import CanonicalRequest, fire_id
from src.v2.publish import catalog_prefix, publish
from src.v2.sidecar import build_sidecar
from src.v2.store import DEFAULT_BUCKET, V1_BUCKET, Store, StoreSettings
from tests.v2.conftest import write_index_cogs

pytestmark = pytest.mark.storage

MULTIPART_THRESHOLD = 5 * 2**20


@pytest.fixture
def store() -> Store:
    if os.environ.get("RUN_STORAGE_TESTS") != "1" or not os.environ.get("S3_ENDPOINT"):
        pytest.skip("set RUN_STORAGE_TESTS=1 and S3_ENDPOINT to run storage tests")
    settings = StoreSettings.from_env()
    if settings.bucket in (DEFAULT_BUCKET, V1_BUCKET):
        pytest.skip("refusing to run storage tests against a production bucket")
    return Store(settings)


def test_push_a_catalog_with_a_large_cog(
    tmp_path: Path,
    store: Store,
    canonical: CanonicalRequest,
    provenance: Provenance,
) -> None:
    # Noise doesn't compress, so ~1.6 Mpx of float32 gives a COG over 5 MiB and
    # push uploads it in parts.
    grid = Grid(epsg=32611, resolution=20, bounds=(560000, 3740000, 585600, 3765600))
    cogs = write_index_cogs(tmp_path / "cogs", grid)
    assert cogs["rbr"].stat().st_size > MULTIPART_THRESHOLD
    rbr_size = cogs["rbr"].stat().st_size

    job_id = canonical.job_id()
    fid = f"storage-test-{fire_id(canonical.fire_event_name, job_id)}"
    sidecar = build_sidecar(canonical, job_id, grid, provenance, cogs)
    built = build_catalog(
        tmp_path / "work",
        fid,
        cogs,
        sidecar,
        title="Storage test",
        description="Push check.",
        item_datetime=canonical.postfire_window[1],
        source_url=None,
    )
    prefix = catalog_prefix(fid)
    try:
        url = publish(built.root, fid, store)
        assert url.endswith(f"{prefix}/catalog.json")
        assert store.get_json(f"{prefix}/catalog.json")["id"] == fid
        cog_key = f"{prefix}/{fid}/severity/rbr.tif"
        head = obstore.head(store._store, cog_key)
        assert head["size"] == rbr_size
        # A range read, as a COG reader makes.
        first = bytes(obstore.get_range(store._store, cog_key, start=0, end=16))
        assert first[:2] in (b"II", b"MM")

        # Publishing again empties the prefix and succeeds.
        publish(built.root, fid, store)
        assert store.get_json(f"{prefix}/catalog.json")["id"] == fid
    finally:
        store.delete_prefix(f"{prefix}/")
    with rasterio.open(built.root / fid / "severity" / "rbr.tif") as ds:
        assert np.isfinite(ds.read(1, window=((10, 20), (10, 20)))).all()
