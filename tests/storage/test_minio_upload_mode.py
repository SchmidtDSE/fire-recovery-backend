"""
How ``save_bytes`` puts objects, as opposed to whether the bytes survive.

Round-trip coverage lives in ``test_minio_storage.py`` and runs against the
MinIO service container. It cannot cover this: MinIO implements S3 multipart
correctly, so a large upload succeeds there whether or not the transfer mode is
right. The mode only matters against GCS's S3-compatibility endpoint, which
rejects the multipart-initiate POST with ``411 Length Required`` -- and there is
no GCS in CI. So these assert on the call obstore receives.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.core.storage.minio import MinioCloudStorage

# obstore's own default. Anything larger takes the multipart path when
# use_multipart is left to obstore to decide.
OBSTORE_CHUNK_SIZE = 5 * 1024 * 1024


@pytest.fixture
def storage() -> MinioCloudStorage:
    return MinioCloudStorage(
        bucket_name="test-bucket",
        endpoint="storage.googleapis.com",
        access_key="key",
        secret_key="secret",
    )


@pytest.fixture
def put_async():
    with patch("src.core.storage.minio.obs.put_async", new=AsyncMock()) as mock:
        yield mock


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "size",
    [
        pytest.param(16, id="tiny"),
        pytest.param(OBSTORE_CHUNK_SIZE - 1, id="just_under_chunk_size"),
        pytest.param(OBSTORE_CHUNK_SIZE + 1, id="just_over_chunk_size"),
        pytest.param(4 * OBSTORE_CHUNK_SIZE, id="several_chunks"),
    ],
)
async def test_never_uses_multipart(storage, put_async, size: int) -> None:
    """Every size takes a single PUT, including well past obstore's threshold.

    Sizes bracket the 5 MiB switchover because that is the only thing obstore
    consults when the caller stays silent, and it is what made the GCS failure
    look size-dependent and therefore intermittent.
    """
    await storage.save_bytes(b"x" * size, "assets/job/fire_severity/dnbr.tif")

    assert put_async.await_args.kwargs["use_multipart"] is False


@pytest.mark.asyncio
async def test_temporary_uploads_also_avoid_multipart(storage, put_async) -> None:
    """The temp/ prefix rewrites the path; it must not bypass the mode."""
    await storage.save_bytes(
        b"x" * (OBSTORE_CHUNK_SIZE + 1), "scratch.tif", temporary=True
    )

    args, kwargs = put_async.await_args
    assert args[1] == "temp/scratch.tif"
    assert kwargs["use_multipart"] is False


@pytest.mark.asyncio
async def test_save_json_inherits_the_mode(storage, put_async) -> None:
    """save_json delegates to save_bytes, so it must not need its own fix."""
    await storage.save_json({"key": "value" * 10}, "stac/fire/item.json")

    assert put_async.await_args.kwargs["use_multipart"] is False
