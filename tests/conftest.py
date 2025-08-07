import os
import uuid
import pytest
from src.core.storage.memory import MemoryStorage
from src.core.storage.interface import StorageInterface


@pytest.fixture
def memory_storage() -> StorageInterface:
    """Fixture for memory storage"""
    return MemoryStorage(base_url="memory://test")


@pytest.fixture
async def memory_storage_with_file(
    memory_storage: StorageInterface,
) -> tuple[StorageInterface, str]:
    """Fixture for memory storage with a test file"""
    test_content = b"This is test content"
    test_path = "test_file.txt"
    await memory_storage.save_bytes(test_content, test_path, temporary=True)
    return memory_storage, test_path


@pytest.fixture
def minio_available() -> bool:
    """Check if GCP MinIO credentials are available for testing"""
    # Check if required environment variables are set
    required_vars = [
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_TEST_BUCKET",
    ]
    return all(os.environ.get(var) for var in required_vars)


@pytest.fixture
def minio_storage(minio_available: bool) -> StorageInterface:
    """Fixture for GCP MinIO storage"""
    if not minio_available:
        pytest.skip("GCP MinIO credentials not available for testing")

    from src.core.storage.minio import MinioCloudStorage

    return MinioCloudStorage(
        endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        secure=os.environ.get("MINIO_SECURE", "True").lower() == "true",
        bucket_name=os.environ.get("MINIO_TEST_BUCKET", "test-bucket"),
    )


@pytest.fixture
def unique_test_id() -> str:
    """Generate a unique test identifier for test isolation"""
    return str(uuid.uuid4())


@pytest.fixture
def unique_parquet_path(unique_test_id: str) -> str:
    """Generate a unique parquet path for test isolation"""
    return f"stac/test_{unique_test_id}_fire_recovery_stac.parquet"
