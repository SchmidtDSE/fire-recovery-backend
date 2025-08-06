from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
import uuid
from src.core.storage.factory import get_storage
from src.core.storage.interface import StorageInterface
from src.config.constants import (
    TEMP_BUCKET_NAME,
    FINAL_BUCKET_NAME,
    TEMP_STORAGE_PROVIDER_TYPE,
    FINAL_STORAGE_PROVIDER_TYPE,
)

# Initialize storage providers at module level
temp_storage_provider = get_storage(
    storage_type=TEMP_STORAGE_PROVIDER_TYPE, bucket_name=TEMP_BUCKET_NAME
)

# For permanent storage (like GCS buckets)
final_storage_provider = get_storage(
    storage_type=FINAL_STORAGE_PROVIDER_TYPE,
    bucket_name=FINAL_BUCKET_NAME,
)


def get_temp_storage() -> StorageInterface:
    """Get the temporary storage provider"""
    return temp_storage_provider


def get_final_storage() -> StorageInterface:
    """Get the permanent storage provider"""
    return final_storage_provider


@asynccontextmanager
async def temp_file(
    suffix: str = "", content: Optional[bytes] = None
) -> AsyncGenerator[str, None]:
    """Context manager for temporary files using storage provider"""
    path = None
    try:
        # Generate a unique path
        file_id = str(uuid.uuid4())
        path = f"{file_id}{suffix}"

        # Store content if provided
        if content:
            await temp_storage_provider.save_bytes(content, path, temporary=True)

        yield path
    finally:
        # Cleanup handled by storage provider's cleanup mechanism
        pass
