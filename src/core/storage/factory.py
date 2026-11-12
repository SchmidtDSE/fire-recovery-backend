from typing import Any
from src.core.storage.interface import StorageInterface
from src.core.storage.minio import MinioCloudStorage
from src.core.storage.memory import MemoryStorage


def get_storage(storage_type: str = "minio", **kwargs: Any) -> StorageInterface:
    """
    Factory function to get the appropriate storage implementation

    Args:
        storage_type: Type of storage ('minio', or 'memory')
        **kwargs: Additional arguments for the storage implementation

    Returns:
        StorageInterface implementation
    """
    if storage_type.lower() == "minio":
        bucket_name = kwargs.get("bucket_name")
        if not bucket_name:
            raise ValueError("bucket_name is required for Minio storage")

        return MinioCloudStorage(
            bucket_name=bucket_name,
            endpoint=kwargs.get("endpoint", "storage.googleapis.com"),
            access_key=kwargs.get("access_key"),
            secret_key=kwargs.get("secret_key"),
            region=kwargs.get("region", "auto"),
            secure=kwargs.get("secure", True),
            base_url=kwargs.get("base_url"),
        )

    elif storage_type.lower() == "memory":
        base_url = kwargs.get("base_url", "memory://")
        return MemoryStorage(base_url=base_url)

    else:
        raise ValueError(f"Unknown storage type: {storage_type}")
