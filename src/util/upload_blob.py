from src.core.storage.storage_factory import StorageFactory


async def upload_to_gcs(
    source_path_or_bytes: str | bytes,
    destination_blob_name: str,
    storage_factory: StorageFactory,
) -> str:
    """
    Upload a file or bytes to GCS using the storage provider

    Args:
        source_path_or_bytes: Either a path in the storage system or bytes content
        destination_blob_name: Destination blob name
        storage_factory: Storage factory instance for getting storage providers

    Returns:
        Public URL of the uploaded blob
    """
    temp_storage = storage_factory.get_temp_storage()
    permanent_storage = storage_factory.get_final_storage()

    # If source is bytes, save directly to permanent storage
    if isinstance(source_path_or_bytes, bytes):
        return await permanent_storage.save_bytes(
            source_path_or_bytes, destination_blob_name
        )

    # If source is a path in our temporary storage, get the bytes and save to permanent
    content = await temp_storage.get_bytes(source_path_or_bytes)
    return await permanent_storage.save_bytes(content, destination_blob_name)
