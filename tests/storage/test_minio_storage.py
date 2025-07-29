import pytest
import json
import uuid


@pytest.mark.asyncio
async def test_minio_save_and_get_bytes(minio_storage):
    """Test saving and retrieving binary data with GCP MinIO"""
    # Use unique path to avoid conflicts between test runs
    test_id = str(uuid.uuid4())
    test_data = b"Hello, MinIO world!"
    test_path = f"test_files/{test_id}/test_file.bin"

    try:
        # Save data
        url = await minio_storage.save_bytes(test_data, test_path)

        # URL should contain the bucket name
        assert minio_storage.bucket_name in url

        # Retrieve data
        retrieved_data = await minio_storage.get_bytes(test_path)
        assert retrieved_data == test_data
    finally:
        # Clean up the specific test file
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_save_and_get_json(minio_storage):
    """Test saving and retrieving JSON data with GCP MinIO"""
    # Use unique path to avoid conflicts between test runs
    test_id = str(uuid.uuid4())
    test_data = {"key": "value", "nested": {"key2": 42}}
    test_path = f"test_files/{test_id}/test_file.json"

    try:
        # Save data
        url = await minio_storage.save_json(test_data, test_path)

        # Retrieve data
        retrieved_data = await minio_storage.get_json(test_path)
        assert retrieved_data == test_data
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_list_files(minio_storage):
    """Test listing files with prefix in GCP MinIO"""
    # Use unique prefix to avoid conflicts between test runs
    test_id = str(uuid.uuid4())
    prefix = f"test_files/{test_id}/prefix"

    try:
        # Save multiple files
        await minio_storage.save_bytes(b"data1", f"{prefix}/file1.bin")
        await minio_storage.save_bytes(b"data2", f"{prefix}/file2.bin")
        await minio_storage.save_bytes(b"data3", f"{prefix}/../other/file3.bin")

        # List files with prefix
        files = await minio_storage.list_files(f"{prefix}/")
        assert len(files) == 2
        assert f"{prefix}/file1.bin" in files
        assert f"{prefix}/file2.bin" in files
        assert f"{prefix}/../other/file3.bin" not in files
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_copy_from_url(minio_storage):
    """Test copying from URL to GCP MinIO storage"""
    # Use unique path to avoid conflicts between test runs
    test_id = str(uuid.uuid4())
    url = "https://raw.githubusercontent.com/pytorch/pytorch/main/README.md"
    target_path = f"test_files/{test_id}/downloaded_readme.md"

    try:
        result_url = await minio_storage.copy_from_url(url, target_path)

        # Verify the file was saved
        retrieved_data = await minio_storage.get_bytes(target_path)
        assert len(retrieved_data) > 0  # Should have content
        assert b"PyTorch" in retrieved_data  # Should contain this text
    finally:
        # Clean up
        await minio_storage.cleanup()
