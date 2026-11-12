import pytest
import uuid
import json
from typing import BinaryIO
from src.core.storage.minio import MinioCloudStorage


@pytest.mark.asyncio
async def test_minio_save_and_get_bytes(minio_storage: MinioCloudStorage) -> None:
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
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_save_and_get_json(minio_storage: MinioCloudStorage) -> None:
    """Test saving and retrieving JSON data with GCP MinIO"""
    # Use unique path to avoid conflicts between test runs
    test_id = str(uuid.uuid4())
    test_data = {"key": "value", "nested": {"key2": 42}}
    test_path = f"test_files/{test_id}/test_file.json"

    try:
        # Save data
        __url = await minio_storage.save_json(test_data, test_path)

        # Retrieve data
        retrieved_data = await minio_storage.get_json(test_path)
        assert retrieved_data == test_data
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_list_files(minio_storage: MinioCloudStorage) -> None:
    """Test listing files with prefix in GCP MinIO"""
    # Use unique prefix to avoid conflicts between test runs
    test_id = str(uuid.uuid4())
    prefix = f"test_files/{test_id}/prefix"

    try:
        # Save multiple files
        await minio_storage.save_bytes(b"data1", f"{prefix}/file1.bin")
        await minio_storage.save_bytes(b"data2", f"{prefix}/file2.bin")

        # List files with prefix
        files = await minio_storage.list_files(f"{prefix}/")
        assert len(files) == 2
        assert f"{prefix}/file1.bin" in files
        assert f"{prefix}/file2.bin" in files
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_copy_from_url(minio_storage: MinioCloudStorage) -> None:
    """Test copying from URL to GCP MinIO storage"""
    # Use unique path to avoid conflicts between test runs
    test_id = str(uuid.uuid4())
    url = "https://raw.githubusercontent.com/pytorch/pytorch/main/README.md"
    target_path = f"test_files/{test_id}/downloaded_readme.md"

    try:
        __result_url = await minio_storage.copy_from_url(url, target_path)

        # Verify the file was saved
        retrieved_data = await minio_storage.get_bytes(target_path)
        assert len(retrieved_data) > 0  # Should have content
        assert b"PyTorch" in retrieved_data  # Should contain this text
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_save_bytes_temporary_flag(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test saving binary data with temporary flag in MinIO"""
    test_id = str(uuid.uuid4())
    test_data = b"Temporary MinIO data"
    temp_path = f"test_files/{test_id}/temp_file.bin"
    perm_path = f"test_files/{test_id}/perm_file.bin"

    try:
        # Save temporary data (should go to temp/ directory)
        temp_url = await minio_storage.save_bytes(test_data, temp_path, temporary=True)
        assert minio_storage.bucket_name in temp_url
        assert "/temp/" in temp_url  # Should be prefixed with temp/

        # Save permanent data
        perm_url = await minio_storage.save_bytes(test_data, perm_path, temporary=False)
        assert minio_storage.bucket_name in perm_url
        assert "/temp/" not in perm_url  # Should NOT be prefixed with temp/

        # Verify both can be retrieved
        temp_retrieved = await minio_storage.get_bytes(f"temp/{temp_path}")
        perm_retrieved = await minio_storage.get_bytes(perm_path)
        assert temp_retrieved == test_data
        assert perm_retrieved == test_data

        # List files to verify locations
        temp_files = await minio_storage.list_files("temp/")
        perm_files = await minio_storage.list_files(f"test_files/{test_id}/")

        assert any(f"temp/{temp_path}" in f for f in temp_files)
        assert perm_path in perm_files
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_save_json_temporary_flag(minio_storage: MinioCloudStorage) -> None:
    """Test saving JSON data with temporary flag in MinIO"""
    test_id = str(uuid.uuid4())
    test_data = {"test": "temporary json", "id": test_id}
    temp_path = f"test_files/{test_id}/temp_data.json"
    perm_path = f"test_files/{test_id}/perm_data.json"

    try:
        # Save temporary JSON (should go to temp/ directory)
        temp_url = await minio_storage.save_json(test_data, temp_path, temporary=True)
        assert minio_storage.bucket_name in temp_url
        assert "/temp/" in temp_url

        # Save permanent JSON
        perm_url = await minio_storage.save_json(test_data, perm_path, temporary=False)
        assert minio_storage.bucket_name in perm_url
        assert "/temp/" not in perm_url

        # Verify both can be retrieved
        temp_retrieved = await minio_storage.get_json(f"temp/{temp_path}")
        perm_retrieved = await minio_storage.get_json(perm_path)
        assert temp_retrieved == test_data
        assert perm_retrieved == test_data
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_copy_from_url_temporary_flag(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test copying from URL with temporary flag in MinIO"""
    test_id = str(uuid.uuid4())
    url = "https://raw.githubusercontent.com/pytorch/pytorch/main/README.md"
    temp_path = f"test_files/{test_id}/temp_readme.md"
    perm_path = f"test_files/{test_id}/perm_readme.md"

    try:
        # Copy as temporary (should go to temp/ directory)
        temp_url = await minio_storage.copy_from_url(url, temp_path, temporary=True)
        assert minio_storage.bucket_name in temp_url
        assert "/temp/" in temp_url

        # Copy as permanent
        perm_url = await minio_storage.copy_from_url(url, perm_path, temporary=False)
        assert minio_storage.bucket_name in perm_url
        assert "/temp/" not in perm_url

        # Verify both files exist and have content
        temp_data = await minio_storage.get_bytes(f"temp/{temp_path}")
        perm_data = await minio_storage.get_bytes(perm_path)
        assert len(temp_data) > 0
        assert len(perm_data) > 0
        assert b"PyTorch" in temp_data
        assert b"PyTorch" in perm_data
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_process_stream_geospatial_workflow(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test MinIO process_stream with realistic geospatial data processing workflow"""
    test_id = str(uuid.uuid4())

    # Simulate fire severity COG data
    mock_cog_data = b"MOCK_COG_DATA: Fire severity raster for spatial analysis"
    source_path = f"test_files/{test_id}/fire_severity_source.tif"

    try:
        # Save source COG data
        await minio_storage.save_bytes(mock_cog_data, source_path)

        # Simulate spatial cropping processor (like crop_cog_with_geometry)
        def spatial_crop_processor(file_obj: BinaryIO) -> bytes:
            """Simulate cropping raster with fire boundary geometry"""
            # In real scenario:
            # 1. rioxarray.open_rasterio(file_obj)
            # 2. data.rio.clip([fire_boundary_geom])
            # 3. Return cropped raster bytes
            original_data = file_obj.read()
            cropped_data = b"CROPPED_FIRE_BOUNDARY:" + original_data
            return cropped_data

        processed_path = f"test_files/{test_id}/cropped_fire_severity.tif"

        # Process using MinIO stream (avoiding temp files)
        result_url = await minio_storage.process_stream(
            source_path, spatial_crop_processor, processed_path, temporary=True
        )

        # Verify processing worked
        assert minio_storage.bucket_name in result_url
        assert "/temp/" in result_url  # Should be in temp/ since temporary=True

        processed_data = await minio_storage.get_bytes(f"temp/{processed_path}")
        assert processed_data.startswith(b"CROPPED_FIRE_BOUNDARY:")
        assert b"Fire severity raster" in processed_data
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_process_stream_cog_creation_workflow(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test MinIO process_stream simulating COG creation from xarray data"""
    test_id = str(uuid.uuid4())

    # Simulate xarray-processed data ready for COG conversion
    xarray_data = {
        "fire_analysis": "processed fire severity metrics",
        "crs": "EPSG:4326",
    }
    source_path = f"test_files/{test_id}/xarray_processed.json"

    try:
        # Save xarray data as JSON
        await minio_storage.save_json(xarray_data, source_path)

        # Simulate COG creation processor (like create_cog function)
        def cog_creation_processor(file_obj: BinaryIO) -> bytes:
            """Simulate creating COG from processed xarray data"""
            # In real scenario:
            # 1. Load xarray DataArray from processed data
            # 2. Use rio_cogeo.cog_translate to create optimized GeoTIFF
            # 3. Return COG bytes
            json_data = json.loads(file_obj.read().decode())
            cog_header = b"CLOUD_OPTIMIZED_GEOTIFF:"
            cog_metadata = json.dumps(json_data).encode()
            return cog_header + cog_metadata

        cog_path = f"test_files/{test_id}/final_fire_severity.cog.tif"

        # Create COG using MinIO stream processing (permanent output)
        result_url = await minio_storage.process_stream(
            source_path, cog_creation_processor, cog_path, temporary=False
        )

        # Verify COG creation worked
        assert minio_storage.bucket_name in result_url
        assert (
            "/temp/" not in result_url
        )  # Should NOT be in temp/ since temporary=False

        cog_data = await minio_storage.get_bytes(cog_path)
        assert cog_data.startswith(b"CLOUD_OPTIMIZED_GEOTIFF:")
        assert b"fire_analysis" in cog_data
    finally:
        # Clean up
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_minio_cleanup_temporary_files(minio_storage: MinioCloudStorage) -> None:
    """Test MinIO cleanup functionality with temporary and permanent files"""
    test_id = str(uuid.uuid4())

    try:
        # Create a mix of temporary and permanent files
        # Permanent files (final outputs)
        await minio_storage.save_bytes(
            b"Final COG output",
            f"outputs/{test_id}/fire_severity_final.cog.tif",
            temporary=False,
        )
        await minio_storage.save_json(
            {"results": "final analysis"},
            f"outputs/{test_id}/analysis_final.json",
            temporary=False,
        )

        # Temporary files (intermediate processing)
        await minio_storage.save_bytes(
            b"Raw downloaded COG", f"processing/{test_id}/raw_cog.tif", temporary=True
        )
        await minio_storage.save_bytes(
            b"Intermediate cropped",
            f"processing/{test_id}/intermediate.tif",
            temporary=True,
        )
        await minio_storage.save_json(
            {"temp": "processing metadata"},
            f"processing/{test_id}/temp_meta.json",
            temporary=True,
        )

        # List all files before cleanup
        __all_files = await minio_storage.list_files("")
        temp_files = await minio_storage.list_files("temp/")
        output_files = await minio_storage.list_files(f"outputs/{test_id}/")

        # Should have files in both temp/ and outputs/ directories
        assert len(temp_files) >= 3  # At least our 3 temp files
        assert len(output_files) == 2  # Exactly our 2 permanent files

        # Clean up temporary files
        removed_count = await minio_storage.cleanup()
        assert removed_count >= 3  # Should remove at least our 3 temp files

        # Verify temporary files are gone but permanent files remain
        remaining_temp_files = await minio_storage.list_files("temp/")
        remaining_output_files = await minio_storage.list_files(f"outputs/{test_id}/")

        # Check that our specific temporary files are gone
        temp_file_names = [
            f"temp/processing/{test_id}/raw_cog.tif",
            f"temp/processing/{test_id}/intermediate.tif",
            f"temp/processing/{test_id}/temp_meta.json",
        ]
        for temp_file in temp_file_names:
            assert temp_file not in remaining_temp_files

        # Permanent files should still exist
        assert len(remaining_output_files) == 2
        assert (
            f"outputs/{test_id}/fire_severity_final.cog.tif" in remaining_output_files
        )
        assert f"outputs/{test_id}/analysis_final.json" in remaining_output_files

    finally:
        # Final cleanup of any remaining test files
        await minio_storage.cleanup()
