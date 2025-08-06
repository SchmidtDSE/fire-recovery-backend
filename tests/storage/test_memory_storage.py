import pytest
import io
import json
from typing import BinaryIO
from unittest.mock import patch
from src.core.storage.memory import MemoryStorage


class TestMemoryStorage:
    @pytest.mark.asyncio
    async def test_save_and_get_bytes(self, memory_storage: MemoryStorage) -> None:
        """Test saving and retrieving binary data"""
        test_data = b"Hello, world!"
        test_path = "test_file.bin"

        # Save data
        url = await memory_storage.save_bytes(test_data, test_path)

        # Verify URL format
        assert url == f"memory://test/{test_path}"

        # Retrieve data
        retrieved_data = await memory_storage.get_bytes(test_path)
        assert retrieved_data == test_data

    @pytest.mark.asyncio
    async def test_save_and_get_json(self, memory_storage: MemoryStorage) -> None:
        """Test saving and retrieving JSON data"""
        test_data = {"key": "value", "nested": {"key2": 42}}
        test_path = "test_file.json"

        # Save data
        url = await memory_storage.save_json(test_data, test_path)

        # Verify URL format
        assert url == f"memory://test/{test_path}"

        # Retrieve data
        retrieved_data = await memory_storage.get_json(test_path)
        assert retrieved_data == test_data

    @pytest.mark.asyncio
    async def test_list_files(self, memory_storage: MemoryStorage) -> None:
        """Test listing files with prefix"""
        # Save multiple files
        await memory_storage.save_bytes(b"data1", "prefix/file1.bin")
        await memory_storage.save_bytes(b"data2", "prefix/file2.bin")
        await memory_storage.save_bytes(b"data3", "other/file3.bin")

        # List files with prefix
        files = await memory_storage.list_files("prefix/")
        assert len(files) == 2
        assert "prefix/file1.bin" in files
        assert "prefix/file2.bin" in files
        assert "other/file3.bin" not in files

    @pytest.mark.asyncio
    async def test_cleanup(self, memory_storage: MemoryStorage) -> None:
        """Test cleanup of temporary files"""
        # Save temporary and permanent files
        await memory_storage.save_bytes(b"temp data", "temp_file.bin", temporary=True)
        await memory_storage.save_bytes(b"perm data", "perm_file.bin", temporary=False)

        # Verify both files exist
        files = await memory_storage.list_files("")
        assert len(files) == 2
        assert "temp_file.bin" in files
        assert "perm_file.bin" in files

        # Clean up temporary files
        removed = await memory_storage.cleanup()
        assert removed == 1

        # Verify only permanent file remains
        files = await memory_storage.list_files("")
        assert len(files) == 1
        assert "perm_file.bin" in files

    @pytest.mark.asyncio
    async def test_cleanup_with_mixed_temporary_files(self, memory_storage: MemoryStorage) -> None:
        """Test cleanup behavior with mixed temporary and permanent files from real workflows"""
        # Save files that represent a typical fire recovery processing workflow
        
        # Permanent files (final outputs)
        await memory_storage.save_bytes(b"Final fire severity COG", "outputs/fire_severity_final.cog.tif", temporary=False)
        await memory_storage.save_json({"analysis": "final results"}, "outputs/analysis_results.json", temporary=False)
        
        # Temporary files (intermediate processing)
        await memory_storage.save_bytes(b"Downloaded raw COG", "temp/raw_fire_data.tif", temporary=True)
        await memory_storage.save_bytes(b"Cropped intermediate", "temp/cropped_intermediate.tif", temporary=True)
        await memory_storage.save_json({"temp": "processing metadata"}, "temp/processing_meta.json", temporary=True)
        
        # Verify all files exist
        all_files = await memory_storage.list_files("")
        assert len(all_files) == 5
        
        # Clean up temporary files
        removed_count = await memory_storage.cleanup()
        assert removed_count == 3  # Should remove all 3 temporary files
        
        # Verify only permanent files remain
        remaining_files = await memory_storage.list_files("")
        assert len(remaining_files) == 2
        assert "outputs/fire_severity_final.cog.tif" in remaining_files
        assert "outputs/analysis_results.json" in remaining_files
        
        # Verify temporary files are gone
        temp_files = await memory_storage.list_files("temp/")
        assert len(temp_files) == 0

    @pytest.mark.asyncio
    async def test_copy_from_url(self, memory_storage: MemoryStorage) -> None:
        """Test copying from URL to storage"""
        # Use a real URL for testing
        url = "https://raw.githubusercontent.com/pytorch/pytorch/main/README.md"
        target_path = "downloaded_readme.md"

        result_url = await memory_storage.copy_from_url(url, target_path)

        # Verify the file was saved
        assert result_url == f"memory://test/{target_path}"
        retrieved_data = await memory_storage.get_bytes(target_path)
        assert len(retrieved_data) > 0  # Should have content
        assert b"PyTorch" in retrieved_data  # Should contain this text

    @pytest.mark.asyncio
    async def test_copy_from_url_temporary_flag(self, memory_storage: MemoryStorage) -> None:
        """Test copying from URL with temporary flag"""
        url = "https://raw.githubusercontent.com/pytorch/pytorch/main/README.md"
        temp_path = "temp_readme.md"
        perm_path = "perm_readme.md"

        # Copy as temporary
        temp_url = await memory_storage.copy_from_url(url, temp_path, temporary=True)
        assert temp_url == f"memory://test/{temp_path}"

        # Copy as permanent
        perm_url = await memory_storage.copy_from_url(url, perm_path, temporary=False)
        assert perm_url == f"memory://test/{perm_path}"

        # Verify both files exist and have content
        temp_data = await memory_storage.get_bytes(temp_path)
        perm_data = await memory_storage.get_bytes(perm_path)
        assert len(temp_data) > 0
        assert len(perm_data) > 0
        assert b"PyTorch" in temp_data
        assert b"PyTorch" in perm_data

        # Check internal storage metadata
        assert memory_storage._storage[temp_path]["temporary"] == True
        assert memory_storage._storage[perm_path]["temporary"] == False

    @pytest.mark.asyncio
    async def test_save_bytes_temporary_flag(self, memory_storage: MemoryStorage) -> None:
        """Test saving binary data with temporary flag"""
        test_data = b"Temporary data"
        temp_path = "temp_file.bin"
        perm_path = "perm_file.bin"

        # Save temporary data
        temp_url = await memory_storage.save_bytes(test_data, temp_path, temporary=True)
        assert temp_url == f"memory://test/{temp_path}"

        # Save permanent data
        perm_url = await memory_storage.save_bytes(test_data, perm_path, temporary=False)
        assert perm_url == f"memory://test/{perm_path}"

        # Verify both can be retrieved
        temp_retrieved = await memory_storage.get_bytes(temp_path)
        perm_retrieved = await memory_storage.get_bytes(perm_path)
        assert temp_retrieved == test_data
        assert perm_retrieved == test_data

        # Check internal storage metadata
        assert memory_storage._storage[temp_path]["temporary"] == True
        assert memory_storage._storage[perm_path]["temporary"] == False

    @pytest.mark.asyncio
    async def test_save_json_temporary_flag(self, memory_storage: MemoryStorage) -> None:
        """Test saving JSON data with temporary flag"""
        test_data = {"temp": "data", "test": True}
        temp_path = "temp_data.json"
        perm_path = "perm_data.json"

        # Save temporary JSON
        temp_url = await memory_storage.save_json(test_data, temp_path, temporary=True)
        assert temp_url == f"memory://test/{temp_path}"

        # Save permanent JSON
        perm_url = await memory_storage.save_json(test_data, perm_path, temporary=False)
        assert perm_url == f"memory://test/{perm_path}"

        # Verify both can be retrieved
        temp_retrieved = await memory_storage.get_json(temp_path)
        perm_retrieved = await memory_storage.get_json(perm_path)
        assert temp_retrieved == test_data
        assert perm_retrieved == test_data

        # Check internal storage metadata
        assert memory_storage._storage[temp_path]["temporary"] == True
        assert memory_storage._storage[perm_path]["temporary"] == False

    @pytest.mark.asyncio
    async def test_process_stream_geospatial_workflow(self, memory_storage: MemoryStorage) -> None:
        """Test process_stream with realistic geospatial data processing workflow"""
        # Simulate a small GeoTIFF-like binary data (mock)
        # In real scenario, this would be actual COG/GeoTIFF data
        mock_tiff_data = b"GeoTIFF mock data representing fire severity raster"
        source_path = "fire_severity.tif"
        
        # Save the source raster data
        await memory_storage.save_bytes(mock_tiff_data, source_path)

        # Simulate geospatial processing function that would normally use rioxarray/xarray
        def spatial_crop_processor(file_obj: BinaryIO) -> bytes:
            """Simulate cropping a raster with geometry (like crop_cog_with_geometry)"""
            # In real scenario, this would:
            # 1. Open with rioxarray.open_rasterio(file_obj)
            # 2. Crop with geometry using data.rio.clip([geom])
            # 3. Return processed bytes
            original_data = file_obj.read()
            # Mock processing: add a header to simulate cropped/processed data
            processed_data = b"CROPPED:" + original_data
            return processed_data

        processed_path = "cropped_fire_severity.tif"
        
        # Process using stream (avoiding temp files)
        result_url = await memory_storage.process_stream(
            source_path, spatial_crop_processor, processed_path, temporary=True
        )

        # Verify processing worked
        assert result_url == f"memory://test/{processed_path}"
        processed_data = await memory_storage.get_bytes(processed_path)
        assert processed_data == b"CROPPED:GeoTIFF mock data representing fire severity raster"
        
        # Verify it was marked as temporary
        assert memory_storage._storage[processed_path]["temporary"] == True

    @pytest.mark.asyncio
    async def test_process_stream_cog_translation_workflow(self, memory_storage: MemoryStorage) -> None:
        """Test process_stream simulating COG creation workflow (like create_cog function)"""
        # Simulate xarray DataArray data that needs to be converted to COG format
        mock_xarray_data = b'{"fire_severity_array": "xarray data representing processed raster"}'
        source_path = "processed_data.json"
        
        # Save the xarray-like data
        await memory_storage.save_bytes(mock_xarray_data, source_path)

        # Simulate COG creation process
        def cog_creation_processor(file_obj: BinaryIO) -> bytes:
            """Simulate creating a Cloud Optimized GeoTIFF from xarray data"""
            # In real scenario, this would:
            # 1. Load xarray data from file_obj
            # 2. Use rio_cogeo.cog_translate to create COG
            # 3. Return the COG bytes
            json_data = json.loads(file_obj.read().decode())
            # Mock COG creation: convert JSON to mock TIFF format
            cog_header = b"COG_HEADER:"
            cog_data = json.dumps(json_data).encode()
            return cog_header + cog_data

        cog_path = "fire_severity.cog.tif"
        
        # Create COG using stream processing
        result_url = await memory_storage.process_stream(
            source_path, cog_creation_processor, cog_path, temporary=False
        )

        # Verify COG creation worked
        assert result_url == f"memory://test/{cog_path}"
        cog_data = await memory_storage.get_bytes(cog_path)
        assert cog_data.startswith(b"COG_HEADER:")
        assert b"fire_severity_array" in cog_data
        
        # Verify it was marked as permanent
        assert memory_storage._storage[cog_path]["temporary"] == False

    @pytest.mark.asyncio
    async def test_process_stream_vegetation_analysis_workflow(self, memory_storage: MemoryStorage) -> None:
        """Test process_stream simulating vegetation geopackage analysis"""
        # Simulate geopackage data for vegetation analysis
        mock_gpkg_data = b"Geopackage data with vegetation polygons for fire recovery analysis"
        veg_source_path = "vegetation.gpkg"
        
        # Save the vegetation geopackage data
        await memory_storage.save_bytes(mock_gpkg_data, veg_source_path)
        
        # Also save fire severity data for zonal statistics
        fire_data = b"Fire severity raster for zonal stats"
        fire_path = "fire_severity.tif"
        await memory_storage.save_bytes(fire_data, fire_path)

        # Simulate vegetation analysis processor
        def vegetation_analysis_processor(veg_file_obj: BinaryIO) -> bytes:
            """Simulate zonal statistics analysis combining vegetation and fire data"""
            # In real scenario, this would:
            # 1. Open vegetation geopackage with geopandas
            # 2. Load fire raster with rioxarray
            # 3. Perform zonal statistics using rasterstats
            # 4. Return analysis results as CSV/JSON
            veg_data = veg_file_obj.read()
            analysis_result = b"Vegetation Analysis Results:\\n" + veg_data[:50] + b"\\n[Zonal statistics would go here]"
            return analysis_result

        results_path = "vegetation_analysis_results.csv"
        
        # Process vegetation analysis using stream
        result_url = await memory_storage.process_stream(
            veg_source_path, vegetation_analysis_processor, results_path, temporary=True
        )

        # Verify analysis worked
        assert result_url == f"memory://test/{results_path}"
        results_data = await memory_storage.get_bytes(results_path)
        assert results_data.startswith(b"Vegetation Analysis Results:")
        assert b"Geopackage data" in results_data
        
        # Verify it was marked as temporary (intermediate analysis result)
        assert memory_storage._storage[results_path]["temporary"] == True
