import pytest
import json
import uuid
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import UploadFile
from io import BytesIO

from src.commands.impl.upload_aoi_command import UploadAOICommand
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandStatus


@pytest.fixture
def mock_storage() -> AsyncMock:
    """Mock storage interface"""
    storage = AsyncMock()
    storage.save_bytes = AsyncMock(return_value="https://storage.example.com/test-path")
    return storage


@pytest.fixture
def mock_stac_manager() -> AsyncMock:
    """Mock STAC manager"""
    stac_manager = AsyncMock()
    stac_manager.create_boundary_item = AsyncMock(
        return_value="https://stac.example.com/boundary-item"
    )
    return stac_manager


@pytest.fixture
def mock_index_registry() -> MagicMock:
    """Mock index registry"""
    return MagicMock()


@pytest.fixture
def sample_geojson() -> dict:
    """Sample valid GeoJSON for testing"""
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-120.0, 35.0],
                [-119.0, 35.0],
                [-119.0, 36.0],
                [-120.0, 36.0],
                [-120.0, 35.0]
            ]]
        },
        "properties": {}
    }


@pytest.fixture
def sample_feature_collection() -> dict:
    """Sample FeatureCollection for testing"""
    return {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-120.0, 35.0],
                    [-119.0, 35.0],
                    [-119.0, 36.0],
                    [-120.0, 36.0],
                    [-120.0, 35.0]
                ]]
            },
            "properties": {}
        }]
    }


@pytest.fixture
def geojson_command_context(mock_storage: AsyncMock, mock_stac_manager: AsyncMock, mock_index_registry: MagicMock, sample_geojson: dict) -> CommandContext:
    """Command context for GeoJSON upload testing"""
    return CommandContext(
        job_id="test-job-123",
        fire_event_name="test-fire",
        geometry=sample_geojson,
        storage=mock_storage,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        metadata={
            "upload_type": "geojson"
        }
    )


@pytest.fixture
def shapefile_upload_file() -> UploadFile:
    """Mock UploadFile for shapefile testing"""
    content = b"fake zip file content"
    file = UploadFile(
        filename="test_shapefile.zip",
        file=BytesIO(content)
    )
    # Mock the read method
    file.read = AsyncMock(return_value=content)  # type: ignore
    return file


@pytest.fixture
def shapefile_command_context(mock_storage: AsyncMock, mock_stac_manager: AsyncMock, mock_index_registry: MagicMock, shapefile_upload_file: UploadFile, sample_geojson: dict) -> CommandContext:
    """Command context for Shapefile upload testing"""
    return CommandContext(
        job_id="test-job-456",
        fire_event_name="test-fire",
        geometry=sample_geojson,  # Provide valid geometry for context validation
        storage=mock_storage,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        metadata={
            "upload_type": "shapefile",
            "upload_data": shapefile_upload_file
        }
    )


class TestUploadAOICommand:
    """Test suite for UploadAOICommand"""
    
    def test_command_properties(self) -> None:
        """Test basic command properties"""
        command = UploadAOICommand()
        
        assert command.get_command_name() == "upload_aoi"
        assert command.get_estimated_duration_seconds() == 30.0
        assert command.supports_retry() is True
        assert command.get_dependencies() == []
        assert command.get_required_permissions() == ["storage:write", "stac:write"]
    
    def test_validate_context_success(self, geojson_command_context: CommandContext) -> None:
        """Test successful context validation"""
        command = UploadAOICommand()
        assert command.validate_context(geojson_command_context) is True
    
    def test_validate_context_missing_job_id(self, geojson_command_context: CommandContext) -> None:
        """Test context validation with missing job_id"""
        geojson_command_context.job_id = ""
        command = UploadAOICommand()
        assert command.validate_context(geojson_command_context) is False
    
    def test_validate_context_missing_storage(self, geojson_command_context: CommandContext) -> None:
        """Test context validation with missing storage"""
        geojson_command_context.storage = None
        command = UploadAOICommand()
        assert command.validate_context(geojson_command_context) is False
    
    def test_validate_context_missing_geometry_for_geojson(self, geojson_command_context: CommandContext) -> None:
        """Test context validation with missing geometry for GeoJSON upload"""
        geojson_command_context.geometry = None
        command = UploadAOICommand()
        assert command.validate_context(geojson_command_context) is False
    
    def test_validate_context_missing_upload_data_for_shapefile(self, shapefile_command_context: CommandContext) -> None:
        """Test context validation with missing upload_data for shapefile upload"""
        shapefile_command_context.metadata = {"upload_type": "shapefile"}  # Remove upload_data
        command = UploadAOICommand()
        assert command.validate_context(shapefile_command_context) is False
    
    @pytest.mark.asyncio
    @patch('src.commands.impl.upload_aoi_command.polygon_to_valid_geojson')
    @patch('src.commands.impl.upload_aoi_command.upload_to_gcs')
    @patch('src.commands.impl.upload_aoi_command.shape')
    async def test_execute_geojson_success(
        self, 
        mock_shape: MagicMock, 
        mock_upload_to_gcs: MagicMock, 
        mock_polygon_to_valid_geojson: MagicMock,
        geojson_command_context: CommandContext,
        sample_geojson: dict
    ) -> None:
        """Test successful GeoJSON upload execution"""
        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = {
            "type": "FeatureCollection",
            "features": [sample_geojson]
        }
        mock_upload_to_gcs.return_value = "https://gcs.example.com/test.geojson"
        
        # Mock shapely bounds
        mock_geom = MagicMock()
        mock_geom.bounds = (-120.0, 35.0, -119.0, 36.0)
        mock_shape.return_value = mock_geom
        
        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)
        
        # Verify result
        assert result.status == CommandStatus.SUCCESS
        assert result.job_id == "test-job-123"
        assert result.fire_event_name == "test-fire"
        assert result.command_name == "upload_aoi"
        assert result.is_success() is True
        
        # Verify result data
        assert result.data is not None
        assert result.data["upload_type"] == "geojson"
        assert result.data["status"] == "complete"
        assert result.data["geometry_validated"] is True
        assert "boundary_geojson_url" in result.data
        assert "stac_item_url" in result.data
        
        # Verify storage was called
        geojson_command_context.storage.save_bytes.assert_called_once()
        
        # Verify STAC manager was called
        geojson_command_context.stac_manager.create_boundary_item.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.commands.impl.upload_aoi_command.upload_to_gcs')
    async def test_execute_shapefile_success(
        self, 
        mock_upload_to_gcs: MagicMock,
        shapefile_command_context: CommandContext,
        shapefile_upload_file: UploadFile
    ) -> None:
        """Test successful Shapefile upload execution"""
        # Setup mocks
        mock_upload_to_gcs.return_value = "https://gcs.example.com/test.zip"
        
        command = UploadAOICommand()
        result = await command.execute(shapefile_command_context)
        
        # Verify result
        assert result.status == CommandStatus.SUCCESS
        assert result.job_id == "test-job-456"
        assert result.fire_event_name == "test-fire"
        assert result.command_name == "upload_aoi"
        assert result.is_success() is True
        
        # Verify result data
        assert result.data is not None
        assert result.data["upload_type"] == "shapefile"
        assert result.data["status"] == "complete"
        assert result.data["filename"] == "test_shapefile.zip"
        assert "shapefile_url" in result.data
        assert "gcs_shapefile_url" in result.data
        
        # Verify storage was called
        shapefile_command_context.storage.save_bytes.assert_called_once()
        
        # Verify file was read
        shapefile_upload_file.read.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_execute_unsupported_upload_type(self, geojson_command_context: CommandContext) -> None:
        """Test execution with unsupported upload type"""
        geojson_command_context.metadata["upload_type"] = "invalid"
        
        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)
        
        # Verify failure result
        assert result.status == CommandStatus.FAILED
        assert result.is_failure() is True
        assert result.error_message is not None
        assert "Unsupported upload type: invalid" in result.error_message
        assert result.error_details is not None
        assert result.error_details["error_type"] == "ValueError"
    
    @pytest.mark.asyncio
    async def test_execute_invalid_geojson_type(self, geojson_command_context: CommandContext) -> None:
        """Test execution with invalid GeoJSON type"""
        geojson_command_context.geometry = {
            "type": "InvalidType",
            "coordinates": []
        }
        
        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)
        
        # Verify failure result
        assert result.status == CommandStatus.FAILED
        assert result.is_failure() is True
        assert result.error_message is not None
        assert "Unsupported GeoJSON type" in result.error_message
    
    @pytest.mark.asyncio
    async def test_execute_invalid_shapefile_extension(self, shapefile_command_context: CommandContext) -> None:
        """Test execution with invalid shapefile extension"""
        # Create upload file with wrong extension
        content = b"fake file content"
        invalid_file = UploadFile(
            filename="test.txt",
            file=BytesIO(content)
        )
        invalid_file.read = AsyncMock(return_value=content)  # type: ignore
        
        shapefile_command_context.metadata["upload_data"] = invalid_file
        
        command = UploadAOICommand()
        result = await command.execute(shapefile_command_context)
        
        # Verify failure result
        assert result.status == CommandStatus.FAILED
        assert result.is_failure() is True
        assert result.error_message is not None
        assert "Only zipped shapefiles (.zip) are supported" in result.error_message
    
    @pytest.mark.asyncio
    @patch('src.commands.impl.upload_aoi_command.polygon_to_valid_geojson')
    async def test_execute_storage_failure(self, mock_polygon_to_valid_geojson: MagicMock, geojson_command_context: CommandContext, sample_geojson: dict) -> None:
        """Test execution with storage failure"""
        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = {
            "type": "FeatureCollection",
            "features": [sample_geojson]
        }
        
        # Make storage fail
        geojson_command_context.storage.save_bytes.side_effect = Exception("Storage failed")
        
        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)
        
        # Verify failure result
        assert result.status == CommandStatus.FAILED
        assert result.is_failure() is True
        assert result.error_message is not None
        assert "Storage failed" in result.error_message
    
    @pytest.mark.asyncio
    async def test_feature_collection_validation(self, geojson_command_context: CommandContext, sample_feature_collection: dict) -> None:
        """Test FeatureCollection validation"""
        geojson_command_context.geometry = sample_feature_collection
        
        with patch('src.commands.impl.upload_aoi_command.polygon_to_valid_geojson') as mock_polygon, \
             patch('src.commands.impl.upload_aoi_command.upload_to_gcs') as mock_upload, \
             patch('src.commands.impl.upload_aoi_command.shape') as mock_shape:
            
            # Setup mocks
            mock_polygon.return_value = {
                "type": "FeatureCollection",
                "features": [sample_feature_collection["features"][0]]
            }
            mock_upload.return_value = "https://gcs.example.com/test.geojson"
            
            mock_geom = MagicMock()
            mock_geom.bounds = (-120.0, 35.0, -119.0, 36.0)
            mock_shape.return_value = mock_geom
            
            command = UploadAOICommand()
            result = await command.execute(geojson_command_context)
            
            # Verify success
            assert result.status == CommandStatus.SUCCESS
            assert result.data is not None
            assert result.data["geometry_validated"] is True
    
    def test_str_and_repr(self) -> None:
        """Test string representations"""
        command = UploadAOICommand()
        
        str_repr = str(command)
        assert "UploadAOICommand" in str_repr
        assert "upload_aoi" in str_repr
        
        repr_str = repr(command)
        assert "UploadAOICommand" in repr_str
        assert "name='upload_aoi'" in repr_str
        assert "estimated_duration=30.0s" in repr_str
        assert "supports_retry=True" in repr_str