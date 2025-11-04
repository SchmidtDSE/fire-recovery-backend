import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import UploadFile
from io import BytesIO

from geojson_pydantic import FeatureCollection, Feature, Polygon
from src.commands.impl.upload_aoi_command import UploadAOICommand
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandStatus


@pytest.fixture
def mock_storage() -> MagicMock:
    """Mock storage interface"""
    storage = MagicMock()

    # Make save_bytes return a URL that includes the path
    async def mock_save_bytes(data, path, temporary=False):
        return f"https://storage.example.com/{path}"

    storage.save_bytes = AsyncMock(side_effect=mock_save_bytes)
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
def mock_storage_factory(mock_storage: AsyncMock) -> MagicMock:
    """Create mock storage factory"""
    from src.core.storage.storage_factory import StorageFactory

    factory = MagicMock(spec=StorageFactory)
    factory.get_temp_storage.return_value = mock_storage
    factory.get_final_storage.return_value = mock_storage
    return factory


@pytest.fixture
def sample_geojson() -> dict:
    """Sample valid GeoJSON for testing"""
    return {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-120.0, 35.0],
                    [-119.0, 35.0],
                    [-119.0, 36.0],
                    [-120.0, 36.0],
                    [-120.0, 35.0],
                ]
            ],
        },
        "properties": {},
    }


@pytest.fixture
def sample_feature_collection() -> dict:
    """Sample FeatureCollection for testing"""
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-120.0, 35.0],
                            [-119.0, 35.0],
                            [-119.0, 36.0],
                            [-120.0, 36.0],
                            [-120.0, 35.0],
                        ]
                    ],
                },
                "properties": {},
            }
        ],
    }


@pytest.fixture
def geojson_command_context(
    mock_storage: AsyncMock,
    mock_storage_factory: MagicMock,
    mock_stac_manager: AsyncMock,
    mock_index_registry: MagicMock,
    sample_geojson: dict,
) -> CommandContext:
    """Command context for GeoJSON upload testing"""
    return CommandContext(
        job_id="test-job-123",
        fire_event_name="test-fire",
        geometry=sample_geojson,
        storage=mock_storage,
        storage_factory=mock_storage_factory,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        metadata={"upload_type": "geojson"},
    )


@pytest.fixture
def shapefile_upload_file() -> UploadFile:
    """Mock UploadFile for shapefile testing"""
    content = b"fake zip file content"
    file = UploadFile(filename="test_shapefile.zip", file=BytesIO(content))
    # Mock the read method
    file.read = AsyncMock(return_value=content)  # type: ignore
    return file


@pytest.fixture
def shapefile_command_context(
    mock_storage: AsyncMock,
    mock_storage_factory: MagicMock,
    mock_stac_manager: AsyncMock,
    mock_index_registry: MagicMock,
    shapefile_upload_file: UploadFile,
    sample_geojson: dict,
) -> CommandContext:
    """Command context for Shapefile upload testing"""
    return CommandContext(
        job_id="test-job-456",
        fire_event_name="test-fire",
        geometry=sample_geojson,  # Provide valid geometry for context validation
        storage=mock_storage,
        storage_factory=mock_storage_factory,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        metadata={"upload_type": "shapefile", "upload_data": shapefile_upload_file},
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

    def test_validate_context_success(
        self, geojson_command_context: CommandContext
    ) -> None:
        """Test successful context validation"""
        command = UploadAOICommand()
        is_valid, message = command.validate_context(geojson_command_context)
        assert is_valid is True
        assert "validation passed" in message.lower()

    def test_validate_context_missing_job_id(
        self, geojson_command_context: CommandContext
    ) -> None:
        """Test context validation with missing job_id"""
        geojson_command_context.job_id = ""
        command = UploadAOICommand()
        is_valid, message = command.validate_context(geojson_command_context)
        assert is_valid is False
        assert "job_id" in message.lower()

    def test_validate_context_missing_storage(
        self, geojson_command_context: CommandContext
    ) -> None:
        """Test context validation with missing storage"""
        geojson_command_context.storage = None
        command = UploadAOICommand()
        is_valid, message = command.validate_context(geojson_command_context)
        assert is_valid is False
        assert "storage" in message.lower()

    def test_validate_context_missing_geometry_for_geojson(
        self, geojson_command_context: CommandContext
    ) -> None:
        """Test context validation with missing geometry for GeoJSON upload"""
        geojson_command_context.geometry = None
        command = UploadAOICommand()
        is_valid, message = command.validate_context(geojson_command_context)
        assert is_valid is False
        assert "geometry" in message.lower()

    def test_validate_context_missing_upload_data_for_shapefile(
        self, shapefile_command_context: CommandContext
    ) -> None:
        """Test context validation with missing upload_data for shapefile upload"""
        shapefile_command_context.metadata = {
            "upload_type": "shapefile"
        }  # Remove upload_data
        command = UploadAOICommand()
        is_valid, message = command.validate_context(shapefile_command_context)
        assert is_valid is False
        assert "upload_data" in message.lower()

    @pytest.mark.asyncio
    @patch("src.commands.impl.upload_aoi_command.polygon_to_valid_geojson")
    @patch("src.commands.impl.upload_aoi_command.upload_to_gcs")
    @patch("src.commands.impl.upload_aoi_command.shape")
    async def test_execute_geojson_success(
        self,
        mock_shape: MagicMock,
        mock_upload_to_gcs: MagicMock,
        mock_polygon_to_valid_geojson: MagicMock,
        geojson_command_context: CommandContext,
        sample_geojson: dict,
    ) -> None:
        """Test successful GeoJSON upload execution"""
        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = FeatureCollection(
            type="FeatureCollection",
            features=[
                Feature(
                    type="Feature",
                    geometry=Polygon(
                        type="Polygon",
                        coordinates=[
                            [
                                (-120.0, 35.0),
                                (-119.0, 35.0),
                                (-119.0, 36.0),
                                (-120.0, 36.0),
                                (-120.0, 35.0),
                            ]
                        ],
                    ),
                    properties={},
                )
            ],
        )
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
    @patch("src.commands.impl.upload_aoi_command.polygon_to_valid_geojson")
    @patch("src.commands.impl.upload_aoi_command.upload_to_gcs")
    @patch("src.commands.impl.upload_aoi_command.shape")
    async def test_execute_shapefile_success(
        self,
        mock_shape: MagicMock,
        mock_upload_to_gcs: MagicMock,
        mock_polygon_to_valid_geojson: MagicMock,
        shapefile_command_context: CommandContext,
        shapefile_upload_file: UploadFile,
        sample_shapefile_zip_bytes: bytes,
    ) -> None:
        """Test successful Shapefile upload execution"""
        from geojson_pydantic import FeatureCollection, Feature, Polygon

        # Setup file with real shapefile bytes
        shapefile_upload_file.read = AsyncMock(return_value=sample_shapefile_zip_bytes)  # type: ignore

        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = FeatureCollection(
            type="FeatureCollection",
            features=[
                Feature(
                    type="Feature",
                    geometry=Polygon(
                        type="Polygon",
                        coordinates=[
                            [
                                (-120.0, 35.0),
                                (-119.0, 35.0),
                                (-119.0, 36.0),
                                (-120.0, 36.0),
                                (-120.0, 35.0),
                            ]
                        ],
                    ),
                    properties={},
                )
            ],
        )
        mock_upload_to_gcs.return_value = "https://gcs.example.com/test.zip"

        mock_geom = MagicMock()
        mock_geom.bounds = (-120.0, 35.0, -119.0, 36.0)
        mock_shape.return_value = mock_geom

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
        assert "boundary_geojson_url" in result.data  # NEW - geometry extracted
        assert "stac_item_url" in result.data  # NEW - STAC created
        assert "gcs_shapefile_url" in result.data

        # Verify storage was called for both zip and geojson
        assert shapefile_command_context.storage.save_bytes.call_count == 2

        # Verify file was read
        shapefile_upload_file.read.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_unsupported_upload_type(
        self, geojson_command_context: CommandContext
    ) -> None:
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
    async def test_execute_invalid_geojson_type(
        self, geojson_command_context: CommandContext
    ) -> None:
        """Test execution with invalid GeoJSON type"""
        geojson_command_context.geometry = {"type": "InvalidType", "coordinates": []}

        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)

        # Verify failure result
        assert result.status == CommandStatus.FAILED
        assert result.is_failure() is True
        assert result.error_message is not None
        assert "Unsupported GeoJSON type" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_invalid_shapefile_extension(
        self, shapefile_command_context: CommandContext
    ) -> None:
        """Test execution with invalid shapefile extension"""
        # Create upload file with wrong extension
        content = b"fake file content"
        invalid_file = UploadFile(filename="test.txt", file=BytesIO(content))
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
    @patch("src.commands.impl.upload_aoi_command.polygon_to_valid_geojson")
    async def test_execute_storage_failure(
        self,
        mock_polygon_to_valid_geojson: MagicMock,
        geojson_command_context: CommandContext,
        sample_geojson: dict,
    ) -> None:
        """Test execution with storage failure"""
        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = FeatureCollection(
            type="FeatureCollection",
            features=[
                Feature(
                    type="Feature",
                    geometry=Polygon(
                        type="Polygon",
                        coordinates=[
                            [
                                (-120.0, 35.0),
                                (-119.0, 35.0),
                                (-119.0, 36.0),
                                (-120.0, 36.0),
                                (-120.0, 35.0),
                            ]
                        ],
                    ),
                    properties={},
                )
            ],
        )

        # Make storage fail
        geojson_command_context.storage.save_bytes.side_effect = Exception(
            "Storage failed"
        )

        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)

        # Verify failure result
        assert result.status == CommandStatus.FAILED
        assert result.is_failure() is True
        assert result.error_message is not None
        assert "Storage failed" in result.error_message

    @pytest.mark.asyncio
    async def test_feature_collection_validation(
        self, geojson_command_context: CommandContext, sample_feature_collection: dict
    ) -> None:
        """Test FeatureCollection validation"""
        geojson_command_context.geometry = sample_feature_collection

        with (
            patch(
                "src.commands.impl.upload_aoi_command.polygon_to_valid_geojson"
            ) as mock_polygon,
            patch("src.commands.impl.upload_aoi_command.upload_to_gcs") as mock_upload,
            patch("src.commands.impl.upload_aoi_command.shape") as mock_shape,
        ):
            # Setup mocks
            mock_polygon.return_value = FeatureCollection(
                type="FeatureCollection",
                features=[
                    Feature(
                        type="Feature",
                        geometry=Polygon(
                            type="Polygon",
                            coordinates=[
                                [
                                    [-120.0, 35.0],
                                    [-119.0, 35.0],
                                    [-119.0, 36.0],
                                    [-120.0, 36.0],
                                    [-120.0, 35.0],
                                ]
                            ],
                        ),
                        properties={},
                    )
                ],
            )
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


class TestShapefileExtraction:
    """Test in-memory shapefile geometry extraction"""

    def test_extract_valid_shapefile(self, sample_shapefile_zip_bytes: bytes) -> None:
        """Test extracting geometry from valid shapefile"""
        command = UploadAOICommand()

        geometry = command._extract_geometry_from_shapefile_zip(
            sample_shapefile_zip_bytes
        )

        assert geometry["type"] in ["Polygon", "MultiPolygon"]
        assert "coordinates" in geometry
        assert len(geometry["coordinates"]) > 0

    def test_extract_empty_shapefile(self, empty_shapefile_zip_bytes: bytes) -> None:
        """Test that empty shapefile raises ValueError"""
        command = UploadAOICommand()

        with pytest.raises(ValueError, match="contains no features"):
            command._extract_geometry_from_shapefile_zip(empty_shapefile_zip_bytes)

    def test_extract_invalid_geometry_type(
        self, point_shapefile_zip_bytes: bytes
    ) -> None:
        """Test that Point geometry raises ValueError"""
        command = UploadAOICommand()

        with pytest.raises(ValueError, match="unsupported geometry types"):
            command._extract_geometry_from_shapefile_zip(point_shapefile_zip_bytes)

    def test_extract_multipolygon_shapefile(
        self, multipolygon_shapefile_zip_bytes: bytes
    ) -> None:
        """Test extracting MultiPolygon from shapefile with multiple features"""
        command = UploadAOICommand()

        geometry = command._extract_geometry_from_shapefile_zip(
            multipolygon_shapefile_zip_bytes
        )

        # Should dissolve into single geometry
        assert geometry["type"] in ["Polygon", "MultiPolygon"]

    def test_extract_corrupted_zip(self) -> None:
        """Test that corrupted zip raises ValueError"""
        command = UploadAOICommand()

        with pytest.raises(ValueError, match="Invalid zip file or no .shp file found"):
            command._extract_geometry_from_shapefile_zip(b"not a zip file")

    def test_extract_zip_without_shp(self, zip_without_shp_bytes: bytes) -> None:
        """Test that zip without .shp file raises ValueError"""
        command = UploadAOICommand()

        with pytest.raises(ValueError, match="no .shp file found"):
            command._extract_geometry_from_shapefile_zip(zip_without_shp_bytes)


class TestBoundaryTypeParameter:
    """Test boundary_type parameter handling"""

    @pytest.mark.asyncio
    @patch("src.commands.impl.upload_aoi_command.polygon_to_valid_geojson")
    @patch("src.commands.impl.upload_aoi_command.upload_to_gcs")
    @patch("src.commands.impl.upload_aoi_command.shape")
    async def test_geojson_upload_with_coarse_boundary_type(
        self,
        mock_shape: MagicMock,
        mock_upload_to_gcs: MagicMock,
        mock_polygon_to_valid_geojson: MagicMock,
        geojson_command_context: CommandContext,
    ) -> None:
        """Test GeoJSON upload with boundary_type='coarse'"""
        from geojson_pydantic import FeatureCollection, Feature, Polygon

        geojson_command_context.metadata["boundary_type"] = "coarse"

        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = FeatureCollection(
            type="FeatureCollection",
            features=[
                Feature(
                    type="Feature",
                    geometry=Polygon(
                        type="Polygon",
                        coordinates=[
                            [
                                (-120.0, 35.0),
                                (-119.0, 35.0),
                                (-119.0, 36.0),
                                (-120.0, 36.0),
                                (-120.0, 35.0),
                            ]
                        ],
                    ),
                    properties={},
                )
            ],
        )
        mock_upload_to_gcs.return_value = "https://gcs.example.com/test.geojson"

        mock_geom = MagicMock()
        mock_geom.bounds = (-120.0, 35.0, -119.0, 36.0)
        mock_shape.return_value = mock_geom

        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)

        assert result.is_success()
        assert result.data["boundary_type"] == "coarse"
        # Verify filename contains "coarse"
        assert "coarse_boundary" in result.data["boundary_geojson_url"]

    @pytest.mark.asyncio
    @patch("src.commands.impl.upload_aoi_command.polygon_to_valid_geojson")
    @patch("src.commands.impl.upload_aoi_command.upload_to_gcs")
    @patch("src.commands.impl.upload_aoi_command.shape")
    async def test_geojson_upload_with_refined_boundary_type(
        self,
        mock_shape: MagicMock,
        mock_upload_to_gcs: MagicMock,
        mock_polygon_to_valid_geojson: MagicMock,
        geojson_command_context: CommandContext,
    ) -> None:
        """Test GeoJSON upload with boundary_type='refined'"""
        from geojson_pydantic import FeatureCollection, Feature, Polygon

        geojson_command_context.metadata["boundary_type"] = "refined"

        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = FeatureCollection(
            type="FeatureCollection",
            features=[
                Feature(
                    type="Feature",
                    geometry=Polygon(
                        type="Polygon",
                        coordinates=[
                            [
                                (-120.0, 35.0),
                                (-119.0, 35.0),
                                (-119.0, 36.0),
                                (-120.0, 36.0),
                                (-120.0, 35.0),
                            ]
                        ],
                    ),
                    properties={},
                )
            ],
        )
        mock_upload_to_gcs.return_value = "https://gcs.example.com/test.geojson"

        mock_geom = MagicMock()
        mock_geom.bounds = (-120.0, 35.0, -119.0, 36.0)
        mock_shape.return_value = mock_geom

        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)

        assert result.is_success()
        assert result.data["boundary_type"] == "refined"
        assert "refined_boundary" in result.data["boundary_geojson_url"]

    @pytest.mark.asyncio
    async def test_invalid_boundary_type_raises_error(
        self, geojson_command_context: CommandContext
    ) -> None:
        """Test that invalid boundary_type raises ValueError"""
        geojson_command_context.metadata["boundary_type"] = "uploaded"  # Invalid!

        command = UploadAOICommand()
        result = await command.execute(geojson_command_context)

        assert result.is_failure()
        assert "Invalid boundary_type" in result.error_message


class TestShapefileUploadIntegration:
    """Test complete shapefile upload flow"""

    @pytest.mark.asyncio
    @patch("src.commands.impl.upload_aoi_command.polygon_to_valid_geojson")
    @patch("src.commands.impl.upload_aoi_command.upload_to_gcs")
    @patch("src.commands.impl.upload_aoi_command.shape")
    async def test_shapefile_upload_creates_geojson_and_stac(
        self,
        mock_shape: MagicMock,
        mock_upload_to_gcs: MagicMock,
        mock_polygon_to_valid_geojson: MagicMock,
        shapefile_command_context: CommandContext,
        sample_shapefile_zip_bytes: bytes,
        shapefile_upload_file: UploadFile,
    ) -> None:
        """Test that shapefile upload extracts geometry, saves GeoJSON, and creates STAC item"""
        from geojson_pydantic import FeatureCollection, Feature, Polygon

        shapefile_upload_file.read = AsyncMock(return_value=sample_shapefile_zip_bytes)  # type: ignore
        shapefile_command_context.metadata["boundary_type"] = "refined"

        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = FeatureCollection(
            type="FeatureCollection",
            features=[
                Feature(
                    type="Feature",
                    geometry=Polygon(
                        type="Polygon",
                        coordinates=[
                            [
                                (-120.0, 35.0),
                                (-119.0, 35.0),
                                (-119.0, 36.0),
                                (-120.0, 36.0),
                                (-120.0, 35.0),
                            ]
                        ],
                    ),
                    properties={},
                )
            ],
        )
        mock_upload_to_gcs.return_value = "https://gcs.example.com/test.zip"

        mock_geom = MagicMock()
        mock_geom.bounds = (-120.0, 35.0, -119.0, 36.0)
        mock_shape.return_value = mock_geom

        command = UploadAOICommand()
        result = await command.execute(shapefile_command_context)

        assert result.is_success()
        assert result.data["upload_type"] == "shapefile"
        assert "shapefile_url" in result.data
        assert "boundary_geojson_url" in result.data  # NEW - geometry extracted
        assert "stac_item_url" in result.data  # NEW - STAC created
        assert result.data["boundary_type"] == "refined"

        # Verify storage was called for both shapefile and geojson
        assert shapefile_command_context.storage.save_bytes.call_count == 2

    @pytest.mark.asyncio
    @patch("src.commands.impl.upload_aoi_command.polygon_to_valid_geojson")
    @patch("src.commands.impl.upload_aoi_command.upload_to_gcs")
    @patch("src.commands.impl.upload_aoi_command.shape")
    async def test_shapefile_upload_with_coarse_boundary_type(
        self,
        mock_shape: MagicMock,
        mock_upload_to_gcs: MagicMock,
        mock_polygon_to_valid_geojson: MagicMock,
        shapefile_command_context: CommandContext,
        sample_shapefile_zip_bytes: bytes,
        shapefile_upload_file: UploadFile,
    ) -> None:
        """Test shapefile upload can use 'coarse' boundary_type"""
        from geojson_pydantic import FeatureCollection, Feature, Polygon

        shapefile_upload_file.read = AsyncMock(return_value=sample_shapefile_zip_bytes)  # type: ignore
        shapefile_command_context.metadata["boundary_type"] = "coarse"

        # Setup mocks
        mock_polygon_to_valid_geojson.return_value = FeatureCollection(
            type="FeatureCollection",
            features=[
                Feature(
                    type="Feature",
                    geometry=Polygon(
                        type="Polygon",
                        coordinates=[
                            [
                                (-120.0, 35.0),
                                (-119.0, 35.0),
                                (-119.0, 36.0),
                                (-120.0, 36.0),
                                (-120.0, 35.0),
                            ]
                        ],
                    ),
                    properties={},
                )
            ],
        )
        mock_upload_to_gcs.return_value = "https://gcs.example.com/test.zip"

        mock_geom = MagicMock()
        mock_geom.bounds = (-120.0, 35.0, -119.0, 36.0)
        mock_shape.return_value = mock_geom

        command = UploadAOICommand()
        result = await command.execute(shapefile_command_context)

        assert result.is_success()
        assert result.data["boundary_type"] == "coarse"
        assert "coarse_boundary" in result.data["boundary_geojson_url"]
