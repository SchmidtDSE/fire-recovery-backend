import pytest
import json
import pandas as pd
import tempfile
import threading
from unittest.mock import Mock, AsyncMock, patch
from io import BytesIO
from typing import cast
from pathlib import Path

from src.commands.impl.vegetation_resolve_command import VegetationResolveCommand
from src.commands.interfaces.command_context import CommandContext
from src.core.storage.interface import StorageInterface
from src.core.storage.storage_factory import StorageFactory
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry
from src.config.vegetation_schema_loader import VegetationSchemaLoader
from src.config.vegetation_schemas import VegetationSchema
from geojson_pydantic import Polygon
from geojson_pydantic.types import Position2D, Position3D


@pytest.fixture
def mock_storage() -> Mock:
    """Create mock storage interface"""
    storage = Mock(spec=StorageInterface)
    storage.copy_from_url = AsyncMock(return_value="mock://storage/path")
    storage.get_bytes = AsyncMock()
    storage.save_bytes = AsyncMock(return_value="mock://storage/path")
    return storage


@pytest.fixture
def mock_storage_factory() -> Mock:
    """Create mock storage factory"""
    factory = Mock(spec=StorageFactory)

    # Mock temp storage
    temp_storage = Mock(spec=StorageInterface)
    temp_storage.save_bytes = AsyncMock(return_value="mock://temp/path")
    factory.get_temp_storage = Mock(return_value=temp_storage)

    # Mock final storage
    final_storage = Mock(spec=StorageInterface)
    final_storage.save_bytes = AsyncMock(return_value="mock://final/path")
    factory.get_final_storage = Mock(return_value=final_storage)

    return factory


@pytest.fixture
def mock_stac_manager() -> Mock:
    """Create mock STAC manager"""
    manager = Mock(spec=STACJSONManager)
    manager.get_items_by_id_and_coarseness = AsyncMock(
        return_value={
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            },
            "bbox": [0, 0, 1, 1],
            "properties": {"datetime": "2023-01-01T00:00:00Z"},
        }
    )
    manager.create_veg_matrix_item = AsyncMock(
        return_value="mock://stac/veg-matrix.json"
    )
    return manager


@pytest.fixture
def mock_index_registry() -> Mock:
    """Create mock index registry"""
    registry = Mock(spec=IndexRegistry)
    return registry


@pytest.fixture
def sample_geometry() -> Polygon:
    """Create sample polygon geometry"""
    coordinates = cast(
        list[list[Position2D | Position3D]],
        [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
    )
    return Polygon(type="Polygon", coordinates=coordinates)


@pytest.fixture
def valid_context(
    mock_storage: Mock,
    mock_storage_factory: Mock,
    mock_stac_manager: Mock,
    mock_index_registry: Mock,
    sample_geometry: Polygon,
) -> CommandContext:
    """Create valid CommandContext for testing"""
    context = CommandContext(
        job_id="test_job_123",
        fire_event_name="test_fire_event",
        geometry=sample_geometry,
        storage=mock_storage,
        storage_factory=mock_storage_factory,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        severity_breaks=[0.1, 0.27, 0.66],
        metadata={
            "veg_gpkg_url": "https://example.com/vegetation.gpkg",
            "fire_cog_url": "https://example.com/fire_severity.tif",
            "geojson_url": "https://example.com/boundary.geojson",
        },
    )
    return context


@pytest.fixture
def sample_vegetation_data() -> bytes:
    """Create sample vegetation geopackage data"""
    # Simulate GPKG data structure (would normally create GeoDataFrame and serialize)
    return b"mock_gpkg_data_with_vegetation_layers"


@pytest.fixture
def sample_fire_data() -> bytes:
    """Create sample fire severity COG data"""
    # Create mock xarray dataset
    return b"mock_cog_tiff_data_with_fire_severity"


@pytest.fixture
def sample_boundary_data() -> bytes:
    """Create sample boundary GeoJSON data"""
    boundary = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
                "properties": {},
            }
        ],
    }
    return json.dumps(boundary).encode("utf-8")


class TestVegetationResolveCommand:
    """Test suite for VegetationResolveCommand"""

    def test_get_command_name(self) -> None:
        """Test command name is correct"""
        command = VegetationResolveCommand()
        assert command.get_command_name() == "vegetation_resolve"

    def test_get_estimated_duration_seconds(self) -> None:
        """Test estimated duration is reasonable"""
        command = VegetationResolveCommand()
        duration = command.get_estimated_duration_seconds()
        assert isinstance(duration, float)
        assert 60 <= duration <= 600  # Between 1-10 minutes

    def test_supports_retry(self) -> None:
        """Test command supports retry"""
        command = VegetationResolveCommand()
        assert command.supports_retry() is True

    def test_get_dependencies(self) -> None:
        """Test command has no dependencies"""
        command = VegetationResolveCommand()
        assert command.get_dependencies() == []

    def test_get_required_permissions(self) -> None:
        """Test required permissions are comprehensive"""
        command = VegetationResolveCommand()
        permissions = command.get_required_permissions()
        expected = ["stac:read", "stac:write", "storage:write", "computation:execute"]
        assert all(perm in permissions for perm in expected)

    def test_validate_context_success(self, valid_context: CommandContext) -> None:
        """Test successful context validation"""
        command = VegetationResolveCommand()
        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is True
        assert error_msg == "Context validation passed"

    def test_validate_context_missing_job_id(
        self, valid_context: CommandContext
    ) -> None:
        """Test validation fails with missing job_id"""
        command = VegetationResolveCommand()
        valid_context.job_id = ""

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "job_id and fire_event_name are required" in error_msg

    def test_validate_context_missing_fire_event_name(
        self, valid_context: CommandContext
    ):
        """Test validation fails with missing fire_event_name"""
        command = VegetationResolveCommand()
        valid_context.fire_event_name = ""

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "job_id and fire_event_name are required" in error_msg

    def test_validate_context_missing_storage(
        self, valid_context: CommandContext
    ) -> None:
        """Test validation fails with missing storage"""
        command = VegetationResolveCommand()
        valid_context.storage = None  # type: ignore

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "storage, storage_factory, and stac_manager are required" in error_msg

    def test_validate_context_missing_veg_gpkg_url(
        self, valid_context: CommandContext
    ) -> None:
        """Test validation fails with missing veg_gpkg_url"""
        command = VegetationResolveCommand()
        valid_context.metadata.pop("veg_gpkg_url")  # type: ignore

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "veg_gpkg_url is required in metadata" in error_msg

    def test_validate_context_missing_fire_cog_url(
        self, valid_context: CommandContext
    ) -> None:
        """Test validation fails with missing fire_cog_url"""
        command = VegetationResolveCommand()
        valid_context.metadata.pop("fire_cog_url")  # type: ignore

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "fire_cog_url is required in metadata" in error_msg

    def test_validate_context_missing_geojson_url(
        self, valid_context: CommandContext
    ) -> None:
        """Test validation fails with missing geojson_url"""
        command = VegetationResolveCommand()
        valid_context.metadata.pop("geojson_url")  # type: ignore

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "geojson_url is required in metadata" in error_msg

    def test_validate_context_invalid_severity_breaks(
        self, valid_context: CommandContext
    ):
        """Test validation fails with invalid severity_breaks"""
        command = VegetationResolveCommand()
        valid_context.severity_breaks = [0.1]  # Too few breaks

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "severity_breaks must contain at least 3 values" in error_msg

    def test_validate_context_none_severity_breaks(
        self, valid_context: CommandContext
    ) -> None:
        """Test validation fails with None severity_breaks"""
        command = VegetationResolveCommand()
        valid_context.severity_breaks = None

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "severity_breaks must contain at least 3 values" in error_msg

    @pytest.mark.asyncio
    async def test_download_input_files_success(
        self,
        valid_context: CommandContext,
        sample_vegetation_data: bytes,
        sample_fire_data: bytes,
        sample_boundary_data: bytes,
    ):
        """Test successful input file downloading"""
        command = VegetationResolveCommand()

        # Mock storage responses
        valid_context.storage.get_bytes.side_effect = [
            sample_vegetation_data,
            sample_fire_data,
            sample_boundary_data,
        ]

        result = await command._download_input_files(
            valid_context,
            "https://example.com/vegetation.gpkg",
            "https://example.com/fire.tif",
            "https://example.com/boundary.geojson",
        )

        assert "vegetation" in result
        assert "fire_severity" in result
        assert "boundary" in result
        assert result["vegetation"] == sample_vegetation_data
        assert result["fire_severity"] == sample_fire_data
        assert result["boundary"] == sample_boundary_data

        # Verify storage calls
        assert valid_context.storage.copy_from_url.call_count == 3
        assert valid_context.storage.get_bytes.call_count == 3

    @pytest.mark.asyncio
    async def test_download_input_files_storage_error(
        self, valid_context: CommandContext
    ):
        """Test input file downloading with storage error"""
        command = VegetationResolveCommand()

        # Mock storage error
        valid_context.storage.copy_from_url.side_effect = Exception("Storage error")

        with pytest.raises(Exception, match="Storage error"):
            await command._download_input_files(
                valid_context,
                "https://example.com/vegetation.gpkg",
                "https://example.com/fire.tif",
                "https://example.com/boundary.geojson",
            )

    @patch("src.commands.impl.vegetation_resolve_command.xr.open_dataset")
    @pytest.mark.asyncio
    async def test_load_fire_data_from_bytes(self, mock_xr_open):
        """Test loading fire data from bytes"""
        command = VegetationResolveCommand()

        # Create mock xarray dataset
        mock_ds = Mock()
        import numpy as np

        mock_data_array = Mock()
        mock_data_array.size = 100
        mock_data_array.values = np.array(
            [[1.0, 2.0], [3.0, 4.0]]
        )  # Provide real numpy array
        mock_data_array.sizes = {"x": 10, "y": 10}

        # Mock data_vars as a dict-like object
        mock_ds.data_vars = {"band_data": mock_data_array}
        mock_ds.__getitem__ = Mock(return_value=mock_data_array)  # Support ds[data_var]

        mock_ds.rio.crs = "EPSG:32611"  # Already projected
        mock_transform = Mock()
        mock_transform.__getitem__ = Mock(
            side_effect=lambda x: 30.0 if x in [0, 4] else 0.0
        )
        mock_ds.rio.transform.return_value = mock_transform
        mock_ds.coords = {"x": Mock(), "y": Mock()}

        mock_xr_open.return_value = mock_ds

        fire_data = b"mock_fire_data"
        fire_ds, metadata = await command._load_fire_data_from_bytes(fire_data)

        assert fire_ds == mock_ds
        assert "crs" in metadata
        assert "pixel_area_ha" in metadata
        assert "data_var" in metadata
        assert metadata["pixel_area_ha"] == 0.09  # 30*30/10000

        # Verify xarray was called with BytesIO
        mock_xr_open.assert_called_once()
        args, kwargs = mock_xr_open.call_args
        assert isinstance(args[0], BytesIO)
        assert kwargs["engine"] == "rasterio"

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_vegetation_data_from_bytes_jotr(self, mock_gpd_read):
        """Test loading JOTR vegetation data from bytes with schema"""
        command = VegetationResolveCommand()

        # Mock JOTR format vegetation data
        mock_gdf = Mock()
        mock_gdf.columns = ["MapUnit_Name", "OBJECTID", "geometry", "veg_type"]
        mock_gdf.__getitem__ = Mock(return_value=["Forest", "Shrubland"])
        mock_gdf.__setitem__ = Mock()
        mock_gdf.crs = "EPSG:4326"
        mock_gdf.to_crs = Mock(return_value=mock_gdf)
        mock_gdf.dropna = Mock(return_value=mock_gdf)
        mock_gdf.__len__ = Mock(return_value=2)

        # Mock the __contains__ method for checking column existence
        def mock_contains(column):
            return column in ["MapUnit_Name", "OBJECTID", "geometry", "veg_type"]

        mock_gdf.__contains__ = Mock(side_effect=mock_contains)

        mock_gpd_read.return_value = mock_gdf

        veg_data = b"mock_vegetation_data"
        result = await command._load_vegetation_data_from_bytes(
            veg_data, "EPSG:32611", "JOTR"
        )

        assert result == mock_gdf
        mock_gpd_read.assert_called_once()
        args, kwargs = mock_gpd_read.call_args
        assert isinstance(args[0], BytesIO)
        assert kwargs["layer"] == "JOTR_VegPolys"

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_vegetation_data_from_bytes_mojn_fallback(self, mock_gpd_read):
        """Test loading MOJN vegetation data from bytes when auto-detection fails"""
        command = VegetationResolveCommand()

        # Mock MOJN format vegetation data
        mock_gdf = Mock()
        mock_gdf.columns = ["MAP_DESC", "FID", "geometry", "veg_type"]
        mock_gdf.__getitem__ = Mock(return_value=["Desert", "Woodland"])
        mock_gdf.__setitem__ = Mock()
        mock_gdf.crs = "EPSG:4326"
        mock_gdf.to_crs = Mock(return_value=mock_gdf)
        mock_gdf.dropna = Mock(return_value=mock_gdf)
        mock_gdf.__len__ = Mock(return_value=2)
        mock_gdf.copy = Mock(return_value=mock_gdf)

        # Mock the __contains__ method for checking column existence
        def mock_contains(column):
            return column in ["MAP_DESC", "FID", "geometry", "veg_type"]

        mock_gdf.__contains__ = Mock(side_effect=mock_contains)

        # First call (with unknown park_unit_id) triggers auto-detection
        # First JOTR schema fails, second MOJN schema succeeds
        mock_gpd_read.side_effect = [Exception("Layer not found"), mock_gdf]

        veg_data = b"mock_vegetation_data"
        result = await command._load_vegetation_data_from_bytes(
            veg_data, "EPSG:32611", "UNKNOWN"
        )

        assert result == mock_gdf
        assert mock_gpd_read.call_count == 2

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_vegetation_data_from_bytes_unsupported_format(
        self, mock_gpd_read
    ):
        """Test loading vegetation data with unsupported format"""
        command = VegetationResolveCommand()

        # All loading strategies fail
        mock_gpd_read.side_effect = [
            Exception("Layer not found"),  # JOTR schema fails
            Exception("Invalid format"),  # MOJN schema fails
            Exception("Read error"),  # Auto-detection fails
        ]

        veg_data = b"mock_vegetation_data"

        with pytest.raises(ValueError, match="Unable to load vegetation data"):
            await command._load_vegetation_data_from_bytes(veg_data, "EPSG:32611", None)

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_boundary_data_from_bytes(self, mock_gpd_read):
        """Test loading boundary data from bytes"""
        command = VegetationResolveCommand()

        mock_gdf = Mock()
        mock_gdf.to_crs.return_value = mock_gdf
        mock_gdf.__len__ = Mock(return_value=5)  # Mock len() for logging
        mock_gpd_read.return_value = mock_gdf

        boundary_data = b"mock_boundary_data"
        result = await command._load_boundary_data_from_bytes(boundary_data)

        assert result == mock_gdf
        mock_gpd_read.assert_called_once()
        args, _ = mock_gpd_read.call_args
        assert isinstance(args[0], BytesIO)

    @pytest.mark.asyncio
    async def test_save_analysis_reports_success(self, valid_context: CommandContext):
        """Test successful saving of analysis reports"""
        command = VegetationResolveCommand()

        # Create sample DataFrame
        result_df = pd.DataFrame(
            {
                "total_ha": [100.0, 50.0],
                "unburned_ha": [60.0, 30.0],
                "low_ha": [20.0, 10.0],
                "moderate_ha": [15.0, 7.0],
                "high_ha": [5.0, 3.0],
            },
            index=["Forest", "Shrubland"],
        )

        # Create sample JSON structure
        json_structure = {
            "vegetation_communities": [
                {"name": "Forest", "total_hectares": 100.0},
                {"name": "Shrubland", "total_hectares": 50.0},
            ]
        }

        # Mock final storage responses for CSV and JSON files
        final_storage = valid_context.storage_factory.get_final_storage()
        final_storage.save_bytes.side_effect = [
            "mock://csv/url",
            "mock://json/url",
        ]

        result = await command._save_analysis_reports(
            valid_context, result_df, json_structure
        )

        assert result["vegetation_matrix_csv"] == "mock://csv/url"
        assert result["vegetation_matrix_json"] == "mock://json/url"

        # Verify final storage was called twice (CSV and JSON)
        assert final_storage.save_bytes.call_count == 2

    @pytest.mark.asyncio
    async def test_save_analysis_reports_storage_error(
        self, valid_context: CommandContext
    ):
        """Test analysis report saving with storage error"""
        command = VegetationResolveCommand()

        result_df = pd.DataFrame({"total_ha": [100.0]}, index=["Forest"])
        json_structure = {"vegetation_communities": []}

        # Mock final storage error
        final_storage = valid_context.storage_factory.get_final_storage()
        final_storage.save_bytes.side_effect = Exception("Storage error")

        with pytest.raises(Exception, match="Storage error"):
            await command._save_analysis_reports(
                valid_context, result_df, json_structure
            )

    def test_add_percentage_columns(self):
        """Test adding percentage columns to DataFrame"""
        command = VegetationResolveCommand()

        df = pd.DataFrame(
            {
                "total_ha": [100.0, 50.0],
                "unburned_ha": [60.0, 30.0],
                "low_ha": [20.0, 10.0],
                "moderate_ha": [15.0, 7.0],
                "high_ha": [5.0, 3.0],
            },
            index=["Forest", "Shrubland"],
        )

        result = command._add_percentage_columns(df)

        # Check percentage columns exist
        assert "unburned_percent" in result.columns
        assert "low_percent" in result.columns
        assert "moderate_percent" in result.columns
        assert "high_percent" in result.columns
        assert "total_percent" in result.columns

        # Check percentage calculations
        assert result.loc["Forest", "unburned_percent"] == 60.0  # 60/100 * 100
        assert result.loc["Forest", "total_percent"] == pytest.approx(
            66.67, abs=0.1
        )  # 100/150 * 100

    def test_create_json_structure(self):
        """Test creating JSON structure for visualization"""
        command = VegetationResolveCommand()

        df = pd.DataFrame(
            {
                "total_ha": [100.0, 50.0],
                "unburned_ha": [60.0, 30.0],
                "low_ha": [20.0, 10.0],
                "moderate_ha": [15.0, 7.0],
                "high_ha": [5.0, 3.0],
                "unburned_percent": [60.0, 60.0],
                "low_percent": [20.0, 20.0],
                "moderate_percent": [15.0, 14.0],
                "high_percent": [5.0, 6.0],
                "unburned_mean": [0.05, 0.04],
                "low_mean": [0.15, 0.16],
                "moderate_mean": [0.40, 0.39],
                "high_mean": [0.80, 0.85],
                "unburned_std": [0.02, 0.03],
                "low_std": [0.05, 0.04],
                "moderate_std": [0.10, 0.12],
                "high_std": [0.15, 0.18],
            },
            index=["Forest", "Shrubland"],
        )

        result = command._create_json_structure(df)

        assert "vegetation_communities" in result
        assert len(result["vegetation_communities"]) == 2

        forest_data = result["vegetation_communities"][0]
        assert forest_data["name"] == "Forest"
        assert forest_data["total_hectares"] == 100.0
        assert "severity_breakdown" in forest_data
        assert "unburned" in forest_data["severity_breakdown"]

    @pytest.mark.asyncio
    async def test_create_vegetation_stac_metadata_success(
        self, valid_context: CommandContext
    ):
        """Test successful STAC metadata creation"""
        command = VegetationResolveCommand()

        asset_urls = {
            "vegetation_matrix_csv": "mock://csv/url",
            "vegetation_matrix_json": "mock://json/url",
        }
        severity_breaks = [0.1, 0.27, 0.66]

        result = await command._create_vegetation_stac_metadata(
            valid_context, asset_urls, severity_breaks
        )

        assert result.startswith("stac://")
        assert valid_context.stac_manager.get_items_by_id_and_coarseness.called
        assert valid_context.stac_manager.create_veg_matrix_item.called

    @pytest.mark.asyncio
    async def test_create_vegetation_stac_metadata_missing_fire_item(
        self, valid_context: CommandContext
    ):
        """Test STAC metadata creation with missing fire severity item uses fallback"""
        command = VegetationResolveCommand()

        # Mock missing fire STAC item - return None for all strategies
        valid_context.stac_manager.get_items_by_id_and_coarseness.return_value = None
        valid_context.stac_manager.get_item_by_id.return_value = None

        asset_urls = {
            "vegetation_matrix_csv": "mock://csv/url",
            "vegetation_matrix_json": "mock://json/url",
        }
        severity_breaks = [0.1, 0.27, 0.66]

        # Should succeed using fallback geometry instead of raising error
        result = await command._create_vegetation_stac_metadata(
            valid_context, asset_urls, severity_breaks
        )

        assert isinstance(result, str)
        assert "stac://" in result

        # Verify fallback geometry was used
        valid_context.stac_manager.create_veg_matrix_item.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_success_full_workflow(self, valid_context: CommandContext):
        """Test successful execution of complete workflow"""
        command = VegetationResolveCommand()

        # Mock all the necessary methods
        with (
            patch.object(command, "_download_input_files") as mock_download,
            patch.object(command, "_analyze_vegetation_impact") as mock_analyze,
            patch.object(command, "_save_analysis_reports") as mock_save,
            patch.object(command, "_create_vegetation_stac_metadata") as mock_stac,
        ):
            # Setup mock returns
            mock_download.return_value = {
                "vegetation": b"data",
                "fire_severity": b"data",
                "boundary": b"data",
            }
            mock_analyze.return_value = (
                pd.DataFrame({"total_ha": [100.0]}, index=["Forest"]),
                {"vegetation_communities": []},
            )
            mock_save.return_value = {
                "vegetation_matrix_csv": "csv_url",
                "vegetation_matrix_json": "json_url",
            }
            mock_stac.return_value = "stac://item"

            result = await command.execute(valid_context)

            assert result.is_success()
            assert result.job_id == "test_job_123"
            assert result.fire_event_name == "test_fire_event"
            assert result.command_name == "vegetation_resolve"
            assert "vegetation_types_analyzed" in result.data
            assert result.has_assets()

    @pytest.mark.asyncio
    async def test_execute_validation_failure(self, valid_context: CommandContext):
        """Test execution with validation failure"""
        command = VegetationResolveCommand()

        # Make context invalid
        valid_context.metadata.pop("veg_gpkg_url")

        result = await command.execute(valid_context)

        assert result.is_failure()
        assert "Context validation failed" in result.error_message
        assert result.command_name == "vegetation_resolve"

    @pytest.mark.asyncio
    async def test_execute_download_failure(self, valid_context: CommandContext):
        """Test execution with download failure"""
        command = VegetationResolveCommand()

        with patch.object(command, "_download_input_files") as mock_download:
            mock_download.side_effect = Exception("Download failed")

            result = await command.execute(valid_context)

            assert result.is_failure()
            assert "Command execution failed" in result.error_message
            assert result.error_details["exception_type"] == "Exception"

    @pytest.mark.asyncio
    async def test_execute_analysis_failure(self, valid_context: CommandContext):
        """Test execution with analysis failure"""
        command = VegetationResolveCommand()

        with (
            patch.object(command, "_download_input_files") as mock_download,
            patch.object(command, "_analyze_vegetation_impact") as mock_analyze,
        ):
            mock_download.return_value = {
                "vegetation": b"data",
                "fire_severity": b"data",
                "boundary": b"data",
            }
            mock_analyze.side_effect = Exception("Analysis failed")

            result = await command.execute(valid_context)

            assert result.is_failure()
            assert "Command execution failed" in result.error_message

    @pytest.mark.asyncio
    async def test_execute_save_failure(self, valid_context: CommandContext):
        """Test execution with save failure"""
        command = VegetationResolveCommand()

        with (
            patch.object(command, "_download_input_files") as mock_download,
            patch.object(command, "_analyze_vegetation_impact") as mock_analyze,
            patch.object(command, "_save_analysis_reports") as mock_save,
        ):
            mock_download.return_value = {
                "vegetation": b"data",
                "fire_severity": b"data",
                "boundary": b"data",
            }
            mock_analyze.return_value = (
                pd.DataFrame({"total_ha": [100.0]}, index=["Forest"]),
                {"vegetation_communities": []},
            )
            mock_save.side_effect = Exception("Save failed")

            result = await command.execute(valid_context)

            assert result.is_failure()
            assert "Command execution failed" in result.error_message


class TestVegetationSchemaIntegration:
    """Integration tests for vegetation schema system with VegetationResolveCommand."""

    @pytest.fixture
    def temp_schema_config(self):
        """Create a temporary schema configuration file for testing."""
        config_data = {
            "park_units": [
                {
                    "id": "JOTR",
                    "name": "Joshua Tree National Park",
                    "layer_name": "JOTR_VegPolys",
                    "vegetation_type_field": "MapUnit_Name",
                    "description_field": "MapUnit_Name",
                    "geometry_column": "geometry",
                    "preserve_fields": ["OBJECTID", "Shape_Area", "Shape_Length"],
                },
                {
                    "id": "MOJN",
                    "name": "Mojave National Preserve",
                    "layer_name": None,
                    "vegetation_type_field": "MAP_DESC",
                    "description_field": "MAP_DESC",
                    "geometry_column": "geometry",
                    "preserve_fields": ["FID", "AREA", "PERIMETER"],
                },
                {
                    "id": "DEFAULT",
                    "name": "Default Vegetation Schema",
                    "layer_name": None,
                    "vegetation_type_field": "veg_type",
                    "description_field": None,
                    "geometry_column": "geometry",
                    "preserve_fields": None,
                },
            ]
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name

        yield temp_path

        # Cleanup
        Path(temp_path).unlink(missing_ok=True)

    @pytest.fixture
    def schema_command_context(
        self,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
        sample_geometry: Polygon,
        temp_schema_config: str,
    ) -> CommandContext:
        """Create CommandContext for schema integration tests."""
        # Reset singleton instance to use test config
        VegetationSchemaLoader._instance = None

        # Create context with schema configuration
        context = CommandContext(
            job_id="schema_test_job_123",
            fire_event_name="schema_test_fire",
            geometry=sample_geometry,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
            severity_breaks=[0.1, 0.27, 0.66],
            metadata={
                "veg_gpkg_url": "https://example.com/vegetation.gpkg",
                "fire_cog_url": "https://example.com/fire_severity.tif",
                "geojson_url": "https://example.com/boundary.geojson",
            },
        )
        return context

    def setup_method(self):
        """Reset schema loader singleton before each test."""
        VegetationSchemaLoader._instance = None

    def test_validate_context_with_valid_park_unit_id(
        self, schema_command_context: CommandContext, temp_schema_config: str
    ):
        """Test context validation with valid park unit ID."""
        # Patch the schema loader to use test config
        with patch.object(VegetationSchemaLoader, "_instance", None):
            with patch(
                "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
            ) as mock_loader_class:
                mock_loader = Mock()
                mock_loader.has_schema.return_value = True
                mock_loader_class.get_instance.return_value = mock_loader

                command = VegetationResolveCommand()
                schema_command_context.metadata["park_unit_id"] = "JOTR"

                is_valid, error_msg = command.validate_context(schema_command_context)

                assert is_valid is True
                assert error_msg == "Context validation passed"
                mock_loader.has_schema.assert_called_once_with("JOTR")

    def test_validate_context_with_invalid_park_unit_id(
        self, schema_command_context: CommandContext, temp_schema_config: str
    ):
        """Test context validation with invalid park unit ID."""
        with patch.object(VegetationSchemaLoader, "_instance", None):
            with patch(
                "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
            ) as mock_loader_class:
                mock_loader = Mock()
                mock_loader.has_schema.return_value = False
                mock_loader.list_available_parks.return_value = [
                    "JOTR",
                    "MOJN",
                    "DEFAULT",
                ]
                mock_loader_class.get_instance.return_value = mock_loader

                command = VegetationResolveCommand()
                schema_command_context.metadata["park_unit_id"] = "INVALID_PARK"

                is_valid, error_msg = command.validate_context(schema_command_context)

                assert is_valid is False
                assert "Unknown park unit 'INVALID_PARK'" in error_msg
                assert "Available: JOTR, MOJN, DEFAULT" in error_msg

    def test_validate_context_without_park_unit_id(
        self, schema_command_context: CommandContext
    ):
        """Test context validation without park unit ID (backward compatibility)."""
        command = VegetationResolveCommand()
        # No park_unit_id in metadata - should still be valid

        is_valid, error_msg = command.validate_context(schema_command_context)

        assert is_valid is True
        assert error_msg == "Context validation passed"

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_vegetation_with_jotr_schema_integration(
        self, mock_gpd_read, temp_schema_config: str
    ):
        """Test loading vegetation data with JOTR schema through integration."""
        # Reset singleton and create command with test config
        VegetationSchemaLoader._instance = None

        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            # Set up schema loader mock
            mock_loader = Mock()
            mock_schema = VegetationSchema(
                layer_name="JOTR_VegPolys",
                vegetation_type_field="MapUnit_Name",
                description_field="MapUnit_Name",
                geometry_column="geometry",
                preserve_fields=["OBJECTID", "Shape_Area", "Shape_Length"],
            )
            mock_loader.get_schema.return_value = mock_schema
            mock_loader_class.get_instance.return_value = mock_loader

            # Mock vegetation data
            mock_gdf = Mock()
            mock_gdf.columns = ["MapUnit_Name", "OBJECTID", "geometry", "veg_type"]
            mock_gdf.__getitem__ = Mock(
                return_value=["Desert Scrub", "Joshua Tree Woodland"]
            )
            mock_gdf.__setitem__ = Mock()
            mock_gdf.__contains__ = Mock(
                side_effect=lambda col: col
                in ["MapUnit_Name", "OBJECTID", "geometry", "veg_type"]
            )
            mock_gdf.crs = "EPSG:4326"
            mock_gdf.to_crs = Mock(return_value=mock_gdf)
            mock_gdf.dropna = Mock(return_value=mock_gdf)
            mock_gdf.__len__ = Mock(return_value=2)

            mock_gpd_read.return_value = mock_gdf

            command = VegetationResolveCommand()
            veg_data = b"mock_jotr_vegetation_data"

            result = await command._load_vegetation_data_from_bytes(
                veg_data, "EPSG:32611", "JOTR"
            )

            assert result == mock_gdf
            mock_loader.get_schema.assert_called_once_with("JOTR")
            mock_gpd_read.assert_called_once()

            # Verify layer name was used
            args, kwargs = mock_gpd_read.call_args
            assert kwargs["layer"] == "JOTR_VegPolys"

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_vegetation_with_mojn_schema_integration(
        self, mock_gpd_read, temp_schema_config: str
    ):
        """Test loading vegetation data with MOJN schema through integration."""
        VegetationSchemaLoader._instance = None

        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            # Set up schema loader mock for MOJN (no layer name)
            mock_loader = Mock()
            mock_schema = VegetationSchema(
                layer_name=None,  # MOJN uses default layer
                vegetation_type_field="MAP_DESC",
                description_field="MAP_DESC",
                geometry_column="geometry",
                preserve_fields=["FID", "AREA", "PERIMETER"],
            )
            mock_loader.get_schema.return_value = mock_schema
            mock_loader_class.get_instance.return_value = mock_loader

            # Mock vegetation data
            mock_gdf = Mock()
            mock_gdf.columns = ["MAP_DESC", "FID", "geometry", "veg_type"]
            mock_gdf.__getitem__ = Mock(
                return_value=["Creosote Bush Scrub", "Desert Pavement"]
            )
            mock_gdf.__setitem__ = Mock()
            mock_gdf.__contains__ = Mock(
                side_effect=lambda col: col
                in ["MAP_DESC", "FID", "geometry", "veg_type"]
            )
            mock_gdf.crs = "EPSG:4326"
            mock_gdf.to_crs = Mock(return_value=mock_gdf)
            mock_gdf.dropna = Mock(return_value=mock_gdf)
            mock_gdf.__len__ = Mock(return_value=2)

            mock_gpd_read.return_value = mock_gdf

            command = VegetationResolveCommand()
            veg_data = b"mock_mojn_vegetation_data"

            result = await command._load_vegetation_data_from_bytes(
                veg_data, "EPSG:32611", "MOJN"
            )

            assert result == mock_gdf
            mock_loader.get_schema.assert_called_once_with("MOJN")

            # Verify no layer name was used (default layer)
            args, kwargs = mock_gpd_read.call_args
            assert "layer" not in kwargs

    @patch("src.commands.impl.vegetation_resolve_command.detect_vegetation_schema")
    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_schema_auto_detection_fallback(
        self, mock_gpd_read, mock_detect_schema, temp_schema_config: str
    ):
        """Test auto-detection fallback when schema loading fails."""
        VegetationSchemaLoader._instance = None

        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            # Set up schema loader to fail for UNKNOWN but succeed for others in Strategy 2
            mock_loader = Mock()

            def get_schema_side_effect(park_id):
                if park_id == "UNKNOWN":
                    raise Exception("Schema not found")
                else:
                    # Return valid schemas for Strategy 2 but they'll fail at gpd.read_file level
                    return VegetationSchema(
                        layer_name=f"{park_id}_VegPolys" if park_id == "JOTR" else None,
                        vegetation_type_field="MapUnit_Name",
                        geometry_column="geometry",
                    )

            mock_loader.get_schema.side_effect = get_schema_side_effect
            mock_loader.list_available_parks.return_value = ["JOTR", "MOJN", "DEFAULT"]
            mock_loader_class.get_instance.return_value = mock_loader

            # Mock successful auto-detection
            mock_gdf = Mock()
            mock_gdf.columns = ["MapUnit_Name", "OBJECTID", "geometry", "veg_type"]
            mock_gdf.__contains__ = Mock(
                side_effect=lambda col: col
                in ["MapUnit_Name", "OBJECTID", "geometry", "veg_type"]
            )
            mock_gdf.__getitem__ = Mock(
                return_value=pd.Series(["Desert Scrub", "Joshua Tree"])
            )
            mock_gdf.__setitem__ = Mock()
            mock_gdf.copy = Mock(return_value=mock_gdf)
            mock_gdf.crs = "EPSG:4326"
            mock_gdf.to_crs = Mock(return_value=mock_gdf)
            mock_gdf.dropna = Mock(return_value=mock_gdf)
            mock_gdf.__len__ = Mock(return_value=2)

            # Strategy 1: Park unit schema (UNKNOWN) will fail at get_schema() level, not gpd.read_file
            # Strategy 2: Schema attempts fail at gpd.read_file level for JOTR, MOJN, DEFAULT
            # Strategy 3: Auto-detection succeeds
            mock_gpd_read.side_effect = [
                Exception("Layer not found"),  # JOTR schema fails
                Exception("Layer not found"),  # MOJN schema fails
                Exception("Layer not found"),  # DEFAULT schema fails
                mock_gdf,  # Auto-detection succeeds (Strategy 3)
            ]

            # Mock the schema detection
            mock_detected_schema = VegetationSchema(
                vegetation_type_field="MapUnit_Name", geometry_column="geometry"
            )
            mock_detect_schema.return_value = mock_detected_schema

            command = VegetationResolveCommand()
            veg_data = b"mock_vegetation_data"

            result = await command._load_vegetation_data_from_bytes(
                veg_data, "EPSG:32611", "UNKNOWN"
            )

            assert result == mock_gdf
            # Should have tried schema loading and then auto-detection
            assert mock_gpd_read.call_count >= 1
            mock_detect_schema.assert_called_once()

    @pytest.mark.asyncio
    async def test_end_to_end_schema_integration(
        self, schema_command_context: CommandContext
    ):
        """Test end-to-end schema integration flow."""
        VegetationSchemaLoader._instance = None

        # Set up park unit ID in context
        schema_command_context.metadata["park_unit_id"] = "JOTR"

        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            # Set up schema loader
            mock_loader = Mock()
            mock_loader.has_schema.return_value = True
            mock_schema = VegetationSchema(
                layer_name="JOTR_VegPolys",
                vegetation_type_field="MapUnit_Name",
                description_field="MapUnit_Name",
                geometry_column="geometry",
            )
            mock_loader.get_schema.return_value = mock_schema
            mock_loader_class.get_instance.return_value = mock_loader

            command = VegetationResolveCommand()

            # Mock all the necessary methods for full workflow
            with (
                patch.object(command, "_download_input_files") as mock_download,
                patch.object(command, "_analyze_vegetation_impact") as mock_analyze,
                patch.object(command, "_save_analysis_reports") as mock_save,
                patch.object(command, "_create_vegetation_stac_metadata") as mock_stac,
            ):
                # Setup mock returns
                mock_download.return_value = {
                    "vegetation": b"jotr_data",
                    "fire_severity": b"fire_data",
                    "boundary": b"boundary_data",
                }
                mock_analyze.return_value = (
                    pd.DataFrame(
                        {"total_ha": [150.0, 75.0]},
                        index=["Desert Scrub", "Joshua Tree Woodland"],
                    ),
                    {
                        "vegetation_communities": [
                            {"name": "Desert Scrub", "total_hectares": 150.0},
                            {"name": "Joshua Tree Woodland", "total_hectares": 75.0},
                        ]
                    },
                )
                mock_save.return_value = {
                    "vegetation_matrix_csv": "csv_url",
                    "vegetation_matrix_json": "json_url",
                }
                mock_stac.return_value = "stac://jotr-item"

                result = await command.execute(schema_command_context)

                assert result.is_success()
                assert result.job_id == "schema_test_job_123"
                assert result.fire_event_name == "schema_test_fire"

                # Verify schema validation was called
                mock_loader.has_schema.assert_called_once_with("JOTR")

                # Verify all workflow steps executed
                mock_download.assert_called_once()
                mock_analyze.assert_called_once()
                mock_save.assert_called_once()
                mock_stac.assert_called_once()

    @pytest.mark.asyncio
    async def test_backward_compatibility_without_park_unit_id(
        self, schema_command_context: CommandContext
    ):
        """Test backward compatibility when park_unit_id is not provided."""
        # Don't set park_unit_id - should still work with auto-detection
        command = VegetationResolveCommand()

        with (
            patch.object(command, "_download_input_files") as mock_download,
            patch.object(command, "_analyze_vegetation_impact") as mock_analyze,
            patch.object(command, "_save_analysis_reports") as mock_save,
            patch.object(command, "_create_vegetation_stac_metadata") as mock_stac,
        ):
            # Setup mock returns
            mock_download.return_value = {
                "vegetation": b"data",
                "fire_severity": b"data",
                "boundary": b"data",
            }
            mock_analyze.return_value = (
                pd.DataFrame({"total_ha": [100.0]}, index=["Generic Vegetation"]),
                {"vegetation_communities": []},
            )
            mock_save.return_value = {
                "vegetation_matrix_csv": "csv_url",
                "vegetation_matrix_json": "json_url",
            }
            mock_stac.return_value = "stac://item"

            result = await command.execute(schema_command_context)

            assert result.is_success()
            # Verify workflow completed without park_unit_id
            mock_analyze.assert_called_once()
            # The analyze call should have None for park_unit_id
            call_args = mock_analyze.call_args
            assert call_args[0][2] is None  # park_unit_id parameter


class TestVegetationSchemaErrorScenarios:
    """Test error scenarios for vegetation schema system."""

    def setup_method(self):
        """Reset schema loader singleton before each test."""
        VegetationSchemaLoader._instance = None

    @pytest.fixture
    def error_context(
        self,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
        sample_geometry: Polygon,
    ) -> CommandContext:
        """Create CommandContext for error scenario testing."""
        return CommandContext(
            job_id="error_test_job",
            fire_event_name="error_test_fire",
            geometry=sample_geometry,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
            severity_breaks=[0.1, 0.27, 0.66],
            metadata={
                "veg_gpkg_url": "https://example.com/vegetation.gpkg",
                "fire_cog_url": "https://example.com/fire_severity.tif",
                "geojson_url": "https://example.com/boundary.geojson",
            },
        )

    def test_schema_loader_initialization_error(self, error_context: CommandContext):
        """Test behavior when schema loader fails to initialize."""
        error_context.metadata["park_unit_id"] = "JOTR"

        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            # Make schema loader initialization fail during command construction
            mock_loader_class.get_instance.side_effect = FileNotFoundError(
                "Config file not found"
            )

            # Should raise the original error during command initialization
            with pytest.raises(FileNotFoundError):
                VegetationResolveCommand()

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_all_schema_loading_strategies_fail(self, mock_gpd_read):
        """Test when all schema loading strategies fail."""
        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            # Set up schema loader to fail
            mock_loader = Mock()
            mock_loader.get_schema.side_effect = Exception("Schema loading failed")
            mock_loader.list_available_parks.return_value = ["JOTR", "MOJN"]
            mock_loader_class.get_instance.return_value = mock_loader

            # All attempts fail
            mock_gpd_read.side_effect = Exception("All loading failed")

            command = VegetationResolveCommand()
            veg_data = b"mock_vegetation_data"

            with pytest.raises(ValueError, match="Unable to load vegetation data"):
                await command._load_vegetation_data_from_bytes(
                    veg_data, "EPSG:32611", "UNKNOWN"
                )

    @pytest.mark.asyncio
    async def test_schema_validation_with_corrupted_config(
        self, error_context: CommandContext
    ):
        """Test schema validation when configuration is corrupted."""
        error_context.metadata["park_unit_id"] = "JOTR"

        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            # Simulate corrupted configuration
            mock_loader = Mock()
            mock_loader.has_schema.side_effect = json.JSONDecodeError(
                "Invalid JSON", "", 0
            )
            mock_loader_class.get_instance.return_value = mock_loader

            command = VegetationResolveCommand()

            with pytest.raises(json.JSONDecodeError):
                command.validate_context(error_context)

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_vegetation_data_missing_required_fields(self, mock_gpd_read):
        """Test loading vegetation data that's missing required fields after schema application."""
        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            mock_loader = Mock()
            mock_schema = VegetationSchema(
                layer_name="TestLayer",
                vegetation_type_field="VEG_TYPE",
                geometry_column="geometry",
            )
            mock_loader.get_schema.return_value = mock_schema
            mock_loader.list_available_parks.return_value = ["JOTR", "MOJN", "DEFAULT"]
            mock_loader_class.get_instance.return_value = mock_loader

            # Mock GeoDataFrame without required vegetation type column
            mock_gdf = Mock()
            mock_gdf.columns = ["geometry", "other_field"]  # Missing VEG_TYPE
            mock_gdf.__contains__ = Mock(
                side_effect=lambda col: col in ["geometry", "other_field"]
            )

            mock_gpd_read.return_value = mock_gdf

            command = VegetationResolveCommand()
            veg_data = b"mock_vegetation_data"

            # Test should fail when using park-specific schema that requires VEG_TYPE field that's missing
            with pytest.raises(ValueError, match="Unable to load vegetation data"):
                await command._load_vegetation_data_from_bytes(
                    veg_data, "EPSG:32611", "TEST_PARK"
                )

    @pytest.mark.asyncio
    async def test_execute_with_schema_loading_failure(
        self, error_context: CommandContext
    ):
        """Test full execution when schema loading fails during vegetation processing."""
        error_context.metadata["park_unit_id"] = "JOTR"

        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            # Set up a working schema loader for validation
            mock_loader = Mock()
            mock_loader.has_schema.return_value = True
            mock_loader_class.get_instance.return_value = mock_loader

            command = VegetationResolveCommand()

            with (
                patch.object(command, "_download_input_files") as mock_download,
                patch.object(command, "_analyze_vegetation_impact") as mock_analyze,
            ):
                mock_download.return_value = {
                    "vegetation": b"data",
                    "fire_severity": b"data",
                    "boundary": b"data",
                }
                # Make vegetation analysis fail with schema error
                mock_analyze.side_effect = ValueError(
                    "Unable to load vegetation data: Schema error"
                )

                result = await command.execute(error_context)

                assert result.is_failure()
                assert "Command execution failed" in result.error_message
                assert "Schema error" in result.error_message

    def test_thread_safety_of_schema_integration(self, error_context: CommandContext):
        """Test thread safety when multiple threads access schema system simultaneously."""
        error_context.metadata["park_unit_id"] = "JOTR"

        with patch(
            "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
        ) as mock_loader_class:
            mock_loader = Mock()
            mock_loader.has_schema.return_value = True
            mock_loader_class.get_instance.return_value = mock_loader

            results = []
            errors = []

            def validate_context():
                try:
                    command = VegetationResolveCommand()
                    is_valid, error_msg = command.validate_context(error_context)
                    results.append((is_valid, error_msg))
                except Exception as e:
                    errors.append(e)

            # Create multiple threads
            threads = []
            for _ in range(10):
                thread = threading.Thread(target=validate_context)
                threads.append(thread)

            # Start all threads
            for thread in threads:
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            # Verify results
            assert len(errors) == 0, f"Errors occurred: {errors}"
            assert len(results) == 10

            # All results should be valid
            for is_valid, error_msg in results:
                assert is_valid is True
                assert error_msg == "Context validation passed"


class TestZonalStatisticsBugFixes:
    """Test fixes for zonal statistics bugs - stdev parameter and per-class statistics."""

    @pytest.mark.asyncio
    async def test_zonal_stats_uses_stdev_not_std(self):
        """Test that zonal_stats calls use 'stdev' parameter, not 'std'."""
        command = VegetationResolveCommand()

        # Create mock data
        import numpy as np
        import geopandas as gpd

        # Mock DataArray with required methods
        mock_data_array = Mock()
        mock_data_array.xvec = Mock()

        # Mock the zonal_stats return value
        mock_stats_result = Mock()
        mock_stats_result.isel = Mock(side_effect=[
            Mock(values=np.array([100.0])),  # count
            Mock(values=np.array([0.5])),     # mean
            Mock(values=np.array([0.1])),     # stdev
        ])
        mock_stats_result.dims = ["zonal_statistics"]
        mock_data_array.xvec.zonal_stats = Mock(return_value=mock_stats_result)

        # Create mock vegetation subset
        mock_veg_subset = Mock(spec=gpd.GeoDataFrame)
        mock_veg_subset.geometry = Mock()

        # Create mock metadata
        metadata = {
            "x_coord": "x",
            "y_coord": "y",
            "pixel_area_ha": 0.09,
        }

        # Call the method
        mean, std = command._calculate_overall_severity(
            mock_data_array, mock_veg_subset, metadata
        )

        # Verify zonal_stats was called with correct parameters
        mock_data_array.xvec.zonal_stats.assert_called_once()
        call_args = mock_data_array.xvec.zonal_stats.call_args

        # Verify stats parameter uses "stdev" not "std"
        assert "stats" in call_args[1]
        assert call_args[1]["stats"] == ["count", "mean", "stdev"]
        assert "std" not in call_args[1]["stats"]

        # Verify method parameter
        assert call_args[1]["method"] == "exactextract"

        # Verify results are computed correctly
        assert mean == 0.5
        assert std == 0.1

    @pytest.mark.asyncio
    async def test_severity_class_stats_calculation(self):
        """Test that _calculate_severity_class_stats properly calculates mean and std."""
        command = VegetationResolveCommand()

        import numpy as np
        import geopandas as gpd

        # Mock DataArray with required methods
        mock_mask_data = Mock()
        mock_mask_data.xvec = Mock()

        # Mock the zonal_stats return value
        mock_stats_result = Mock()
        mock_stats_result.isel = Mock(side_effect=[
            Mock(values=np.array([50.0, 50.0])),      # count values for 2 polygons
            Mock(values=np.array([0.25, 0.35])),      # mean values for 2 polygons
            Mock(values=np.array([0.05, 0.08])),      # stdev values for 2 polygons
        ])
        mock_stats_result.dims = ["zonal_statistics"]
        mock_mask_data.xvec.zonal_stats = Mock(return_value=mock_stats_result)

        # Create mock vegetation subset
        mock_veg_subset = Mock(spec=gpd.GeoDataFrame)
        mock_veg_subset.geometry = Mock()

        # Create mock metadata
        metadata = {
            "x_coord": "x",
            "y_coord": "y",
            "pixel_area_ha": 0.09,
        }

        # Call the method
        mean, std = command._calculate_severity_class_stats(
            mock_mask_data, mock_veg_subset, metadata
        )

        # Verify zonal_stats was called with correct parameters
        mock_mask_data.xvec.zonal_stats.assert_called_once()
        call_args = mock_mask_data.xvec.zonal_stats.call_args

        # Verify stats parameter
        assert call_args[1]["stats"] == ["count", "mean", "stdev"]
        assert call_args[1]["method"] == "exactextract"

        # Verify pixel-weighted mean calculation
        # (50*0.25 + 50*0.35) / (50+50) = 30/100 = 0.3
        assert mean == 0.3

        # Verify std is the mean of stdev values
        # (0.05 + 0.08) / 2 = 0.065
        assert std == 0.065

    @pytest.mark.asyncio
    async def test_zonal_statistics_populates_per_class_stats(self):
        """Test that _calculate_zonal_statistics populates mean and std for each severity class."""
        command = VegetationResolveCommand()

        import geopandas as gpd

        # Create mock masks
        mock_masks = {}
        for severity in ["unburned", "low", "moderate", "high", "original"]:
            mock_mask = Mock()
            mock_mask.xvec = Mock()
            mock_masks[severity] = mock_mask

        # Mock pixel count calculations
        with patch.object(command, '_calculate_severity_pixels') as mock_pixels:
            # Return different pixel counts for each severity class
            mock_pixels.side_effect = [10.0, 20.0, 30.0, 40.0]  # For each severity class

            # Mock severity class stats calculations
            with patch.object(command, '_calculate_severity_class_stats') as mock_class_stats:
                # Return different mean/std for each severity class
                mock_class_stats.side_effect = [
                    (0.05, 0.01),   # unburned
                    (0.20, 0.05),   # low
                    (0.45, 0.10),   # moderate
                    (0.75, 0.15),   # high
                ]

                # Mock overall severity calculation
                with patch.object(command, '_calculate_overall_severity') as mock_overall:
                    mock_overall.return_value = (0.35, 0.12)

                    # Create mock vegetation subset
                    mock_veg_subset = Mock(spec=gpd.GeoDataFrame)
                    mock_veg_subset.__len__ = Mock(return_value=5)
                    mock_veg_subset.geometry = Mock()
                    mock_veg_subset.geometry.area = Mock()
                    mock_veg_subset.geometry.area.sum = Mock(return_value=1000.0)

                    # Create mock metadata
                    metadata = {
                        "x_coord": "x",
                        "y_coord": "y",
                        "pixel_area_ha": 0.09,
                    }

                    # Call the method
                    results = await command._calculate_zonal_statistics(
                        mock_masks, mock_veg_subset, metadata
                    )

                    # Verify per-class statistics are populated with non-zero values
                    assert results["unburned_mean"] == 0.05
                    assert results["unburned_std"] == 0.01
                    assert results["low_mean"] == 0.20
                    assert results["low_std"] == 0.05
                    assert results["moderate_mean"] == 0.45
                    assert results["moderate_std"] == 0.10
                    assert results["high_mean"] == 0.75
                    assert results["high_std"] == 0.15

                    # Verify overall statistics
                    assert results["mean_severity"] == 0.35
                    assert results["std_dev"] == 0.12

                    # Verify hectares calculations
                    assert results["unburned_ha"] == 10.0 * 0.09
                    assert results["low_ha"] == 20.0 * 0.09
                    assert results["moderate_ha"] == 30.0 * 0.09
                    assert results["high_ha"] == 40.0 * 0.09

                    # Verify _calculate_severity_class_stats was called 4 times (once per severity class)
                    assert mock_class_stats.call_count == 4

    @pytest.mark.asyncio
    async def test_zonal_stats_error_handling_with_stdev(self):
        """Test that errors in zonal_stats with stdev parameter are handled gracefully."""
        command = VegetationResolveCommand()

        import geopandas as gpd

        # Mock DataArray that raises an error
        mock_data_array = Mock()
        mock_data_array.xvec = Mock()
        mock_data_array.xvec.zonal_stats = Mock(side_effect=Exception("Unsupported stat: std"))

        # Create mock vegetation subset
        mock_veg_subset = Mock(spec=gpd.GeoDataFrame)
        mock_veg_subset.geometry = Mock()

        # Create mock metadata
        metadata = {
            "x_coord": "x",
            "y_coord": "y",
            "pixel_area_ha": 0.09,
        }

        # Call should handle error and return zeros
        mean, std = command._calculate_overall_severity(
            mock_data_array, mock_veg_subset, metadata
        )

        # Should return 0,0 on error
        assert mean == 0.0
        assert std == 0.0

    def test_json_structure_includes_per_class_stats(self):
        """Test that JSON structure includes mean and std for each severity class."""
        command = VegetationResolveCommand()

        # Create DataFrame with per-class statistics
        df = pd.DataFrame(
            {
                "total_ha": [100.0],
                "unburned_ha": [30.0],
                "low_ha": [25.0],
                "moderate_ha": [25.0],
                "high_ha": [20.0],
                "unburned_percent": [30.0],
                "low_percent": [25.0],
                "moderate_percent": [25.0],
                "high_percent": [20.0],
                "unburned_mean": [0.05],
                "low_mean": [0.18],
                "moderate_mean": [0.45],
                "high_mean": [0.75],
                "unburned_std": [0.01],
                "low_std": [0.05],
                "moderate_std": [0.10],
                "high_std": [0.15],
            },
            index=["Joshua Tree Woodland"],
        )

        result = command._create_json_structure(df)

        # Verify structure
        assert "vegetation_communities" in result
        assert len(result["vegetation_communities"]) == 1

        community = result["vegetation_communities"][0]
        assert community["name"] == "Joshua Tree Woodland"

        # Verify each severity class has mean_severity and std_dev in breakdown
        for severity in ["unburned", "low", "moderate", "high"]:
            assert severity in community["severity_breakdown"]
            assert "mean_severity" in community["severity_breakdown"][severity]
            assert "std_dev" in community["severity_breakdown"][severity]

            # Verify values are not zero
            if severity == "unburned":
                assert community["severity_breakdown"][severity]["mean_severity"] == 0.05
                assert community["severity_breakdown"][severity]["std_dev"] == 0.01
            elif severity == "low":
                assert community["severity_breakdown"][severity]["mean_severity"] == 0.18
                assert community["severity_breakdown"][severity]["std_dev"] == 0.05
            elif severity == "moderate":
                assert community["severity_breakdown"][severity]["mean_severity"] == 0.45
                assert community["severity_breakdown"][severity]["std_dev"] == 0.1
            elif severity == "high":
                assert community["severity_breakdown"][severity]["mean_severity"] == 0.75
                assert community["severity_breakdown"][severity]["std_dev"] == 0.15

    @pytest.mark.asyncio
    async def test_result_dataframe_has_all_columns_initialized(self):
        """Test that result DataFrame is initialized with all required columns."""
        command = VegetationResolveCommand()

        import geopandas as gpd
        from shapely.geometry import Polygon as ShapelyPolygon

        # Create minimal mock data to test DataFrame initialization
        with (
            patch.object(command, "_load_fire_data_from_bytes") as mock_load_fire,
            patch.object(command, "_load_vegetation_data_from_bytes") as mock_load_veg,
            patch.object(command, "_load_boundary_data_from_bytes") as mock_load_boundary,
            patch.object(command, "_create_severity_masks") as mock_create_masks,
            patch.object(command, "_calculate_zonal_statistics") as mock_zonal_stats,
            patch("src.commands.impl.vegetation_resolve_command.gpd.clip") as mock_clip,
        ):
            # Mock fire data
            mock_fire_ds = Mock()
            mock_fire_ds.__getitem__ = Mock(return_value=Mock())
            metadata = {
                "crs": "EPSG:32611",
                "data_var": "band_data",
                "pixel_area_ha": 0.09,
                "x_coord": "x",
                "y_coord": "y",
            }
            mock_load_fire.return_value = (mock_fire_ds, metadata)

            # Mock vegetation data with 2 vegetation types
            veg_data = {
                "veg_type": ["Forest", "Shrubland"],
                "geometry": [
                    ShapelyPolygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
                    ShapelyPolygon([(2, 2), (3, 2), (3, 3), (2, 3)]),
                ],
            }
            mock_veg_gdf = gpd.GeoDataFrame(veg_data, crs="EPSG:32611")
            mock_load_veg.return_value = mock_veg_gdf
            mock_clip.return_value = mock_veg_gdf

            # Mock boundary data
            boundary_data = {
                "geometry": [ShapelyPolygon([(0, 0), (5, 0), (5, 5), (0, 5)])]
            }
            mock_boundary_gdf = gpd.GeoDataFrame(boundary_data, crs="EPSG:32611")
            mock_boundary_gdf.geometry.union_all = Mock(
                return_value=boundary_data["geometry"][0]
            )
            mock_load_boundary.return_value = mock_boundary_gdf

            # Mock severity masks
            mock_create_masks.return_value = {
                "unburned": Mock(),
                "low": Mock(),
                "moderate": Mock(),
                "high": Mock(),
                "original": Mock(),
            }

            # Mock zonal statistics to return all statistics
            mock_zonal_stats.return_value = {
                "unburned_ha": 10.0,
                "low_ha": 20.0,
                "moderate_ha": 15.0,
                "high_ha": 5.0,
                "total_pixel_count": 100,
                "unburned_mean": 0.05,
                "unburned_std": 0.01,
                "low_mean": 0.20,
                "low_std": 0.05,
                "moderate_mean": 0.45,
                "moderate_std": 0.10,
                "high_mean": 0.75,
                "high_std": 0.15,
                "mean_severity": 0.35,
                "std_dev": 0.12,
            }

            file_data = {
                "vegetation": b"veg_data",
                "fire_severity": b"fire_data",
                "boundary": b"boundary_data",
            }

            result_df, json_structure = await command._analyze_vegetation_impact(
                file_data, [0.1, 0.27, 0.66], None
            )

            # Verify all expected columns exist in the DataFrame
            expected_columns = [
                "unburned_ha",
                "low_ha",
                "moderate_ha",
                "high_ha",
                "total_ha",
                "unburned_mean",
                "unburned_std",
                "low_mean",
                "low_std",
                "moderate_mean",
                "moderate_std",
                "high_mean",
                "high_std",
                "mean_severity",
                "std_dev",
            ]

            for col in expected_columns:
                assert col in result_df.columns, f"Column '{col}' is missing from result DataFrame"

            # Verify all rows have non-NaN values for these columns
            for veg_type in result_df.index:
                for col in expected_columns:
                    value = result_df.loc[veg_type, col]
                    assert not pd.isna(value), f"Value for '{col}' in '{veg_type}' is NaN"
                    # Values should be numeric (float)
                    assert isinstance(value, (int, float)), f"Value for '{col}' in '{veg_type}' is not numeric"

            # Verify mean and std values match what was returned from zonal_stats
            assert result_df.loc["Forest", "unburned_mean"] == 0.05
            assert result_df.loc["Forest", "unburned_std"] == 0.01
            assert result_df.loc["Forest", "low_mean"] == 0.20
            assert result_df.loc["Forest", "low_std"] == 0.05


class TestVegetationAnalysisEdgeCases:
    """Test edge cases in vegetation analysis processing."""

    @pytest.fixture
    def edge_case_context(
        self,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
        sample_geometry: Polygon,
    ) -> CommandContext:
        """Create CommandContext for edge case testing."""
        return CommandContext(
            job_id="edge_case_job",
            fire_event_name="edge_case_fire",
            geometry=sample_geometry,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
            severity_breaks=[0.1, 0.27, 0.66],
            metadata={
                "veg_gpkg_url": "https://example.com/vegetation.gpkg",
                "fire_cog_url": "https://example.com/fire_severity.tif",
                "geojson_url": "https://example.com/boundary.geojson",
                "park_unit_id": "JOTR",
            },
        )

    @patch("src.commands.impl.vegetation_resolve_command.gpd.clip")
    @pytest.mark.asyncio
    async def test_empty_vegetation_dataset(self, mock_clip):
        """Test analysis with empty vegetation dataset."""
        command = VegetationResolveCommand()

        with (
            patch.object(command, "_load_vegetation_data_from_bytes") as mock_load_veg,
            patch.object(command, "_load_fire_data_from_bytes") as mock_load_fire,
            patch.object(
                command, "_load_boundary_data_from_bytes"
            ) as mock_load_boundary,
            patch.object(command, "_create_severity_masks") as mock_create_masks,
        ):
            # Mock empty vegetation data with proper geopandas handling
            import geopandas as gpd

            empty_gdf = gpd.GeoDataFrame(columns=["veg_type", "geometry"])
            empty_gdf.crs = "EPSG:32611"
            mock_load_veg.return_value = empty_gdf
            mock_clip.return_value = empty_gdf  # Return empty after clipping

            # Mock fire and boundary data (simplified since _create_severity_masks is mocked)
            mock_fire_ds = Mock()
            mock_fire_ds.__getitem__ = Mock(return_value=Mock())  # Mock dataset access
            metadata = {
                "crs": "EPSG:32611",
                "data_var": "band_data",  # Add missing data_var field
                "pixel_area_ha": 0.01,
                "x_coord": "x",
                "y_coord": "y",
            }
            mock_load_fire.return_value = (mock_fire_ds, metadata)
            mock_boundary_gdf = Mock()
            mock_load_boundary.return_value = mock_boundary_gdf

            # Mock severity masks creation to return empty masks
            mock_create_masks.return_value = {
                "unburned": Mock(),
                "low": Mock(),
                "moderate": Mock(),
                "high": Mock(),
                "original": Mock(),
            }

            # The method should handle empty datasets gracefully
            file_data = {
                "vegetation": b"empty_data",
                "fire_severity": b"fire_data",
                "boundary": b"boundary_data",
            }
            result_df, json_structure = await command._analyze_vegetation_impact(
                file_data, [0.1, 0.27, 0.66], "JOTR"
            )

            # Should return empty but properly structured results
            assert len(result_df) == 0
            assert json_structure == {"vegetation_communities": []}

    @pytest.mark.asyncio
    async def test_vegetation_data_with_null_types(self):
        """Test analysis with vegetation data containing null vegetation types."""
        command = VegetationResolveCommand()

        with patch.object(command, "_download_input_files") as mock_download:
            mock_download.return_value = {
                "vegetation": b"data_with_nulls",
                "fire_severity": b"fire_data",
                "boundary": b"boundary_data",
            }

            with (
                patch.object(command, "_load_fire_data_from_bytes") as mock_load_fire,
                patch.object(
                    command, "_load_boundary_data_from_bytes"
                ) as mock_load_boundary,
                patch(
                    "src.commands.impl.vegetation_resolve_command.gpd.read_file"
                ) as mock_gpd_read,
                patch(
                    "src.commands.impl.vegetation_resolve_command.VegetationSchemaLoader"
                ) as mock_loader_class,
                patch(
                    "src.commands.impl.vegetation_resolve_command.detect_vegetation_schema"
                ) as mock_detect_schema,
                patch(
                    "src.commands.impl.vegetation_resolve_command.gpd.clip"
                ) as mock_clip,
                patch.object(command, "_create_severity_masks") as mock_create_masks,
                patch.object(
                    command, "_calculate_zonal_statistics"
                ) as mock_calculate_zonal,
            ):
                # Mock schema loader
                mock_loader = Mock()
                mock_loader.list_available_parks.return_value = [
                    "JOTR",
                    "MOJN",
                    "DEFAULT",
                ]
                mock_loader.get_schema.side_effect = Exception(
                    "Schema loading failed"
                )  # Force fallback to auto-detection
                mock_loader_class.get_instance.return_value = mock_loader

                # Mock auto-detection
                from src.config.vegetation_schemas import VegetationSchema

                mock_detected_schema = VegetationSchema(
                    vegetation_type_field="veg_type", geometry_column="geometry"
                )
                mock_detect_schema.return_value = mock_detected_schema

                # Mock vegetation data with null values
                mock_gdf = Mock()
                mock_gdf.columns = ["veg_type", "geometry"]
                mock_gdf.__contains__ = Mock(
                    side_effect=lambda col: col in ["veg_type", "geometry"]
                )
                mock_gdf.__getitem__ = Mock(return_value=pd.Series(["Valid Type"]))
                mock_gdf.__setitem__ = Mock()
                mock_gdf.copy = Mock(return_value=mock_gdf)
                mock_gdf.crs = "EPSG:4326"
                mock_gdf.to_crs = Mock(return_value=mock_gdf)

                # Simulate dropna removing null vegetation types
                filtered_gdf = Mock()
                filtered_gdf.__len__ = Mock(
                    return_value=1
                )  # 1 remaining after removing nulls
                valid_series = pd.Series(["Valid Type"], name="veg_type")
                valid_series.unique = Mock(return_value=pd.Index(["Valid Type"]))
                filtered_gdf.__getitem__ = Mock(return_value=valid_series)
                filtered_gdf.__setitem__ = Mock()
                filtered_gdf.__contains__ = Mock(
                    side_effect=lambda col: col in ["veg_type", "geometry"]
                )
                filtered_gdf.crs = "EPSG:4326"
                filtered_gdf.to_crs = Mock(return_value=filtered_gdf)
                mock_gdf.dropna = Mock(return_value=filtered_gdf)
                mock_gdf.__len__ = Mock(return_value=3)  # 3 original features

                # Setup mock_gpd_read to handle the fallback scenario
                # First calls will fail for schema loading, final call succeeds for auto-detection
                mock_gpd_read.side_effect = [
                    Exception("Schema loading failed"),  # Park-specific schema fails
                    Exception(
                        "JOTR schema fails"
                    ),  # Auto-detection schema attempts fail
                    Exception("MOJN schema fails"),
                    Exception("DEFAULT schema fails"),
                    mock_gdf,  # Auto-detection fallback succeeds
                ]

                # Mock other components
                mock_fire_ds = Mock()
                mock_fire_ds.__getitem__ = Mock(
                    return_value=Mock()
                )  # For accessing fire_ds[data_var]
                mock_load_fire.return_value = (
                    mock_fire_ds,
                    {
                        "crs": "EPSG:32611",
                        "data_var": "band_data",
                        "pixel_area_ha": 0.01,
                    },
                )

                mock_boundary_gdf = Mock()
                mock_boundary_gdf.geometry.union_all.return_value = (
                    Mock()
                )  # Mock boundary geometry
                mock_load_boundary.return_value = mock_boundary_gdf

                # Mock gpd.clip to return the clipped vegetation data
                mock_clip.return_value = filtered_gdf

                # Mock severity masks and zonal statistics
                mock_create_masks.return_value = {
                    "unburned": Mock(),
                    "low": Mock(),
                    "moderate": Mock(),
                    "high": Mock(),
                }
                mock_calculate_zonal.return_value = {
                    "unburned_ha": 10.0,
                    "low_ha": 5.0,
                    "moderate_ha": 3.0,
                    "high_ha": 2.0,
                    "total_pixel_count": 100,
                    "unburned_mean": 0.05,
                    "low_mean": 0.15,
                    "moderate_mean": 0.4,
                    "high_mean": 0.8,
                    "unburned_std": 0.02,
                    "low_std": 0.05,
                    "moderate_std": 0.1,
                    "high_std": 0.15,
                    "mean_severity": 0.25,
                    "std_dev": 0.3,
                }

                file_data = {
                    "vegetation": b"data_with_nulls",
                    "fire_severity": b"fire_data",
                    "boundary": b"boundary_data",
                }

                # Should handle null values by filtering them out
                result_df, json_structure = await command._analyze_vegetation_impact(
                    file_data, [0.1, 0.27, 0.66], "JOTR"
                )

                # Verify dropna was called to remove null vegetation types
                mock_gdf.dropna.assert_called_once_with(subset=["veg_type"])

                # Verify the analysis completed successfully
                assert len(result_df) == 1  # One vegetation type after filtering
                assert "Valid Type" in result_df.index
                assert "vegetation_communities" in json_structure
                assert len(json_structure["vegetation_communities"]) == 1

    @pytest.mark.asyncio
    async def test_malformed_download_urls(self, edge_case_context: CommandContext):
        """Test download input files with malformed URLs."""
        command = VegetationResolveCommand()

        # Test various malformed URL scenarios
        invalid_urls = [
            ("", "fire.tif", "boundary.geojson"),  # Empty vegetation URL
            ("veg.gpkg", "", "boundary.geojson"),  # Empty fire URL
            ("veg.gpkg", "fire.tif", ""),  # Empty boundary URL
            (
                "invalid://scheme/veg.gpkg",
                "fire.tif",
                "boundary.geojson",
            ),  # Invalid scheme
            ("   ", "fire.tif", "boundary.geojson"),  # Whitespace only
            (None, "fire.tif", "boundary.geojson"),  # None URL
        ]

        for veg_url, fire_url, geojson_url in invalid_urls:
            with pytest.raises(ValueError, match="(Invalid.*URL|Unsupported.*URL)"):
                await command._download_input_files(
                    edge_case_context, veg_url, fire_url, geojson_url
                )

    @pytest.mark.asyncio
    async def test_empty_file_downloads(self, edge_case_context: CommandContext):
        """Test behavior when downloaded files are empty."""
        command = VegetationResolveCommand()

        # Mock storage to return empty files
        edge_case_context.storage.copy_from_url = AsyncMock()
        edge_case_context.storage.get_bytes.side_effect = [
            b"",  # Empty vegetation file
            b"mock_fire_data",
            b"mock_boundary_data",
        ]

        with pytest.raises(ValueError, match="Vegetation GPKG file is empty"):
            await command._download_input_files(
                edge_case_context,
                "https://example.com/vegetation.gpkg",
                "https://example.com/fire.tif",
                "https://example.com/boundary.geojson",
            )

    @pytest.mark.asyncio
    async def test_download_storage_failures(self, edge_case_context: CommandContext):
        """Test various storage failure scenarios during download."""
        command = VegetationResolveCommand()

        # Test copy_from_url failure
        edge_case_context.storage.copy_from_url.side_effect = Exception("Network error")

        with pytest.raises(
            ValueError, match="Failed to download vegetation GPKG.*Network error"
        ):
            await command._download_input_files(
                edge_case_context,
                "https://example.com/vegetation.gpkg",
                "https://example.com/fire.tif",
                "https://example.com/boundary.geojson",
            )

        # Test get_bytes failure
        edge_case_context.storage.copy_from_url.side_effect = None  # Reset
        edge_case_context.storage.copy_from_url = AsyncMock()
        edge_case_context.storage.get_bytes.side_effect = Exception(
            "Storage read error"
        )

        with pytest.raises(
            ValueError,
            match="Failed to retrieve vegetation GPKG data.*Storage read error",
        ):
            await command._download_input_files(
                edge_case_context,
                "https://example.com/vegetation.gpkg",
                "https://example.com/fire.tif",
                "https://example.com/boundary.geojson",
            )
