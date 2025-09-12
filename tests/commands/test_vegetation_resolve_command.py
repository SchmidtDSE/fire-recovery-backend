import pytest
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from io import BytesIO

from src.commands.impl.vegetation_resolve_command import VegetationResolveCommand
from src.commands.interfaces.command_context import CommandContext
from src.core.storage.interface import StorageInterface
from src.core.storage.storage_factory import StorageFactory
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry
from geojson_pydantic import Polygon


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
    temp_storage = Mock(spec=StorageInterface)
    temp_storage.save_bytes = AsyncMock(return_value="mock://temp/path")
    factory.get_temp_storage = Mock(return_value=temp_storage)
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
    return Polygon(
        type="Polygon", coordinates=[[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]
    )


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

    def test_get_command_name(self):
        """Test command name is correct"""
        command = VegetationResolveCommand()
        assert command.get_command_name() == "vegetation_resolve"

    def test_get_estimated_duration_seconds(self):
        """Test estimated duration is reasonable"""
        command = VegetationResolveCommand()
        duration = command.get_estimated_duration_seconds()
        assert isinstance(duration, float)
        assert 60 <= duration <= 600  # Between 1-10 minutes

    def test_supports_retry(self):
        """Test command supports retry"""
        command = VegetationResolveCommand()
        assert command.supports_retry() is True

    def test_get_dependencies(self):
        """Test command has no dependencies"""
        command = VegetationResolveCommand()
        assert command.get_dependencies() == []

    def test_get_required_permissions(self):
        """Test required permissions are comprehensive"""
        command = VegetationResolveCommand()
        permissions = command.get_required_permissions()
        expected = ["stac:read", "stac:write", "storage:write", "computation:execute"]
        assert all(perm in permissions for perm in expected)

    def test_validate_context_success(self, valid_context: CommandContext):
        """Test successful context validation"""
        command = VegetationResolveCommand()
        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is True
        assert error_msg == "Context validation passed"

    def test_validate_context_missing_job_id(self, valid_context: CommandContext):
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

    def test_validate_context_missing_storage(self, valid_context: CommandContext):
        """Test validation fails with missing storage"""
        command = VegetationResolveCommand()
        valid_context.storage = None

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "storage, storage_factory, and stac_manager are required" in error_msg

    def test_validate_context_missing_veg_gpkg_url(self, valid_context: CommandContext):
        """Test validation fails with missing veg_gpkg_url"""
        command = VegetationResolveCommand()
        valid_context.metadata.pop("veg_gpkg_url")

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "veg_gpkg_url is required in metadata" in error_msg

    def test_validate_context_missing_fire_cog_url(self, valid_context: CommandContext):
        """Test validation fails with missing fire_cog_url"""
        command = VegetationResolveCommand()
        valid_context.metadata.pop("fire_cog_url")

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "fire_cog_url is required in metadata" in error_msg

    def test_validate_context_missing_geojson_url(self, valid_context: CommandContext):
        """Test validation fails with missing geojson_url"""
        command = VegetationResolveCommand()
        valid_context.metadata.pop("geojson_url")

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

    def test_validate_context_none_severity_breaks(self, valid_context: CommandContext):
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
        mock_ds.data_vars = {"band_data": Mock()}
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
        """Test loading JOTR vegetation data from bytes"""
        command = VegetationResolveCommand()

        # Mock JOTR format vegetation data
        mock_gdf = Mock()
        mock_gdf.__getitem__ = Mock(return_value=["Forest", "Shrubland"])
        mock_gdf.__setitem__ = Mock()
        mock_gdf.crs = "EPSG:4326"
        mock_gdf.to_crs = Mock(return_value=mock_gdf)

        mock_gpd_read.return_value = mock_gdf

        veg_data = b"mock_vegetation_data"
        result = await command._load_vegetation_data_from_bytes(veg_data, "EPSG:32611")

        assert result == mock_gdf
        mock_gpd_read.assert_called_once()
        args, kwargs = mock_gpd_read.call_args
        assert isinstance(args[0], BytesIO)
        assert kwargs["layer"] == "JOTR_VegPolys"

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_vegetation_data_from_bytes_mojn_fallback(self, mock_gpd_read):
        """Test loading MOJN vegetation data from bytes as fallback"""
        command = VegetationResolveCommand()

        # Mock MOJN format vegetation data
        mock_gdf = Mock()
        mock_gdf.__getitem__ = Mock(return_value=["Desert", "Woodland"])
        mock_gdf.__setitem__ = Mock()
        mock_gdf.crs = "EPSG:4326"
        mock_gdf.to_crs = Mock(return_value=mock_gdf)

        # First call (JOTR) fails, second call (MOJN) succeeds
        mock_gpd_read.side_effect = [Exception("Layer not found"), mock_gdf]

        veg_data = b"mock_vegetation_data"
        result = await command._load_vegetation_data_from_bytes(veg_data, "EPSG:32611")

        assert result == mock_gdf
        assert mock_gpd_read.call_count == 2

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_vegetation_data_from_bytes_unsupported_format(
        self, mock_gpd_read
    ):
        """Test loading vegetation data with unsupported format"""
        command = VegetationResolveCommand()

        # Both formats fail
        mock_gpd_read.side_effect = [
            Exception("Layer not found"),
            Exception("Invalid format"),
        ]

        veg_data = b"mock_vegetation_data"

        with pytest.raises(ValueError, match="Unsupported vegetation data format"):
            await command._load_vegetation_data_from_bytes(veg_data, "EPSG:32611")

    @patch("src.commands.impl.vegetation_resolve_command.gpd.read_file")
    @pytest.mark.asyncio
    async def test_load_boundary_data_from_bytes(self, mock_gpd_read):
        """Test loading boundary data from bytes"""
        command = VegetationResolveCommand()

        mock_gdf = Mock()
        mock_gdf.to_crs.return_value = mock_gdf
        mock_gpd_read.return_value = mock_gdf

        boundary_data = b"mock_boundary_data"
        result = await command._load_boundary_data_from_bytes(boundary_data)

        assert result == mock_gdf
        mock_gpd_read.assert_called_once()
        args, kwargs = mock_gpd_read.call_args
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

        # Mock storage responses
        valid_context.storage.save_bytes.side_effect = [
            "mock://csv/url",
            "mock://json/url",
        ]

        result = await command._save_analysis_reports(
            valid_context, result_df, json_structure
        )

        assert result["vegetation_matrix_csv"] == "mock://csv/url"
        assert result["vegetation_matrix_json"] == "mock://json/url"

        # Verify storage was called twice (CSV and JSON)
        assert valid_context.storage.save_bytes.call_count == 2

    @pytest.mark.asyncio
    async def test_save_analysis_reports_storage_error(
        self, valid_context: CommandContext
    ):
        """Test analysis report saving with storage error"""
        command = VegetationResolveCommand()

        result_df = pd.DataFrame({"total_ha": [100.0]}, index=["Forest"])
        json_structure = {"vegetation_communities": []}

        # Mock storage error
        valid_context.storage.save_bytes.side_effect = Exception("Storage error")

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
        """Test STAC metadata creation with missing fire severity item"""
        command = VegetationResolveCommand()

        # Mock missing fire STAC item
        valid_context.stac_manager.get_items_by_id_and_coarseness.return_value = None

        asset_urls = {"vegetation_matrix_csv": "mock://csv/url"}
        severity_breaks = [0.1, 0.27, 0.66]

        with pytest.raises(ValueError, match="Fire severity STAC item not found"):
            await command._create_vegetation_stac_metadata(
                valid_context, asset_urls, severity_breaks
            )

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
