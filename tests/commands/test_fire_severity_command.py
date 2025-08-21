import pytest
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, Any
import xarray as xr
import numpy as np
import rioxarray

from src.commands.impl.fire_severity_command import FireSeverityAnalysisCommand
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult, CommandStatus
from src.core.storage.interface import StorageInterface
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry


class MockIndexCalculator:
    """Mock index calculator for testing"""

    def __init__(self, index_name: str):
        self._index_name = index_name

    @property
    def index_name(self) -> str:
        return self._index_name

    async def calculate(self, prefire_data: Any, postfire_data: Any, context: Dict[str, Any]) -> xr.DataArray:
        # Return mock xarray data
        data = xr.DataArray(
            np.random.random((10, 10)),
            dims=["y", "x"],
            coords={"y": range(10), "x": range(10)},
        )
        return data

    def requires_pre_and_post(self) -> bool:
        return True

    def get_dependencies(self) -> list:
        return []


@pytest.fixture
def mock_storage() -> Mock:
    """Create mock storage interface"""
    storage = Mock(spec=StorageInterface)
    storage.save_bytes = AsyncMock(return_value="mock://saved/path.tif")
    return storage


@pytest.fixture
def mock_stac_manager() -> Mock:
    """Create mock STAC manager"""
    manager = Mock(spec=STACJSONManager)
    manager.create_fire_severity_item = AsyncMock(return_value="mock://stac/item.json")
    return manager


@pytest.fixture
def mock_index_registry() -> Mock:
    """Create mock index registry with calculators"""
    registry = Mock(spec=IndexRegistry)

    # Create mock calculators
    nbr_calc = MockIndexCalculator("nbr")
    dnbr_calc = MockIndexCalculator("dnbr")

    def get_calculator(index_name: str) -> MockIndexCalculator | None:
        calculators = {
            "nbr": nbr_calc,
            "dnbr": dnbr_calc,
            "rdnbr": MockIndexCalculator("rdnbr"),
            "rbr": MockIndexCalculator("rbr"),
        }
        return calculators.get(index_name)

    registry.get_calculator = get_calculator
    return registry


@pytest.fixture
def command_context(mock_storage: Mock, mock_stac_manager: Mock, mock_index_registry: Mock) -> CommandContext:
    """Create test command context for fire severity analysis"""
    return CommandContext(
        job_id="test-fire-job-123",
        fire_event_name="test-wildfire",
        geometry={
            "type": "Polygon",
            "coordinates": [
                [[-120, 35], [-119, 35], [-119, 36], [-120, 36], [-120, 35]]
            ],
        },
        storage=mock_storage,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        computation_config={
            "prefire_date_range": ["2023-06-01", "2023-06-15"],
            "postfire_date_range": ["2023-07-01", "2023-07-15"],
            "collection": "sentinel-2-l2a",
            "buffer_meters": 100,
            "indices": ["nbr", "dnbr"],
        },
        metadata={"test_metadata": "fire_severity_test"},
    )


class TestFireSeverityAnalysisCommand:
    """Test FireSeverityAnalysisCommand functionality"""

    def test_command_properties(self):
        """Test basic command properties"""
        command = FireSeverityAnalysisCommand()

        assert command.get_command_name() == "fire_severity_analysis"
        assert command.get_estimated_duration_seconds() == 300.0
        assert command.supports_retry() is True
        assert command.get_dependencies() == []
        assert "stac:read" in command.get_required_permissions()
        assert "storage:write" in command.get_required_permissions()
        assert "computation:execute" in command.get_required_permissions()

    def test_context_validation_success(self, command_context):
        """Test successful context validation"""
        command = FireSeverityAnalysisCommand()
        assert command.validate_context(command_context) is True

    def test_context_validation_missing_dates(self, command_context):
        """Test context validation with missing date ranges"""
        command = FireSeverityAnalysisCommand()

        # Remove date ranges from computation config
        command_context.computation_config = {}
        assert command.validate_context(command_context) is False

    def test_context_validation_missing_geometry(self, command_context):
        """Test context validation with missing geometry"""
        command = FireSeverityAnalysisCommand()

        command_context.geometry = None
        assert command.validate_context(command_context) is False

    def test_context_validation_missing_storage(self, command_context):
        """Test context validation with missing storage"""
        command = FireSeverityAnalysisCommand()

        command_context.storage = None
        assert command.validate_context(command_context) is False

    def test_get_buffered_bounds(self):
        """Test buffered bounds calculation"""
        command = FireSeverityAnalysisCommand()

        geometry = {
            "type": "Polygon",
            "coordinates": [
                [[-120, 35], [-119, 35], [-119, 36], [-120, 36], [-120, 35]]
            ],
        }

        bounds = command._get_buffered_bounds(geometry, 100)

        # Should be a tuple of 4 coordinates
        assert len(bounds) == 4
        assert all(isinstance(b, float) for b in bounds)

        # Bounds should be expanded from original
        minx, miny, maxx, maxy = bounds
        assert minx < -120
        assert miny < 35
        assert maxx > -119
        assert maxy > 36

    def test_subset_data_by_date_range(self):
        """Test data subsetting by date range"""
        command = FireSeverityAnalysisCommand()

        # Create mock xarray data with time dimension
        time_coords = np.array(
            ["2023-06-01", "2023-06-15", "2023-07-01", "2023-07-15"], dtype="datetime64"
        )

        data = xr.DataArray(
            np.random.random((4, 10, 10)),
            dims=["time", "y", "x"],
            coords={"time": time_coords, "y": range(10), "x": range(10)},
        )

        # Subset to June data
        subset = command._subset_data_by_date_range(data, ["2023-06-01", "2023-06-15"])

        # Should contain only June data
        assert len(subset.time) == 2
        assert subset.time[0] == np.datetime64("2023-06-01")
        assert subset.time[1] == np.datetime64("2023-06-15")

    def test_prepare_data_for_cog(self):
        """Test data preparation for COG creation"""
        command = FireSeverityAnalysisCommand()

        # Create test data
        data = xr.DataArray(
            np.random.random((10, 10)),
            dims=["y", "x"],
            coords={"y": range(10), "x": range(10)},
        )

        prepared = command._prepare_data_for_cog(data)

        # Should be float32
        assert prepared.dtype == np.float32

        # Should have nodata value set
        assert prepared.rio.nodata == -9999.0

        # Should have CRS set
        assert prepared.rio.crs is not None

    @pytest.mark.asyncio
    @patch("src.commands.impl.fire_severity_command.StacEndpointHandler")
    @patch("src.commands.impl.fire_severity_command.stackstac")
    async def test_fetch_satellite_data(
        self, mock_stackstac, mock_stac_handler_class, command_context
    ):
        """Test satellite data fetching workflow"""
        command = FireSeverityAnalysisCommand()

        # Setup mocks
        mock_handler = Mock()
        mock_stac_handler_class.return_value = mock_handler

        mock_handler.search_items = AsyncMock(
            return_value=(
                ["item1", "item2"],  # Mock STAC items
                {"nir_band": "B08", "swir_band": "B12", "epsg": 4326},  # Mock config
            )
        )
        mock_handler.get_band_names.return_value = ("B08", "B12")
        mock_handler.get_epsg_code.return_value = 4326

        # Mock stackstac.stack
        mock_data = xr.DataArray(
            np.random.random((4, 2, 10, 10)),
            dims=["time", "band", "y", "x"],
            coords={
                "time": np.array(
                    ["2023-06-01", "2023-06-15", "2023-07-01", "2023-07-15"],
                    dtype="datetime64",
                ),
                "band": ["B08", "B12"],
                "y": range(10),
                "x": range(10),
            },
        )
        mock_stackstac.stack.return_value = mock_data

        # Test the method
        result = await command._fetch_satellite_data(
            command_context,
            ["2023-06-01", "2023-06-15"],
            ["2023-07-01", "2023-07-15"],
            "sentinel-2-l2a",
            100.0,
        )

        # Verify results
        assert "prefire_data" in result
        assert "postfire_data" in result
        assert "nir_band" in result
        assert "swir_band" in result
        assert result["nir_band"] == "B08"
        assert result["swir_band"] == "B12"

    @pytest.mark.asyncio
    async def test_calculate_burn_indices(self, command_context):
        """Test burn indices calculation using strategy pattern"""
        command = FireSeverityAnalysisCommand()

        # Create mock stac data
        stac_data = {
            "prefire_data": xr.DataArray(
                np.random.random((2, 2, 10, 10)), dims=["time", "band", "x", "y"]
            ),
            "postfire_data": xr.DataArray(
                np.random.random((2, 2, 10, 10)), dims=["time", "band", "x", "y"]
            ),
            "nir_band": "B08",
            "swir_band": "B12",
        }

        # Test calculation
        result = await command._calculate_burn_indices(
            command_context, stac_data, ["nbr", "dnbr"]
        )

        # Verify results
        assert "nbr" in result
        assert "dnbr" in result
        assert isinstance(result["nbr"], xr.DataArray)
        assert isinstance(result["dnbr"], xr.DataArray)

    @pytest.mark.asyncio
    async def test_save_results_as_cogs(self, command_context):
        """Test saving results as COGs"""
        command = FireSeverityAnalysisCommand()

        # Create mock index results
        index_results = {
            "nbr": xr.DataArray(np.random.random((10, 10)), dims=["y", "x"]),
            "dnbr": xr.DataArray(np.random.random((10, 10)), dims=["y", "x"]),
        }

        # Test saving
        asset_urls = await command._save_results_as_cogs(command_context, index_results)

        # Verify results
        assert "nbr" in asset_urls
        assert "dnbr" in asset_urls
        assert asset_urls["nbr"] == "mock://saved/path.tif"
        assert asset_urls["dnbr"] == "mock://saved/path.tif"

        # Verify storage was called
        assert command_context.storage.save_bytes.call_count == 2

    @pytest.mark.asyncio
    async def test_create_stac_metadata(self, command_context):
        """Test STAC metadata creation"""
        command = FireSeverityAnalysisCommand()

        asset_urls = {"nbr": "mock://nbr.tif", "dnbr": "mock://dnbr.tif"}

        result = await command._create_stac_metadata(
            command_context,
            asset_urls,
            ["2023-06-01", "2023-06-15"],
            ["2023-07-01", "2023-07-15"],
        )

        # Verify STAC item was created
        assert result == "mock://stac/item.json"
        command_context.stac_manager.create_fire_severity_item.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.commands.impl.fire_severity_command.StacEndpointHandler")
    @patch("src.commands.impl.fire_severity_command.stackstac")
    async def test_execute_success(
        self, mock_stackstac, mock_stac_handler_class, command_context
    ):
        """Test successful command execution end-to-end"""
        command = FireSeverityAnalysisCommand()

        # Setup comprehensive mocks for full execution
        mock_handler = Mock()
        mock_stac_handler_class.return_value = mock_handler

        mock_handler.search_items = AsyncMock(
            return_value=(
                ["item1", "item2"],
                {"nir_band": "B08", "swir_band": "B12", "epsg": 4326},
            )
        )
        mock_handler.get_band_names.return_value = ("B08", "B12")
        mock_handler.get_epsg_code.return_value = 4326

        mock_data = xr.DataArray(
            np.random.random((4, 2, 10, 10)),
            dims=["time", "band", "y", "x"],
            coords={
                "time": np.array(
                    ["2023-06-01", "2023-06-15", "2023-07-01", "2023-07-15"],
                    dtype="datetime64",
                ),
                "band": ["B08", "B12"],
                "y": range(10),
                "x": range(10),
            },
        )
        mock_stackstac.stack.return_value = mock_data

        # Execute command
        result = await command.execute(command_context)

        # Verify success
        assert result.is_success()
        assert result.command_name == "fire_severity_analysis"
        assert result.job_id == "test-fire-job-123"
        assert result.execution_time_ms > 0
        assert result.data["analysis_complete"] is True
        assert "indices_calculated" in result.data
        assert result.has_assets()

    @pytest.mark.asyncio
    async def test_execute_failure_missing_config(self, command_context):
        """Test command execution failure with missing configuration"""
        command = FireSeverityAnalysisCommand()

        # Remove required config
        command_context.computation_config = {}

        result = await command.execute(command_context)

        # Verify failure
        assert result.is_failure()
        assert result.command_name == "fire_severity_analysis"
        assert result.error_message is not None
        assert not result.has_assets()


# Integration test that can be run standalone
if __name__ == "__main__":

    async def integration_test():
        """Simple integration test"""
        print("Running FireSeverityAnalysisCommand integration test...")

        command = FireSeverityAnalysisCommand()
        print(f"✅ Command created: {command.get_command_name()}")
        print(f"   Duration estimate: {command.get_estimated_duration_seconds()}s")
        print(f"   Supports retry: {command.supports_retry()}")
        print(f"   Dependencies: {command.get_dependencies()}")
        print(f"   Permissions: {command.get_required_permissions()}")

        # Test validation with minimal context
        from unittest.mock import Mock

        storage = Mock()
        stac_manager = Mock()
        index_registry = Mock()

        context = CommandContext(
            job_id="test-job",
            fire_event_name="test-fire",
            geometry={"type": "Point", "coordinates": [-120, 35]},
            storage=storage,
            stac_manager=stac_manager,
            index_registry=index_registry,
            computation_config={
                "prefire_date_range": ["2023-06-01", "2023-06-15"],
                "postfire_date_range": ["2023-07-01", "2023-07-15"],
            },
        )

        is_valid = command.validate_context(context)
        print(f"✅ Context validation: {is_valid}")

        print("🎉 Integration test completed successfully!")

    import asyncio

    asyncio.run(integration_test())
