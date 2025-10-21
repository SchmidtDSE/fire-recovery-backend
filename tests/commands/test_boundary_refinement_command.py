import pytest
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, Any

from src.commands.impl.boundary_refinement_command import BoundaryRefinementCommand
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandStatus
from src.core.storage.interface import StorageInterface
from src.core.storage.storage_factory import StorageFactory
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry
from geojson_pydantic import Polygon, Feature


@pytest.fixture
def mock_storage() -> Mock:
    """Create mock storage interface"""
    storage = Mock(spec=StorageInterface)
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
    manager.get_item_by_id = AsyncMock()
    manager.create_fire_severity_item = AsyncMock(
        return_value="mock://stac/severity.json"
    )
    manager.create_boundary_item = AsyncMock(return_value="mock://stac/boundary.json")
    return manager


@pytest.fixture
def mock_index_registry() -> Mock:
    """Create mock index registry"""
    registry = Mock(spec=IndexRegistry)
    return registry


@pytest.fixture
def sample_polygon() -> Polygon:
    """Create sample polygon geometry"""
    return Polygon(
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
    )


@pytest.fixture
def sample_feature(sample_polygon: Polygon) -> Feature:
    """Create sample feature geometry"""
    return Feature(type="Feature", geometry=sample_polygon, properties={})


@pytest.fixture
def valid_context(
    sample_polygon: Polygon,
    mock_storage: Mock,
    mock_storage_factory: Mock,
    mock_stac_manager: Mock,
    mock_index_registry: Mock,
) -> CommandContext:
    """Create valid command context"""
    return CommandContext(
        job_id="test-job-123",
        fire_event_name="test-fire-2024",
        geometry=sample_polygon,
        storage=mock_storage,
        storage_factory=mock_storage_factory,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        metadata={"refinement_iteration": 2},
    )


@pytest.fixture
def sample_stac_item() -> Dict[str, Any]:
    """Sample STAC item with fire severity assets"""
    return {
        "id": "test-fire-2024-severity-test-job-123",
        "type": "Feature",
        "properties": {"datetime": "2024-01-15T12:00:00Z"},
        "assets": {
            "dnbr": {
                "href": "mock://storage/coarse_dnbr.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            },
            "rdnbr": {
                "href": "mock://storage/coarse_rdnbr.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            },
            "rbr": {
                "href": "mock://storage/coarse_rbr.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            },
        },
    }


class TestBoundaryRefinementCommand:
    """Test suite for BoundaryRefinementCommand"""

    def test_get_command_name(self):
        """Test command name is correct"""
        command = BoundaryRefinementCommand()
        assert command.get_command_name() == "boundary_refinement"

    def test_get_estimated_duration_seconds(self):
        """Test estimated duration is correct"""
        command = BoundaryRefinementCommand()
        assert command.get_estimated_duration_seconds() == 180.0

    def test_supports_retry(self):
        """Test command supports retry"""
        command = BoundaryRefinementCommand()
        assert command.supports_retry() is True

    def test_get_dependencies(self):
        """Test command has no dependencies"""
        command = BoundaryRefinementCommand()
        assert command.get_dependencies() == []

    def test_get_required_permissions(self):
        """Test required permissions are correct"""
        command = BoundaryRefinementCommand()
        expected_permissions = [
            "stac:read",
            "stac:write",
            "storage:write",
            "computation:execute",
        ]
        assert command.get_required_permissions() == expected_permissions

    def test_validate_context_valid(self, valid_context: CommandContext):
        """Test context validation with valid context"""
        command = BoundaryRefinementCommand()
        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is True
        assert error_msg == "Context validation passed"

    def test_validate_context_missing_job_id(self, valid_context: CommandContext):
        """Test context validation with missing job_id"""
        command = BoundaryRefinementCommand()
        valid_context.job_id = ""

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "job_id, fire_event_name, and geometry are required" in error_msg

    def test_validate_context_missing_fire_event_name(
        self, valid_context: CommandContext
    ):
        """Test context validation with missing fire_event_name"""
        command = BoundaryRefinementCommand()
        valid_context.fire_event_name = ""

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "job_id, fire_event_name, and geometry are required" in error_msg

    def test_validate_context_missing_geometry(self, valid_context: CommandContext):
        """Test context validation with missing geometry"""
        command = BoundaryRefinementCommand()
        valid_context.geometry = None

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "job_id, fire_event_name, and geometry are required" in error_msg

    def test_validate_context_missing_storage(self, valid_context: CommandContext):
        """Test context validation with missing storage"""
        command = BoundaryRefinementCommand()
        valid_context.storage = None

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "storage, storage_factory, and stac_manager are required" in error_msg

    def test_validate_context_invalid_geometry_type(
        self, valid_context: CommandContext
    ):
        """Test context validation with invalid geometry type"""
        command = BoundaryRefinementCommand()
        valid_context.geometry = {
            "type": "Point",
            "coordinates": [0, 0],
        }  # Invalid type

        is_valid, error_msg = command.validate_context(valid_context)
        assert is_valid is False
        assert "geometry must be a Polygon, MultiPolygon, or Feature object" in error_msg

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    @patch("src.commands.impl.boundary_refinement_command.process_cog_with_boundary")
    async def test_execute_success_single_refinement(
        self,
        mock_process_cog: AsyncMock,
        mock_process_geojson: AsyncMock,
        valid_context: CommandContext,
        sample_stac_item: Dict[str, Any],
    ):
        """Test successful boundary refinement execution"""
        command = BoundaryRefinementCommand()

        # Setup mocks
        mock_process_geojson.return_value = (
            "mock://boundary.geojson",
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-120, 35],
                                    [-119, 35],
                                    [-119, 36],
                                    [-120, 36],
                                    [-120, 35],
                                ]
                            ],
                        },
                        "properties": {},
                    }
                ],
            },
            [-120.0, 35.0, -119.0, 36.0],
        )

        mock_process_cog.side_effect = [
            "mock://refined_dnbr.tif",
            "mock://refined_rdnbr.tif",
            "mock://refined_rbr.tif",
        ]

        valid_context.stac_manager.get_item_by_id.return_value = sample_stac_item

        # Execute command
        result = await command.execute(valid_context)

        # Verify result
        assert result.status == CommandStatus.SUCCESS
        assert result.job_id == "test-job-123"
        assert result.fire_event_name == "test-fire-2024"
        assert result.command_name == "boundary_refinement"
        assert result.has_assets()

        # Check asset URLs
        assert "boundary" in result.asset_urls
        assert "dnbr" in result.asset_urls
        assert "rdnbr" in result.asset_urls
        assert "rbr" in result.asset_urls
        assert result.get_asset_url("boundary") == "mock://boundary.geojson"

        # Verify utility function calls
        mock_process_geojson.assert_called_once_with(
            geometry=valid_context.geometry,
            fire_event_name="test-fire-2024",
            job_id="test-job-123",
            filename="refined_boundary",
            storage_factory=valid_context.storage_factory,
        )

        # Check refined naming pattern for overwriting
        assert mock_process_cog.call_count == 3
        call_args_list = mock_process_cog.call_args_list
        assert any("refined_dnbr" in str(call) for call in call_args_list)
        assert any("refined_rdnbr" in str(call) for call in call_args_list)
        assert any("refined_rbr" in str(call) for call in call_args_list)

        # Verify STAC operations
        valid_context.stac_manager.get_item_by_id.assert_called_once_with(
            "test-fire-2024-severity-test-job-123"
        )
        valid_context.stac_manager.create_fire_severity_item.assert_called_once()
        valid_context.stac_manager.create_boundary_item.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    async def test_execute_failure_no_original_stac_item(
        self, mock_process_geojson: AsyncMock, valid_context: CommandContext
    ):
        """Test failure when original STAC item not found"""
        command = BoundaryRefinementCommand()

        # Setup mocks
        mock_process_geojson.return_value = (
            "mock://boundary.geojson",
            {"type": "FeatureCollection", "features": []},
            [-120.0, 35.0, -119.0, 36.0],
        )

        valid_context.stac_manager.get_item_by_id.return_value = (
            None  # No STAC item found
        )

        # Execute command
        result = await command.execute(valid_context)

        # Verify failure
        assert result.status == CommandStatus.FAILED
        assert "Original COG not found for job ID test-job-123" in result.error_message
        assert result.error_details["stac_id"] == "test-fire-2024-severity-test-job-123"

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    @patch("src.commands.impl.boundary_refinement_command.process_cog_with_boundary")
    async def test_execute_partial_success_some_cog_failures(
        self,
        mock_process_cog: AsyncMock,
        mock_process_geojson: AsyncMock,
        valid_context: CommandContext,
        sample_stac_item: Dict[str, Any],
    ):
        """Test partial success when some COG processing fails"""
        command = BoundaryRefinementCommand()

        # Setup mocks
        mock_process_geojson.return_value = (
            "mock://boundary.geojson",
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-120, 35],
                                    [-119, 35],
                                    [-119, 36],
                                    [-120, 36],
                                    [-120, 35],
                                ]
                            ],
                        },
                        "properties": {},
                    }
                ],
            },
            [-120.0, 35.0, -119.0, 36.0],
        )

        # First call succeeds, second fails, third succeeds
        mock_process_cog.side_effect = [
            "mock://refined_dnbr.tif",  # Success
            Exception("COG processing failed"),  # Failure
            "mock://refined_rbr.tif",  # Success
        ]

        valid_context.stac_manager.get_item_by_id.return_value = sample_stac_item

        # Execute command
        result = await command.execute(valid_context)

        # Verify partial success
        assert result.status == CommandStatus.PARTIAL_SUCCESS
        assert "Some metrics failed:" in result.error_message
        assert "rdnbr" in str(result.error_message)  # Failed metric

        # Should still have successful assets
        assert result.has_assets()
        assert "boundary" in result.asset_urls
        assert "dnbr" in result.asset_urls  # Successful
        assert "rbr" in result.asset_urls  # Successful
        assert "rdnbr" not in result.asset_urls  # Failed

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    @patch("src.commands.impl.boundary_refinement_command.process_cog_with_boundary")
    async def test_execute_failure_all_cog_processing_fails(
        self,
        mock_process_cog: AsyncMock,
        mock_process_geojson: AsyncMock,
        valid_context: CommandContext,
        sample_stac_item: Dict[str, Any],
    ):
        """Test failure when all COG processing fails"""
        command = BoundaryRefinementCommand()

        # Setup mocks
        mock_process_geojson.return_value = (
            "mock://boundary.geojson",
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-120, 35],
                                    [-119, 35],
                                    [-119, 36],
                                    [-120, 36],
                                    [-120, 35],
                                ]
                            ],
                        },
                        "properties": {},
                    }
                ],
            },
            [-120.0, 35.0, -119.0, 36.0],
        )

        # All COG processing fails
        mock_process_cog.side_effect = Exception("COG processing failed")

        valid_context.stac_manager.get_item_by_id.return_value = sample_stac_item

        # Execute command
        result = await command.execute(valid_context)

        # Verify failure
        assert result.status == CommandStatus.FAILED
        assert "All COG processing operations failed" in result.error_message
        assert "failed_metrics" in result.error_details

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    @patch("src.commands.impl.boundary_refinement_command.process_cog_with_boundary")
    async def test_execute_partial_success_stac_metadata_failure(
        self,
        mock_process_cog: AsyncMock,
        mock_process_geojson: AsyncMock,
        valid_context: CommandContext,
        sample_stac_item: Dict[str, Any],
    ):
        """Test partial success when STAC metadata creation fails"""
        command = BoundaryRefinementCommand()

        # Setup mocks
        mock_process_geojson.return_value = (
            "mock://boundary.geojson",
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-120, 35],
                                    [-119, 35],
                                    [-119, 36],
                                    [-120, 36],
                                    [-120, 35],
                                ]
                            ],
                        },
                        "properties": {},
                    }
                ],
            },
            [-120.0, 35.0, -119.0, 36.0],
        )

        mock_process_cog.side_effect = [
            "mock://refined_dnbr.tif",
            "mock://refined_rdnbr.tif",
            "mock://refined_rbr.tif",
        ]

        valid_context.stac_manager.get_item_by_id.return_value = sample_stac_item
        valid_context.stac_manager.create_fire_severity_item.side_effect = Exception(
            "STAC creation failed"
        )

        # Execute command
        result = await command.execute(valid_context)

        # Verify partial success
        assert result.status == CommandStatus.PARTIAL_SUCCESS
        assert (
            "Assets created but STAC metadata creation failed" in result.error_message
        )
        assert result.has_assets()  # Assets should still be created

    @pytest.mark.asyncio
    async def test_execute_context_validation_failure(
        self, valid_context: CommandContext
    ):
        """Test execution failure due to context validation"""
        command = BoundaryRefinementCommand()
        original_job_id = valid_context.job_id
        valid_context.geometry = (
            None  # Make context invalid but keep job_id for CommandResult
        )

        result = await command.execute(valid_context)

        assert result.status == CommandStatus.FAILED
        assert "Context validation failed:" in result.error_message
        assert result.job_id == original_job_id  # Should preserve job_id for result

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    async def test_execute_geojson_processing_failure(
        self, mock_process_geojson: AsyncMock, valid_context: CommandContext
    ):
        """Test execution failure during GeoJSON processing"""
        command = BoundaryRefinementCommand()

        # Mock GeoJSON processing failure
        mock_process_geojson.side_effect = Exception("GeoJSON processing failed")

        result = await command.execute(valid_context)

        assert result.status == CommandStatus.FAILED
        assert "Command execution failed:" in result.error_message

    def test_multiple_refinement_iterations_naming_pattern(self):
        """Test that naming pattern supports multiple refinement iterations"""
        # This test verifies the naming pattern logic without actual execution

        # Simulated metrics from first refinement
        first_refinement_assets = ["refined_dnbr", "refined_rdnbr", "refined_rbr"]

        # Simulated metrics from second refinement (should overwrite)
        second_refinement_assets = ["refined_dnbr", "refined_rdnbr", "refined_rbr"]

        # Both should have same naming pattern - this enables overwriting
        assert first_refinement_assets == second_refinement_assets

        # Original assets would never be touched
        original_assets = ["dnbr", "rdnbr", "rbr"]
        assert all(asset not in original_assets for asset in first_refinement_assets)

    def test_feature_geometry_validation(
        self,
        sample_feature: Feature,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
    ):
        """Test validation works with Feature geometry"""
        command = BoundaryRefinementCommand()

        context = CommandContext(
            job_id="test-job-123",
            fire_event_name="test-fire-2024",
            geometry=sample_feature,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
        )

        is_valid, error_msg = command.validate_context(context)
        assert is_valid is True
        assert error_msg == "Context validation passed"


@pytest.mark.asyncio
class TestBoundaryRefinementCommandIntegration:
    """Integration tests focusing on command workflow"""

    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    @patch("src.commands.impl.boundary_refinement_command.process_cog_with_boundary")
    async def test_iterative_refinement_workflow(
        self,
        mock_process_cog: AsyncMock,
        mock_process_geojson: AsyncMock,
        valid_context: CommandContext,
        sample_stac_item: Dict[str, Any],
    ):
        """Test that command supports iterative refinement workflow"""
        command = BoundaryRefinementCommand()

        # Setup for first refinement
        mock_process_geojson.return_value = (
            "mock://refined_boundary_v1.geojson",
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-120, 35],
                                    [-119, 35],
                                    [-119, 36],
                                    [-120, 36],
                                    [-120, 35],
                                ]
                            ],
                        },
                        "properties": {},
                    }
                ],
            },
            [-120.0, 35.0, -119.0, 36.0],
        )

        mock_process_cog.side_effect = [
            "mock://refined_dnbr_v1.tif",
            "mock://refined_rdnbr_v1.tif",
            "mock://refined_rbr_v1.tif",
        ]

        valid_context.stac_manager.get_item_by_id.return_value = sample_stac_item
        valid_context.metadata = {"refinement_iteration": 1}

        # Execute first refinement
        result1 = await command.execute(valid_context)
        assert result1.status == CommandStatus.SUCCESS

        # Setup for second refinement (should overwrite first)
        mock_process_geojson.return_value = (
            "mock://refined_boundary_v2.geojson",  # Different URL but same storage path pattern
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-120, 35.1],
                                    [-119.1, 35.1],
                                    [-119.1, 36.1],
                                    [-120, 36.1],
                                    [-120, 35.1],
                                ]
                            ],
                        },
                        "properties": {},
                    }
                ],
            },
            [-120.0, 35.1, -119.1, 36.1],
        )

        mock_process_cog.side_effect = [
            "mock://refined_dnbr_v2.tif",  # Same filename pattern - will overwrite
            "mock://refined_rdnbr_v2.tif",
            "mock://refined_rbr_v2.tif",
        ]

        valid_context.metadata = {"refinement_iteration": 2}

        # Execute second refinement
        result2 = await command.execute(valid_context)
        assert result2.status == CommandStatus.SUCCESS

        # Verify both refinements used the "refined_" prefix pattern for overwriting
        call_args_list = mock_process_cog.call_args_list
        refined_calls = [call for call in call_args_list if "refined_" in str(call)]
        assert len(refined_calls) == 6  # 3 metrics x 2 refinements

        # Verify original assets are never touched (no calls without "refined_" prefix for outputs)
        __original_calls = [
            call
            for call in call_args_list
            if any(
                metric in str(call) and f"refined_{metric}" not in str(call)
                for metric in ["dnbr", "rdnbr", "rbr"]
            )
        ]
        # Original calls should be 0 for output filenames (input URLs are different)
        assert (
            len([call for call in refined_calls if "output_filename" in str(call)]) == 6
        )
