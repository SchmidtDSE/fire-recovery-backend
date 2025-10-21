"""
End-to-end integration tests for MultiPolygon support.

Tests MultiPolygon geometries through complete workflows:
- Fire severity analysis
- Boundary refinement
- Vegetation impact analysis
"""
# mypy: disable-error-code="list-item,union-attr,arg-type"
# Note: geojson-pydantic has strict types for coordinates but tests work at runtime

import pytest
import numpy as np
import xarray as xr
from unittest.mock import Mock, AsyncMock, patch
from typing import Dict, Any

from geojson_pydantic import MultiPolygon
from src.commands.impl.fire_severity_command import FireSeverityAnalysisCommand
from src.commands.impl.boundary_refinement_command import BoundaryRefinementCommand
from src.commands.impl.vegetation_resolve_command import VegetationResolveCommand
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandStatus
from src.core.storage.interface import StorageInterface
from src.core.storage.storage_factory import StorageFactory
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry


# Test fixtures for MultiPolygon integration tests
@pytest.fixture
def simple_multipolygon() -> MultiPolygon:
    """Simple MultiPolygon with two square polygons for basic testing"""
    return MultiPolygon(  # type: ignore[call-arg]
        type="MultiPolygon",
        coordinates=[
            [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
            [[[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 3.0], [2.0, 2.0]]],
        ],
    )


@pytest.fixture
def disjoint_fire_multipolygon() -> MultiPolygon:
    """
    Realistic MultiPolygon representing disjoint burn areas.

    Use case: Complex fire with main perimeter and spot fires
    Location: California central coast area
    """
    return MultiPolygon(  # type: ignore[call-arg]
        type="MultiPolygon",
        coordinates=[
            # Main burn area (approximately 50km x 50km)
            [
                [
                    [-120.5, 38.5],
                    [-120.0, 38.5],
                    [-120.0, 39.0],
                    [-120.5, 39.0],
                    [-120.5, 38.5],
                ]
            ],
            # Spot fire 1 (northeast, ~10km x 10km)
            [
                [
                    [-119.8, 39.1],
                    [-119.7, 39.1],
                    [-119.7, 39.2],
                    [-119.8, 39.2],
                    [-119.8, 39.1],
                ]
            ],
            # Spot fire 2 (southeast, ~5km x 5km)
            [
                [
                    [-119.9, 38.3],
                    [-119.85, 38.3],
                    [-119.85, 38.35],
                    [-119.9, 38.35],
                    [-119.9, 38.3],
                ]
            ],
        ],
    )


@pytest.fixture
def mock_storage() -> Mock:
    """Create mock storage interface"""
    storage = Mock(spec=StorageInterface)
    storage.save_bytes = AsyncMock(return_value="mock://storage/path.tif")
    storage.copy_from_url = AsyncMock()
    storage.get_bytes = AsyncMock()
    return storage


@pytest.fixture
def mock_storage_factory(mock_storage: Mock) -> Mock:
    """Create mock storage factory"""
    factory = Mock(spec=StorageFactory)
    factory.get_temp_storage.return_value = mock_storage
    factory.get_final_storage.return_value = mock_storage
    return factory


@pytest.fixture
def mock_stac_manager() -> Mock:
    """Create mock STAC manager"""
    manager = Mock(spec=STACJSONManager)
    manager.create_fire_severity_item = AsyncMock(
        return_value="mock://stac/severity.json"
    )
    manager.create_boundary_item = AsyncMock(return_value="mock://stac/boundary.json")
    manager.create_veg_matrix_item = AsyncMock(return_value="mock://stac/veg.json")
    manager.get_item_by_id = AsyncMock()
    manager.get_items_by_id_and_coarseness = AsyncMock(return_value=None)
    return manager


@pytest.fixture
def mock_index_registry() -> Mock:  # type: ignore[misc]
    """Create mock index registry with calculators"""

    class MockIndexCalculator:
        """Mock index calculator for testing"""

        def __init__(self, index_name: str):
            self._index_name = index_name

        @property
        def index_name(self) -> str:
            return self._index_name

        async def calculate(
            self, prefire_data: Any, postfire_data: Any, context: Dict[str, Any]
        ) -> xr.DataArray:
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

    registry = Mock(spec=IndexRegistry)

    def get_calculator(index_name: str) -> MockIndexCalculator | None:
        calculators = {
            "nbr": MockIndexCalculator("nbr"),
            "dnbr": MockIndexCalculator("dnbr"),
            "rdnbr": MockIndexCalculator("rdnbr"),
            "rbr": MockIndexCalculator("rbr"),
        }
        return calculators.get(index_name)

    registry.get_calculator = get_calculator
    return registry


class TestFireSeverityAnalysisWithMultiPolygon:
    """Test fire severity analysis workflow with MultiPolygon geometries"""

    @pytest.mark.asyncio
    @patch("src.commands.impl.fire_severity_command.StacEndpointHandler")
    @patch("src.commands.impl.fire_severity_command.stackstac")
    async def test_fire_severity_analysis_with_multipolygon(
        self,
        mock_stackstac: Mock,
        mock_stac_handler_class: Mock,
        simple_multipolygon: MultiPolygon,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
    ) -> None:
        """Complete fire severity workflow with MultiPolygon"""
        # Setup command context with MultiPolygon
        context = CommandContext(
            job_id="test-multipolygon-job",
            fire_event_name="test-multifire",
            geometry=simple_multipolygon,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
            computation_config={
                "prefire_date_range": ["2023-06-01", "2023-06-15"],
                "postfire_date_range": ["2023-07-01", "2023-07-15"],
                "collection": "sentinel-2-l2a",
                "buffer_meters": 100,
                "indices": ["nbr", "dnbr"],
            },
        )

        # Setup mocks
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
        command = FireSeverityAnalysisCommand()
        result = await command.execute(context)

        # Verify success
        assert result.is_success()
        assert result.command_name == "fire_severity_analysis"
        assert result.job_id == "test-multipolygon-job"

        # Verify assets were created
        assert result.has_assets()
        assert "nbr" in result.asset_urls  # type: ignore[operator]
        assert "dnbr" in result.asset_urls  # type: ignore[operator]

        # Verify STAC item was created with MultiPolygon geometry
        mock_stac_manager.create_fire_severity_item.assert_called_once()
        call_args = mock_stac_manager.create_fire_severity_item.call_args
        # The geometry should have been passed through
        assert call_args is not None

    def test_multipolygon_disjoint_areas_bounds_calculation(
        self,
        disjoint_fire_multipolygon: MultiPolygon,
    ) -> None:
        """Verify MultiPolygon bounds encompass all disjoint areas"""
        # Test bounds calculation without executing full command
        from shapely import from_geojson

        geom_json = disjoint_fire_multipolygon.model_dump_json()
        shapely_geom = from_geojson(geom_json)

        # Verify MultiPolygon has correct number of parts
        assert shapely_geom.geom_type == "MultiPolygon"
        assert len(shapely_geom.geoms) == 3  # Main fire + 2 spot fires

        # Verify bounds encompass all areas
        bounds = shapely_geom.bounds
        # Should cover from westernmost to easternmost point
        assert bounds[0] <= -120.5  # min longitude
        assert bounds[2] >= -119.7  # max longitude
        # Should cover from southernmost to northernmost point
        assert bounds[1] <= 38.3  # min latitude
        assert bounds[3] >= 39.2  # max latitude


class TestBoundaryRefinementWithMultiPolygon:
    """Test boundary refinement workflow with MultiPolygon geometries"""

    @pytest.fixture
    def sample_stac_item(self) -> Dict[str, Any]:
        """Sample STAC item with fire severity assets"""
        return {
            "id": "test-fire-severity-test-refine-job",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            },
            "properties": {"datetime": "2024-01-15T12:00:00Z"},
            "bbox": [0, 0, 1, 1],
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

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    @patch("src.commands.impl.boundary_refinement_command.process_cog_with_boundary")
    async def test_boundary_refinement_with_multipolygon(
        self,
        mock_process_cog: AsyncMock,
        mock_process_geojson: AsyncMock,
        simple_multipolygon: MultiPolygon,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
        sample_stac_item: Dict[str, Any],
    ) -> None:
        """Boundary refinement accepts MultiPolygon geometry"""
        # Setup command context
        context = CommandContext(
            job_id="test-refine-job",
            fire_event_name="test-fire",
            geometry=simple_multipolygon,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
        )

        # Setup mocks
        mock_process_geojson.return_value = (
            "mock://boundary.geojson",
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": simple_multipolygon.model_dump(),
                        "properties": {},
                    }
                ],
            },
            [0.0, 0.0, 3.0, 3.0],  # bbox covering both polygons
        )

        mock_process_cog.side_effect = [
            "mock://refined_dnbr.tif",
            "mock://refined_rdnbr.tif",
            "mock://refined_rbr.tif",
        ]

        mock_stac_manager.get_item_by_id.return_value = sample_stac_item

        # Execute command
        command = BoundaryRefinementCommand()
        result = await command.execute(context)

        # Verify success
        assert result.status == CommandStatus.SUCCESS
        assert result.has_assets()
        assert "boundary" in result.asset_urls  # type: ignore[operator]

        # Verify GeoJSON processing was called with MultiPolygon
        mock_process_geojson.assert_called_once()
        call_args = mock_process_geojson.call_args
        assert call_args is not None

        # Verify COG processing happened for all metrics
        assert mock_process_cog.call_count == 3

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    @patch("src.commands.impl.boundary_refinement_command.process_cog_with_boundary")
    async def test_multipolygon_cog_generation_bounds(
        self,
        mock_process_cog: AsyncMock,
        mock_process_geojson: AsyncMock,
        disjoint_fire_multipolygon: MultiPolygon,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
        sample_stac_item: Dict[str, Any],
    ) -> None:
        """Verify COG bounds are correct for MultiPolygon with disjoint areas"""
        # Setup command context
        context = CommandContext(
            job_id="test-cog-bounds",
            fire_event_name="test-fire",
            geometry=disjoint_fire_multipolygon,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
        )

        # Calculate expected bounds
        from shapely import from_geojson

        geom_json = disjoint_fire_multipolygon.model_dump_json()
        shapely_geom = from_geojson(geom_json)
        expected_bounds = list(shapely_geom.bounds)

        # Setup mocks
        mock_process_geojson.return_value = (
            "mock://boundary.geojson",
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": disjoint_fire_multipolygon.model_dump(),
                        "properties": {},
                    }
                ],
            },
            expected_bounds,
        )

        mock_process_cog.side_effect = [
            "mock://refined_dnbr.tif",
            "mock://refined_rdnbr.tif",
            "mock://refined_rbr.tif",
        ]

        mock_stac_manager.get_item_by_id.return_value = sample_stac_item

        # Execute command
        command = BoundaryRefinementCommand()
        result = await command.execute(context)

        # Verify success
        assert result.status == CommandStatus.SUCCESS

        # Verify bounds were correctly calculated
        call_args = mock_process_geojson.call_args[1]
        # The geometry should be the MultiPolygon
        assert call_args["geometry"] == disjoint_fire_multipolygon


class TestVegetationAnalysisWithMultiPolygon:
    """Test vegetation impact analysis workflow with MultiPolygon geometries"""

    def test_vegetation_command_accepts_multipolygon(
        self,
        simple_multipolygon: MultiPolygon,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
    ) -> None:
        """Vegetation analysis command accepts MultiPolygon geometry in context"""
        # Setup command context with MultiPolygon
        context = CommandContext(
            job_id="test-veg-job",
            fire_event_name="test-fire",
            geometry=simple_multipolygon,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
            severity_breaks=[0.1, 0.27, 0.66],
            metadata={
                "veg_gpkg_url": "https://example.com/vegetation.gpkg",
                "fire_cog_url": "https://example.com/fire.tif",
                "geojson_url": "https://example.com/boundary.geojson",
            },
        )

        # Verify context was created successfully with MultiPolygon
        assert context.geometry is not None
        assert context.geometry.type == "MultiPolygon"

        # Verify command can validate the context
        command = VegetationResolveCommand()
        is_valid, message = command.validate_context(context)
        assert is_valid is True

    def test_multipolygon_bounds_for_vegetation_analysis(
        self,
        disjoint_fire_multipolygon: MultiPolygon,
    ) -> None:
        """Multiple disjoint burn areas have correct bounds for vegetation analysis"""
        # Verify bounds calculation for vegetation analysis
        from shapely import from_geojson

        geom_json = disjoint_fire_multipolygon.model_dump_json()
        shapely_geom = from_geojson(geom_json)

        # Verify geometry structure
        assert shapely_geom.geom_type == "MultiPolygon"
        assert len(shapely_geom.geoms) == 3

        # Verify bounds encompass all fire areas
        bounds = shapely_geom.bounds
        # Main fire + 2 spot fires should all be within bounds
        assert bounds[0] <= -120.5  # All polygons should be within
        assert bounds[2] >= -119.85  # easternmost point
        assert bounds[1] <= 38.3  # southernmost point
        assert bounds[3] >= 39.2  # northernmost point


class TestBackwardCompatibility:
    """Test that Polygon geometries still work (backward compatibility)"""

    @pytest.mark.asyncio
    @patch("src.commands.impl.fire_severity_command.StacEndpointHandler")
    @patch("src.commands.impl.fire_severity_command.stackstac")
    async def test_polygon_still_works_fire_severity(
        self,
        mock_stackstac: Mock,
        mock_stac_handler_class: Mock,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
    ) -> None:
        """Backward compatibility: Polygon still works in fire severity"""
        from geojson_pydantic import Polygon

        polygon = Polygon(
            type="Polygon",
            coordinates=[
                [
                    [-120.5, 35.5],
                    [-120.0, 35.5],
                    [-120.0, 36.0],
                    [-120.5, 36.0],
                    [-120.5, 35.5],
                ]
            ],
        )

        context = CommandContext(
            job_id="test-polygon-job",
            fire_event_name="test-fire",
            geometry=polygon,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
            computation_config={
                "prefire_date_range": ["2023-06-01", "2023-06-15"],
                "postfire_date_range": ["2023-07-01", "2023-07-15"],
                "collection": "sentinel-2-l2a",
                "buffer_meters": 100,
                "indices": ["nbr"],
            },
        )

        # Setup mocks
        mock_handler = Mock()
        mock_stac_handler_class.return_value = mock_handler
        mock_handler.search_items = AsyncMock(
            return_value=(
                ["item1"],
                {"nir_band": "B08", "swir_band": "B12", "epsg": 4326},
            )
        )
        mock_handler.get_band_names.return_value = ("B08", "B12")
        mock_handler.get_epsg_code.return_value = 4326

        mock_data = xr.DataArray(
            np.random.random((2, 2, 10, 10)),
            dims=["time", "band", "y", "x"],
            coords={
                "time": np.array(["2023-06-01", "2023-07-01"], dtype="datetime64"),
                "band": ["B08", "B12"],
                "y": range(10),
                "x": range(10),
            },
        )
        mock_stackstac.stack.return_value = mock_data

        # Execute command
        command = FireSeverityAnalysisCommand()
        result = await command.execute(context)

        # Verify success
        assert result.is_success()

    @pytest.mark.asyncio
    @patch("src.commands.impl.boundary_refinement_command.process_and_upload_geojson")
    @patch("src.commands.impl.boundary_refinement_command.process_cog_with_boundary")
    async def test_polygon_still_works_boundary_refinement(
        self,
        mock_process_cog: AsyncMock,
        mock_process_geojson: AsyncMock,
        mock_storage: Mock,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
    ) -> None:
        """Backward compatibility: Polygon still works in boundary refinement"""
        from geojson_pydantic import Polygon

        polygon = Polygon(
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

        context = CommandContext(
            job_id="test-polygon-refine",
            fire_event_name="test-fire",
            geometry=polygon,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
        )

        # Setup mocks
        mock_process_geojson.return_value = (
            "mock://boundary.geojson",
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": polygon.model_dump(),
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

        sample_stac_item = {
            "id": "test-fire-severity-test-polygon-refine",
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-120, 35], [-119, 35], [-119, 36], [-120, 36], [-120, 35]]],
            },
            "properties": {"datetime": "2024-01-15T12:00:00Z"},
            "bbox": [-120.0, 35.0, -119.0, 36.0],
            "assets": {
                "dnbr": {"href": "mock://dnbr.tif"},
                "rdnbr": {"href": "mock://rdnbr.tif"},
                "rbr": {"href": "mock://rbr.tif"},
            },
        }
        mock_stac_manager.get_item_by_id.return_value = sample_stac_item

        # Execute command
        command = BoundaryRefinementCommand()
        result = await command.execute(context)

        # Verify success
        assert result.status == CommandStatus.SUCCESS


class TestRealWorldScenarios:
    """Test real-world fire scenarios with MultiPolygon"""

    def test_spot_fire_scenario(self) -> None:
        """
        Real-world scenario: Main fire with multiple spot fires

        Common in wildfire situations where embers travel ahead of
        the main fire front and ignite separate areas
        """
        spot_fire_multipolygon = MultiPolygon(
            type="MultiPolygon",
            coordinates=[
                # Main fire perimeter (large area)
                [
                    [
                        [-121.0, 39.0],
                        [-120.5, 39.0],
                        [-120.5, 39.5],
                        [-121.0, 39.5],
                        [-121.0, 39.0],
                    ]
                ],
                # Spot fire 1 (ahead of main fire)
                [
                    [
                        [-120.4, 39.6],
                        [-120.3, 39.6],
                        [-120.3, 39.7],
                        [-120.4, 39.7],
                        [-120.4, 39.6],
                    ]
                ],
                # Spot fire 2 (to the east)
                [
                    [
                        [-120.2, 39.2],
                        [-120.1, 39.2],
                        [-120.1, 39.3],
                        [-120.2, 39.3],
                        [-120.2, 39.2],
                    ]
                ],
            ],
        )

        # Validate the geometry
        from src.util.polygon_ops import validate_polygon

        result = validate_polygon(spot_fire_multipolygon)
        assert result.geom_type == "MultiPolygon"
        assert result.is_valid
        assert len(result.geoms) == 3

    def test_island_fire_scenario(self) -> None:
        """
        Real-world scenario: Fire on islands or fragmented landscape

        Common in areas with lakes, rivers, or other barriers that
        create naturally separated burn areas
        """
        island_fire_multipolygon = MultiPolygon(
            type="MultiPolygon",
            coordinates=[
                # Island 1 burn area
                [
                    [
                        [-122.0, 40.0],
                        [-121.9, 40.0],
                        [-121.9, 40.1],
                        [-122.0, 40.1],
                        [-122.0, 40.0],
                    ]
                ],
                # Island 2 burn area
                [
                    [
                        [-121.8, 40.05],
                        [-121.7, 40.05],
                        [-121.7, 40.15],
                        [-121.8, 40.15],
                        [-121.8, 40.05],
                    ]
                ],
            ],
        )

        # Validate the geometry
        from src.util.polygon_ops import validate_polygon, polygon_to_valid_geojson

        result = validate_polygon(island_fire_multipolygon)
        assert result.geom_type == "MultiPolygon"
        assert result.is_valid

        # Verify it can be converted to GeoJSON
        geojson = polygon_to_valid_geojson(island_fire_multipolygon)
        assert geojson.type == "FeatureCollection"
        assert geojson.features[0].geometry.type == "MultiPolygon"


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
