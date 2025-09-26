import pytest
from typing import Dict, Any

from src.stac.stac_json_repository import STACJSONRepository
from src.core.storage.interface import StorageInterface


class TestSTACJSONRepository:
    """Test suite for STACJSONRepository"""

    @pytest.fixture
    def sample_stac_item(self) -> Dict[str, Any]:
        """Sample STAC item for testing"""
        return {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "test_fire-severity-job_123",
            "properties": {
                "datetime": "2023-08-15T12:00:00Z",
                "fire_event_name": "test_fire",
                "job_id": "job_123",
                "product_type": "fire_severity",
                "boundary_type": "coarse",
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-120.5, 35.5],
                        [-120.0, 35.5],
                        [-120.0, 36.0],
                        [-120.5, 36.0],
                        [-120.5, 35.5],
                    ]
                ],
            },
            "bbox": [-120.5, 35.5, -120.0, 36.0],
            "assets": {
                "rbr": {
                    "href": "https://storage.example.com/rbr.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "title": "Relativized Burn Ratio (RBR)",
                    "roles": ["data"],
                }
            },
            "links": [],
        }

    @pytest.mark.asyncio
    async def test_add_item_to_empty_repository(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test adding items to an empty repository"""
        repository = STACJSONRepository(memory_storage)

        # Add item
        result_path = await repository.add_item(sample_stac_item)
        assert result_path is not None

        # Verify item was added and can be retrieved
        item = await repository.get_item_by_id(sample_stac_item["id"])
        assert item is not None
        assert item["id"] == sample_stac_item["id"]

    @pytest.mark.asyncio
    async def test_add_multiple_items(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test adding multiple items"""
        repository = STACJSONRepository(memory_storage)

        # Create second item
        second_item = sample_stac_item.copy()
        second_item["id"] = "test_fire-boundary-job_123"
        second_item["properties"] = second_item["properties"].copy()
        second_item["properties"]["product_type"] = "fire_boundary"

        # Add both items
        await repository.add_item(sample_stac_item)
        await repository.add_item(second_item)

        # Both items should be retrievable by fire event
        items = await repository.get_items_by_fire_event("test_fire")
        assert len(items) == 2

        item_ids = [item["id"] for item in items]
        assert sample_stac_item["id"] in item_ids
        assert second_item["id"] in item_ids

    @pytest.mark.asyncio
    async def test_get_items_by_fire_event(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test retrieving items by fire event name"""
        repository = STACJSONRepository(memory_storage)

        # Create items for different fire events
        fire1_item = sample_stac_item.copy()
        fire1_item["properties"] = fire1_item["properties"].copy()
        fire1_item["properties"]["fire_event_name"] = "fire_one"

        fire2_item = sample_stac_item.copy()
        fire2_item["id"] = "fire_two-severity-job_123"
        fire2_item["properties"] = fire2_item["properties"].copy()
        fire2_item["properties"]["fire_event_name"] = "fire_two"

        await repository.add_item(fire1_item)
        await repository.add_item(fire2_item)

        # Test retrieval by fire event
        fire1_items = await repository.get_items_by_fire_event("fire_one")
        assert len(fire1_items) == 1
        assert fire1_items[0]["properties"]["fire_event_name"] == "fire_one"

        fire2_items = await repository.get_items_by_fire_event("fire_two")
        assert len(fire2_items) == 1
        assert fire2_items[0]["properties"]["fire_event_name"] == "fire_two"

        # Non-existent fire event should return empty list
        empty_items = await repository.get_items_by_fire_event("non_existent")
        assert empty_items == []

    @pytest.mark.asyncio
    async def test_get_item_by_id(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test retrieving a specific item by ID"""
        repository = STACJSONRepository(memory_storage)

        await repository.add_item(sample_stac_item)

        # Test retrieval by ID
        item = await repository.get_item_by_id(sample_stac_item["id"])
        assert item is not None
        assert item["id"] == sample_stac_item["id"]

        # Non-existent ID should return None
        non_existent = await repository.get_item_by_id("non-existent-id")
        assert non_existent is None

    @pytest.mark.asyncio
    async def test_get_item_by_fire_event_and_id(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test retrieving a specific item by fire event and ID (more efficient method)"""
        repository = STACJSONRepository(memory_storage)

        await repository.add_item(sample_stac_item)

        # Test retrieval by fire event and ID
        item = await repository.get_item_by_fire_event_and_id(
            "test_fire", sample_stac_item["id"]
        )
        assert item is not None
        assert item["id"] == sample_stac_item["id"]

        # Wrong fire event should return None
        wrong_fire = await repository.get_item_by_fire_event_and_id(
            "wrong_fire", sample_stac_item["id"]
        )
        assert wrong_fire is None

        # Non-existent ID should return None
        non_existent = await repository.get_item_by_fire_event_and_id(
            "test_fire", "non-existent-id"
        )
        assert non_existent is None

    @pytest.mark.asyncio
    async def test_get_items_by_id_and_coarseness(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test retrieving items by ID and boundary type"""
        repository = STACJSONRepository(memory_storage)

        # Create items with different boundary types and different job_ids to ensure unique paths
        coarse_item = sample_stac_item.copy()
        coarse_item["id"] = "test_fire-severity-coarse"
        coarse_item["properties"] = coarse_item["properties"].copy()
        coarse_item["properties"]["boundary_type"] = "coarse"
        coarse_item["properties"]["job_id"] = "job_coarse"

        refined_item = sample_stac_item.copy()
        refined_item["id"] = "test_fire-severity-refined"
        refined_item["properties"] = refined_item["properties"].copy()
        refined_item["properties"]["boundary_type"] = "refined"
        refined_item["properties"]["job_id"] = "job_refined"

        await repository.add_item(coarse_item)
        await repository.add_item(refined_item)

        # Test retrieval by ID and boundary type
        coarse_result = await repository.get_items_by_id_and_coarseness(
            "test_fire-severity-coarse", "coarse"
        )
        assert coarse_result is not None
        assert coarse_result["properties"]["boundary_type"] == "coarse"

        refined_result = await repository.get_items_by_id_and_coarseness(
            "test_fire-severity-refined", "refined"
        )
        assert refined_result is not None
        assert refined_result["properties"]["boundary_type"] == "refined"

        # Wrong combination should return None
        wrong_combo = await repository.get_items_by_id_and_coarseness(
            "test_fire-severity-coarse", "refined"
        )
        assert wrong_combo is None

    @pytest.mark.asyncio
    async def test_get_items_by_id_and_classification_breaks(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test retrieving items by ID and classification breaks"""
        repository = STACJSONRepository(memory_storage)

        # Create item with classification breaks
        veg_item = sample_stac_item.copy()
        veg_item["id"] = "test_fire-veg-matrix-job_123"
        veg_item["properties"] = veg_item["properties"].copy()
        veg_item["properties"]["product_type"] = "vegetation_fire_matrix"
        veg_item["properties"]["classification_breaks"] = [0.1, 0.27, 0.44, 0.66]

        await repository.add_item(veg_item)

        # Test retrieval with matching classification breaks
        result = await repository.get_items_by_id_and_classification_breaks(
            "test_fire-veg-matrix-job_123", [0.1, 0.27, 0.44, 0.66]
        )
        assert result is not None
        assert result["id"] == "test_fire-veg-matrix-job_123"

        # Test retrieval with None classification breaks (should match by ID only)
        result_none = await repository.get_items_by_id_and_classification_breaks(
            "test_fire-veg-matrix-job_123", None
        )
        assert result_none is not None
        assert result_none["id"] == "test_fire-veg-matrix-job_123"

        # Test with wrong classification breaks
        wrong_result = await repository.get_items_by_id_and_classification_breaks(
            "test_fire-veg-matrix-job_123", [0.2, 0.4, 0.6, 0.8]
        )
        assert wrong_result is None

    @pytest.mark.asyncio
    async def test_search_items_by_product_type(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test searching items by product type"""
        repository = STACJSONRepository(memory_storage)

        # Create items with different product types
        severity_item = sample_stac_item.copy()
        severity_item["properties"]["product_type"] = "fire_severity"

        boundary_item = sample_stac_item.copy()
        boundary_item["id"] = "test_fire-boundary-job_123"
        boundary_item["properties"] = boundary_item["properties"].copy()
        boundary_item["properties"]["product_type"] = "fire_boundary"

        await repository.add_item(severity_item)
        await repository.add_item(boundary_item)

        # Search by product type
        severity_results = await repository.search_items(
            fire_event_name="test_fire", product_type="fire_severity"
        )
        assert len(severity_results) == 1
        assert severity_results[0]["properties"]["product_type"] == "fire_severity"

        boundary_results = await repository.search_items(
            fire_event_name="test_fire", product_type="fire_boundary"
        )
        assert len(boundary_results) == 1
        assert boundary_results[0]["properties"]["product_type"] == "fire_boundary"

        # Search without product type filter should return all
        all_results = await repository.search_items(fire_event_name="test_fire")
        assert len(all_results) == 2

    @pytest.mark.asyncio
    async def test_search_items_with_bbox(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test searching items with bounding box filter (placeholder test)"""
        repository = STACJSONRepository(memory_storage)

        await repository.add_item(sample_stac_item)

        # Note: bbox filtering not implemented in repository yet
        # This test verifies it doesn't crash when bbox parameter is passed
        intersecting_bbox = [-121.0, 35.0, -120.0, 36.5]
        results = await repository.search_items(
            fire_event_name="test_fire", bbox=intersecting_bbox
        )
        # Should return all items since bbox filtering is not implemented
        assert len(results) >= 0

    @pytest.mark.asyncio
    async def test_search_items_with_datetime_range(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test searching items with datetime range filter (placeholder test)"""
        repository = STACJSONRepository(memory_storage)

        await repository.add_item(sample_stac_item)

        # Note: datetime filtering not implemented in repository yet
        # This test verifies it doesn't crash when datetime_range parameter is passed
        datetime_range = ["2023-08-01T00:00:00Z", "2023-08-31T23:59:59Z"]
        results = await repository.search_items(
            fire_event_name="test_fire", datetime_range=datetime_range
        )
        # Should return all items since datetime filtering is not implemented
        assert len(results) >= 0

        # Test with partial range (start only)
        start_only_range = ["2023-08-01T00:00:00Z", ""]
        results_start = await repository.search_items(
            fire_event_name="test_fire", datetime_range=start_only_range
        )
        assert len(results_start) >= 0

        # Test with partial range (end only)
        end_only_range = ["", "2023-08-31T23:59:59Z"]
        results_end = await repository.search_items(
            fire_event_name="test_fire", datetime_range=end_only_range
        )
        assert len(results_end) >= 0

    @pytest.mark.asyncio
    async def test_empty_repository_handling(
        self, memory_storage: StorageInterface
    ) -> None:
        """Test handling when no items exist yet"""
        repository = STACJSONRepository(memory_storage)

        # All queries should return empty results without error
        items = await repository.get_items_by_fire_event("non_existent")
        assert items == []

        item = await repository.get_item_by_id("non-existent-id")
        assert item is None

        item_by_fire = await repository.get_item_by_fire_event_and_id(
            "non_existent", "non-existent-id"
        )
        assert item_by_fire is None

        search_results = await repository.search_items("non_existent")
        assert search_results == []

    @pytest.mark.asyncio
    async def test_generate_item_path(
        self, memory_storage: StorageInterface, sample_stac_item: Dict[str, Any]
    ) -> None:
        """Test the internal path generation logic"""
        repository = STACJSONRepository(memory_storage)

        # Test path generation
        path = repository._generate_item_path(
            sample_stac_item["properties"], sample_stac_item["id"]
        )

        expected_path = "stac/test_fire/fire_severity-coarse-job_123.json"
        assert path == expected_path

        # Test with missing properties
        minimal_properties = {"fire_event_name": "test_fire"}
        path_minimal = repository._generate_item_path(minimal_properties, "test_id")
        expected_minimal = "stac/test_fire/unknown-coarse-unknown.json"
        assert path_minimal == expected_minimal

    @pytest.mark.asyncio
    async def test_extract_search_terms_from_path(
        self, memory_storage: StorageInterface
    ) -> None:
        """Test the internal path parsing logic"""
        repository = STACJSONRepository(memory_storage)

        # Test normal path
        path = "stac/test_fire/fire_severity-coarse-job_123.json"
        terms = repository._extract_search_terms_from_path(path)

        expected_terms = {
            "fire_event_name": "test_fire",
            "product_type": "fire_severity",
            "boundary_type": "coarse",
        }
        assert terms == expected_terms

        # Test invalid path
        invalid_path = "invalid/path/structure.json"
        invalid_terms = repository._extract_search_terms_from_path(invalid_path)
        assert invalid_terms == {}

        # Test path without product separator
        no_separator_path = "stac/test_fire/no_separator.json"
        no_separator_terms = repository._extract_search_terms_from_path(
            no_separator_path
        )
        assert no_separator_terms == {}
