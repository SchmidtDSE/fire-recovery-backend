import pytest
from typing import Dict, Any, AsyncGenerator

from src.stac.stac_catalog_manager import STACCatalogManager
from src.core.storage.interface import StorageInterface
from tests.conftest import STAC_TEST_BASE_URL, STAC_TEST_ASSETS


class TestSTACCatalogManager:
    """Test suite for STACCatalogManager"""

    @pytest.fixture
    async def catalog_manager(self, minio_storage: StorageInterface) -> AsyncGenerator[STACCatalogManager, None]:
        """Create a STACCatalogManager instance for testing"""
        manager = STACCatalogManager(
            base_url=STAC_TEST_BASE_URL,
            storage=minio_storage
        )
        yield manager
        # Cleanup after test
        await minio_storage.cleanup(max_age_seconds=0)  # Remove all files

    @pytest.fixture
    def sample_geometry(self) -> Dict[str, Any]:
        """Sample geometry for testing"""
        return {
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
        }

    @pytest.mark.asyncio
    async def test_initialize_catalog(self, catalog_manager: STACCatalogManager) -> None:
        """Test initializing the root catalog and collections"""
        # Initialize catalog
        root_catalog = await catalog_manager.initialize_catalog()
        
        assert root_catalog.id == "fire-recovery-catalog"
        assert "Fire Recovery Analysis" in root_catalog.description
        
        # Check that collections were created
        collections = list(root_catalog.get_collections())
        assert len(collections) == 3
        
        collection_ids = {col.id for col in collections}
        expected_ids = {"fire-severity", "fire-boundaries", "vegetation-matrices"}
        assert collection_ids == expected_ids

    @pytest.mark.asyncio
    async def test_add_fire_severity_item(
        self, catalog_manager: STACCatalogManager, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test adding a fire severity item to the catalog"""
        # Initialize catalog first
        await catalog_manager.initialize_catalog()
        
        # Add fire severity item
        cog_urls = {"rbr": STAC_TEST_ASSETS["rbr_cog"]}
        
        item_dict = await catalog_manager.add_fire_severity_item(
            fire_event_name="test_fire",
            job_id="job_123",
            cog_urls=cog_urls,
            geometry=sample_geometry,
            datetime_str="2023-08-15T12:00:00Z",
        )
        
        # Verify item properties
        assert item_dict["id"] == "test_fire-severity-job_123"
        assert item_dict["properties"]["product_type"] == "fire_severity"
        assert "rbr" in item_dict["assets"]
        
        # Verify item was added to collection
        collection_items = await catalog_manager.get_collection_items("fire-severity")
        assert len(collection_items) == 1
        assert collection_items[0]["id"] == "test_fire-severity-job_123"

    @pytest.mark.asyncio
    async def test_add_boundary_item(self, catalog_manager: STACCatalogManager) -> None:
        """Test adding a boundary item to the catalog"""
        await catalog_manager.initialize_catalog()
        
        bbox = [-120.5, 35.5, -120.0, 36.0]
        
        item_dict = await catalog_manager.add_boundary_item(
            fire_event_name="test_fire",
            job_id="job_123",
            boundary_geojson_url=STAC_TEST_ASSETS["boundary_geojson"],
            bbox=bbox,
            datetime_str="2023-08-15T12:00:00Z",
        )
        
        assert item_dict["id"] == "test_fire-boundary-job_123"
        assert item_dict["properties"]["product_type"] == "fire_boundary"
        
        collection_items = await catalog_manager.get_collection_items("fire-boundaries")
        assert len(collection_items) == 1

    @pytest.mark.asyncio
    async def test_add_veg_matrix_item(
        self, catalog_manager: STACCatalogManager, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test adding a vegetation matrix item to the catalog"""
        await catalog_manager.initialize_catalog()
        
        bbox = [-120.5, 35.5, -120.0, 36.0]
        classification_breaks = [0.1, 0.27, 0.44, 0.66]
        
        item_dict = await catalog_manager.add_veg_matrix_item(
            fire_event_name="test_fire",
            job_id="job_123",
            fire_veg_matrix_csv_url=STAC_TEST_ASSETS["veg_matrix_csv"],
            fire_veg_matrix_json_url=STAC_TEST_ASSETS["veg_matrix_json"],
            geometry=sample_geometry,
            bbox=bbox,
            classification_breaks=classification_breaks,
            datetime_str="2023-08-15T12:00:00Z",
        )
        
        assert item_dict["id"] == "test_fire-veg-matrix-job_123"
        assert item_dict["properties"]["product_type"] == "vegetation_fire_matrix"
        
        collection_items = await catalog_manager.get_collection_items("vegetation-matrices")
        assert len(collection_items) == 1

    @pytest.mark.asyncio
    async def test_get_catalog_and_collections(self, catalog_manager: STACCatalogManager) -> None:
        """Test retrieving catalog and collection metadata"""
        await catalog_manager.initialize_catalog()
        
        # Get root catalog
        catalog_dict = await catalog_manager.get_catalog()
        assert catalog_dict is not None
        assert catalog_dict["id"] == "fire-recovery-catalog"
        
        # Get specific collection
        collection_dict = await catalog_manager.get_collection("fire-severity")
        assert collection_dict is not None
        assert collection_dict["id"] == "fire-severity"
        assert "Fire Severity Analysis" in collection_dict["title"]

    @pytest.mark.asyncio
    async def test_proper_catalog_links(
        self, catalog_manager: STACCatalogManager, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test that items have proper catalog links"""
        await catalog_manager.initialize_catalog()
        
        # Add an item
        cog_urls = {"rbr": STAC_TEST_ASSETS["rbr_cog"]}
        item_dict = await catalog_manager.add_fire_severity_item(
            fire_event_name="test_fire",
            job_id="job_123",
            cog_urls=cog_urls,
            geometry=sample_geometry,
            datetime_str="2023-08-15T12:00:00Z",
        )
        
        # Check links
        links = {link["rel"]: link["href"] for link in item_dict["links"]}
        
        assert "collection" in links
        assert "parent" in links
        assert "root" in links
        
        # Verify link URLs are properly formed
        assert links["root"] == f"{STAC_TEST_BASE_URL}/catalog.json"
        assert "fire-severity" in links["collection"]
        assert "fire-severity" in links["parent"]

    @pytest.mark.asyncio
    async def test_multiple_items_same_collection(
        self, catalog_manager: STACCatalogManager, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test adding multiple items to the same collection"""
        await catalog_manager.initialize_catalog()
        
        cog_urls = {"rbr": STAC_TEST_ASSETS["rbr_cog"]}
        
        # Add first item
        await catalog_manager.add_fire_severity_item(
            fire_event_name="fire_one",
            job_id="job_123",
            cog_urls=cog_urls,
            geometry=sample_geometry,
            datetime_str="2023-08-15T12:00:00Z",
        )
        
        # Add second item
        await catalog_manager.add_fire_severity_item(
            fire_event_name="fire_two",
            job_id="job_456",
            cog_urls=cog_urls,
            geometry=sample_geometry,
            datetime_str="2023-08-16T12:00:00Z",
        )
        
        # Both items should be in the collection
        collection_items = await catalog_manager.get_collection_items("fire-severity")
        assert len(collection_items) == 2
        
        item_ids = {item["id"] for item in collection_items}
        expected_ids = {"fire_one-severity-job_123", "fire_two-severity-job_456"}
        assert item_ids == expected_ids

    @pytest.mark.asyncio
    async def test_factory_methods(self) -> None:
        """Test the factory methods for creating catalog managers"""
        # Test factory methods
        testing_manager = STACCatalogManager.for_testing(STAC_TEST_BASE_URL)
        production_manager = STACCatalogManager.for_production("https://prod.example.com")
        
        assert testing_manager.base_url == STAC_TEST_BASE_URL
        assert production_manager.base_url == "https://prod.example.com"
        
        # Different storage instances
        assert testing_manager.storage != production_manager.storage