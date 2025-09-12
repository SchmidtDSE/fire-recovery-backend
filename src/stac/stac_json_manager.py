from typing import Dict, List, Any, Optional
from src.core.storage.interface import StorageInterface
from src.stac.stac_json_repository import STACJSONRepository
from src.stac.stac_item_factory import STACItemFactory
from geojson_pydantic import Polygon, Feature


class STACJSONManager:
    """Manager class for STAC operations using individual JSON files and pystac validation"""

    def __init__(
        self,
        base_url: str,
        storage: StorageInterface,
    ):
        """
        Initialize the STAC JSON manager

        Args:
            base_url: Base URL for STAC assets and links
            storage: Storage interface for storing JSON files
        """
        self.base_url = base_url
        self.storage = storage
        self._repository = STACJSONRepository(storage)
        self._item_factory = STACItemFactory(base_url)

    @classmethod
    def for_testing(cls, base_url: str, storage: StorageInterface) -> "STACJSONManager":
        """Create a manager instance configured for testing"""
        return cls(base_url=base_url, storage=storage)

    @classmethod
    def for_production(
        cls, base_url: str, storage: StorageInterface
    ) -> "STACJSONManager":
        """Create a manager instance configured for production"""
        return cls(base_url=base_url, storage=storage)

    async def create_fire_severity_item(
        self,
        fire_event_name: str,
        job_id: str,
        cog_urls: Dict[str, str],
        geometry: Polygon | Feature,
        datetime_str: str,
        boundary_type: str = "coarse",
        skip_validation: bool = False,
    ) -> str:
        """
        Create and store a fire severity STAC item

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            cog_urls: Dictionary of COG URLs {'rbr': url, 'dnbr': url, 'rdnbr': url}
            geometry: GeoJSON geometry object
            datetime_str: Timestamp for the item
            boundary_type: Type of boundary ('coarse' or 'refined')

        Returns:
            The created STAC item
        """
        # Create STAC item using factory with pystac validation
        stac_item = self._item_factory.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_urls=cog_urls,
            geometry=geometry,
            datetime_str=datetime_str,
            boundary_type=boundary_type,
            skip_validation=skip_validation,
        )

        # Store the item and return the storage URL
        stac_item_url = await self._repository.add_item(
            stac_item, skip_validation=skip_validation
        )

        return stac_item_url

    async def create_boundary_item(
        self,
        fire_event_name: str,
        job_id: str,
        boundary_geojson_url: str,
        bbox: List[float],
        datetime_str: str,
        boundary_type: str = "coarse",
        skip_validation: bool = False,
    ) -> Dict[str, Any]:
        """
        Create and store a boundary STAC item

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            boundary_geojson_url: URL to the boundary GeoJSON file
            bbox: Bounding box [minx, miny, maxx, maxy]
            datetime_str: Timestamp for the item
            boundary_type: Type of boundary ('coarse' or 'refined')

        Returns:
            The created STAC item
        """
        # Create STAC item using factory with pystac validation
        stac_item = self._item_factory.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            boundary_geojson_url=boundary_geojson_url,
            bbox=bbox,
            datetime_str=datetime_str,
            boundary_type=boundary_type,
            skip_validation=skip_validation,
        )

        # Store the item
        await self._repository.add_item(stac_item, skip_validation=skip_validation)

        return stac_item

    async def create_veg_matrix_item(
        self,
        fire_event_name: str,
        job_id: str,
        fire_veg_matrix_csv_url: str,
        fire_veg_matrix_json_url: str,
        geometry: Polygon | Feature,
        bbox: List[float],
        classification_breaks: List[float],
        datetime_str: str,
        skip_validation: bool = False,
    ) -> Dict[str, Any]:
        """
        Create and store a vegetation matrix STAC item

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            fire_veg_matrix_csv_url: URL to the CSV matrix file
            fire_veg_matrix_json_url: URL to the JSON matrix file
            geometry: GeoJSON geometry object
            bbox: Bounding box [minx, miny, maxx, maxy]
            classification_breaks: Classification break values
            datetime_str: Timestamp for the item

        Returns:
            The created STAC item
        """
        # Create STAC item using factory with pystac validation
        stac_item = self._item_factory.create_veg_matrix_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            fire_veg_matrix_csv_url=fire_veg_matrix_csv_url,
            fire_veg_matrix_json_url=fire_veg_matrix_json_url,
            geometry=geometry,
            bbox=bbox,
            classification_breaks=classification_breaks,
            datetime_str=datetime_str,
            skip_validation=skip_validation,
        )

        # Store the item
        await self._repository.add_item(stac_item, skip_validation=skip_validation)

        return stac_item

    # Retrieval methods - delegate to repository
    async def get_item_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """Get a STAC item by ID"""
        return await self._repository.get_item_by_id(item_id)

    async def get_item_by_fire_event_and_id(
        self, fire_event_name: str, item_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get a STAC item by fire event name and ID (more efficient)"""
        return await self._repository.get_item_by_fire_event_and_id(
            fire_event_name, item_id
        )

    async def get_items_by_fire_event(
        self, fire_event_name: str
    ) -> List[Dict[str, Any]]:
        """Get all STAC items for a fire event"""
        return await self._repository.get_items_by_fire_event(fire_event_name)

    async def get_items_by_id_and_coarseness(
        self, item_id: str, boundary_type: str
    ) -> Optional[Dict[str, Any]]:
        """Get a STAC item by ID and boundary type"""
        return await self._repository.get_items_by_id_and_coarseness(
            item_id, boundary_type
        )

    async def get_items_by_id_and_classification_breaks(
        self, item_id: str, classification_breaks: Optional[List[float]] = None
    ) -> Optional[Dict[str, Any]]:
        """Get a STAC item by ID and classification breaks"""
        return await self._repository.get_items_by_id_and_classification_breaks(
            item_id, classification_breaks
        )

    async def search_items(
        self,
        fire_event_name: str,
        product_type: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Search for STAC items using filters"""
        return await self._repository.search_items(
            fire_event_name=fire_event_name,
            product_type=product_type,
            bbox=bbox,
            datetime_range=datetime_range,
        )
