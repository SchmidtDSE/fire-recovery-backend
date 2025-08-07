from typing import Dict, List, Any, Optional
from geojson_pydantic import Polygon

from src.core.storage.interface import StorageInterface
from src.config.storage import get_temp_storage, get_final_storage
from src.stac.stac_item_factory import STACItemFactory
from src.stac.stac_geoparquet_repository import STACGeoParquetRepository


class STACGeoParquetManager:
    """
    Manages STAC items stored in GeoParquet format using MinIO storage.
    Coordinates between STAC item creation and storage operations.
    """

    def __init__(self, base_url: str, storage: StorageInterface):
        """
        Initialize the STAC GeoParquet manager

        Args:
            base_url: Base URL for STAC assets (likely a MinIO endpoint)
            storage: Storage interface (e.g., MinIO) for storing parquet files
        """
        self.base_url = base_url
        self.storage = storage
        self.parquet_path = "stac/fire_recovery_stac.parquet"

        # Initialize factory and repository
        self._item_factory = STACItemFactory(base_url)
        self._repository = STACGeoParquetRepository(storage)

    def get_parquet_path(self, fire_event_name: str) -> str:
        """Get path to the GeoParquet file for a fire event"""
        return f"stac/{fire_event_name}.parquet"

    def get_parquet_url(self, fire_event_name: str) -> str:
        """Get the URL to the GeoParquet file for a fire event"""
        return f"{self.base_url}/{fire_event_name}.parquet"

    def validate_stac_item(self, item: Dict[str, Any]) -> None:
        """
        Validate a STAC item against the STAC specification using stac-pydantic.

        Args:
            item: The STAC item to validate

        Raises:
            ValidationError: If the STAC item is invalid
        """
        self._item_factory.validate_stac_item(item)

    async def create_fire_severity_item(
        self,
        fire_event_name: str,
        job_id: str,
        cog_urls: Dict[str, str],  # Changed to accept dictionary of URLs
        geometry: Polygon,
        datetime_str: str,
        boundary_type: str = "coarse",
    ) -> Dict[str, Any]:
        """
        Create a STAC item for fire severity analysis and add it to the GeoParquet file

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            cog_urls: Dictionary of COG URLs for each metric {'rbr': url, 'dnbr': url, 'rdnbr': url}
            geometry: GeoJSON geometry object
            datetime_str: Timestamp for the item
            boundary_type: Type of boundary ('coarse' or 'refined')

        Returns:
            The created STAC item
        """
        # Create STAC item using factory
        stac_item = self._item_factory.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_urls=cog_urls,
            geometry=geometry,
            datetime_str=datetime_str,
            boundary_type=boundary_type,
        )

        # Add item to the fire event's GeoParquet file using repository
        await self._repository.add_items([stac_item])

        return stac_item

    async def create_boundary_item(
        self,
        fire_event_name: str,
        job_id: str,
        boundary_geojson_url: str,
        bbox: List[float],
        datetime_str: str,
        boundary_type: str = "coarse",
    ) -> Dict[str, Any]:
        """
        Create a STAC item for boundary refinement and add it to the GeoParquet file
        """
        try:
            # Create STAC item using factory
            stac_item = self._item_factory.create_boundary_item(
                fire_event_name=fire_event_name,
                job_id=job_id,
                boundary_geojson_url=boundary_geojson_url,
                bbox=bbox,
                datetime_str=datetime_str,
                boundary_type=boundary_type,
            )

            # Add item to the fire event's GeoParquet file using repository
            await self._repository.add_items([stac_item])

            return stac_item
        except Exception as e:
            print(f"Error creating boundary item: {str(e)}")
            raise e

    async def create_veg_matrix_item(
        self,
        fire_event_name: str,
        job_id: str,
        fire_veg_matrix_csv_url: str,
        fire_veg_matrix_json_url: str,
        geometry: Dict[str, Any],
        bbox: List[float],
        classification_breaks: List[float],
        datetime_str: str,
    ) -> Dict[str, Any]:
        """
        Create a STAC item for a vegetation/fire severity matrix.

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
        # Create STAC item using factory
        stac_item = self._item_factory.create_veg_matrix_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            fire_veg_matrix_csv_url=fire_veg_matrix_csv_url,
            fire_veg_matrix_json_url=fire_veg_matrix_json_url,
            geometry=geometry,
            bbox=bbox,
            classification_breaks=classification_breaks,
            datetime_str=datetime_str,
        )

        # Add item to the fire event's GeoParquet file using repository
        await self._repository.add_items([stac_item])

        return stac_item

    async def add_items_to_parquet(
        self, fire_event_name: str, items: List[Dict[str, Any]]
    ) -> str:
        """
        Add STAC items to the consolidated GeoParquet file

        Returns:
            Path to the updated GeoParquet file
        """
        # Validate all items before adding
        for item in items:
            self.validate_stac_item(item)

        # Add items using repository
        return await self._repository.add_items(items)

    async def get_items_by_fire_event(
        self, fire_event_name: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all STAC items for a fire event from the GeoParquet file
        """
        return await self._repository.get_items_by_fire_event(fire_event_name)

    async def get_item_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID from the GeoParquet file
        """
        return await self._repository.get_item_by_id(item_id)

    async def get_items_by_id_and_coarseness(
        self, item_id: str, boundary_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID and boundary type from the GeoParquet file
        """
        return await self._repository.get_items_by_id_and_coarseness(
            item_id, boundary_type
        )

    async def get_items_by_id_and_classification_breaks(
        self, item_id: str, classification_breaks: Optional[List[float]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID and classification breaks from the GeoParquet file
        """
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
        """
        Search for STAC items using filters
        """
        return await self._repository.search_items(
            fire_event_name=fire_event_name,
            product_type=product_type,
            bbox=bbox,
            datetime_range=datetime_range,
        )

    @classmethod
    def for_testing(cls, base_url: str) -> "STACGeoParquetManager":
        """
        Create a STACGeoParquetManager configured for testing using temporary storage

        Args:
            base_url: Base URL for STAC assets

        Returns:
            STACGeoParquetManager configured with temporary storage (TEMP_BUCKET_NAME)
        """
        return cls(base_url=base_url, storage=get_temp_storage())

    @classmethod
    def for_production(cls, base_url: str) -> "STACGeoParquetManager":
        """
        Create a STACGeoParquetManager configured for production using permanent storage

        Args:
            base_url: Base URL for STAC assets

        Returns:
            STACGeoParquetManager configured with permanent storage (FINAL_BUCKET_NAME)
        """
        return cls(base_url=base_url, storage=get_final_storage())
