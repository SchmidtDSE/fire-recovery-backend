from typing import Dict, List, Any, Optional
import rustac
from src.core.storage.interface import StorageInterface


class STACGeoParquetRepository:
    """Repository class for STAC GeoParquet storage operations using rustac and obstore"""

    def __init__(self, storage: StorageInterface):
        """
        Initialize the STAC GeoParquet repository

        Args:
            storage: Storage interface (e.g., MinIO) for storing parquet files
        """
        self.storage = storage
        self.parquet_path = "stac/fire_recovery_stac.parquet"

    async def _get_existing_items(self) -> List[Dict[str, Any]]:
        """Get existing items from parquet file, return empty list if file doesn't exist"""
        try:
            result = await rustac.read(
                self.parquet_path, store=self.storage.get_obstore()
            )
            return result["features"]
        except Exception:
            # File doesn't exist or other error
            return []

    async def _write_items_to_storage(self, items: List[Dict[str, Any]]) -> str:
        """Write items to parquet format and save to storage"""
        await rustac.write(
            self.parquet_path,
            items,
            store=self.storage.get_obstore(),
            format="geoparquet",
        )
        return self.parquet_path

    async def _search_parquet(self, **search_params: Any) -> List[Dict[str, Any]]:
        """Search parquet file with given parameters"""
        try:
            full_https_path = self.storage.get_url(self.parquet_path)
            return await rustac.search(full_https_path, **search_params)
        except Exception:
            # File doesn't exist or other error
            return []

    async def add_items(self, items: List[Dict[str, Any]]) -> str:
        """
        Add STAC items to the consolidated GeoParquet file

        Args:
            items: List of STAC items to add

        Returns:
            Path to the updated GeoParquet file
        """
        # Get existing items and combine with new ones
        existing_items = await self._get_existing_items()
        all_items = existing_items + items

        # Write combined items to storage
        return await self._write_items_to_storage(all_items)

    async def get_items_by_fire_event(
        self, fire_event_name: str
    ) -> List[Dict[str, Any]]:
        """
        Retrieve all STAC items for a fire event from the GeoParquet file
        """
        return await self._search_parquet(
            filter={
                "op": "=",
                "args": [{"property": "fire_event_name"}, fire_event_name],
            }
        )

    async def get_item_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID from the GeoParquet file
        """
        items = await self._search_parquet(ids=[item_id])
        return items[0] if items else None

    async def get_items_by_id_and_coarseness(
        self, item_id: str, boundary_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID and boundary type from the GeoParquet file
        """
        items = await self._search_parquet(
            filter={
                "op": "and",
                "args": [
                    {"op": "=", "args": [{"property": "id"}, item_id]},
                    {"op": "=", "args": [{"property": "boundary_type"}, boundary_type]},
                ],
            }
        )
        return items[0] if items else None

    async def get_items_by_id_and_classification_breaks(
        self, item_id: str, classification_breaks: Optional[List[float]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID and classification breaks from the GeoParquet file
        """
        # Build the base filter for item ID
        id_filter: Dict[str, Any] = {"op": "=", "args": [{"property": "id"}, item_id]}

        # If classification_breaks is provided, add it to the filter
        combined_filter: Dict[str, Any]
        if classification_breaks is not None:
            classification_filter: Dict[str, Any] = {
                "op": "=",
                "args": [{"property": "classification_breaks"}, classification_breaks],
            }

            # Combine both filters with AND
            combined_filter = {
                "op": "and",
                "args": [id_filter, classification_filter],
            }
        else:
            combined_filter = id_filter

        items = await self._search_parquet(filter=combined_filter)
        return items[0] if items else None

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
        # Build filter for fire_event_name
        fire_event_filter = {
            "op": "=",
            "args": [{"property": "fire_event_name"}, fire_event_name],
        }

        # Build search parameters
        search_params: Dict[str, Any] = {}

        # Add bbox filter if provided
        if bbox:
            search_params["bbox"] = bbox

        # Add datetime filter if provided
        if datetime_range and len(datetime_range) == 2:
            start_date, end_date = datetime_range
            if start_date and end_date:
                search_params["datetime"] = f"{start_date}/{end_date}"
            elif start_date:
                search_params["datetime"] = f"{start_date}/.."
            elif end_date:
                search_params["datetime"] = f"../{end_date}"

        # Combine product_type filter with fire_event_filter if needed
        if product_type:
            product_filter = {
                "op": "=",
                "args": [{"property": "properties.product_type"}, product_type],
            }
            search_params["filter"] = {
                "op": "and",
                "args": [fire_event_filter, product_filter],
            }
        else:
            search_params["filter"] = fire_event_filter

        return await self._search_parquet(**search_params)
