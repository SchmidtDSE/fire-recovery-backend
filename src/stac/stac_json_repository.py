from typing import Dict, List, Any, Optional
import pystac
from src.core.storage.interface import StorageInterface


class STACJSONRepository:
    """Repository class for STAC JSON storage operations using pystac and individual JSON files"""

    def __init__(self, storage: StorageInterface):
        """
        Initialize the STAC JSON repository

        Args:
            storage: Storage interface (e.g., MinIO) for storing JSON files
        """
        self.storage = storage

    def item_to_dict(
        self, item: pystac.Item, skip_validation: bool = False
    ) -> Dict[str, Any]:
        """
        Convert a pystac Item to a dictionary, avoiding HREF validation in tests.

        Args:
            item: The pystac Item to convert
            skip_validation: If True, skip HREF transformation to avoid validation

        Returns:
            Dictionary representation of the STAC item
        """
        if not skip_validation:
            return item.to_dict()
        else:
            return item.to_dict(transform_hrefs=False)

    def _generate_item_path(self, item_properties: Dict[str, Any], item_id: str) -> str:
        """Generate storage path for STAC item based on properties"""
        fire_event_name = item_properties.get("fire_event_name", "unknown")
        product_type = item_properties.get("product_type", "unknown")
        job_id = item_properties.get("job_id", "unknown")
        
        # Use product_type-job_id as filename for uniqueness
        filename = f"{product_type}-{job_id}.json"
        return f"stac/{fire_event_name}/{filename}"

    def _extract_search_terms_from_path(self, path: str) -> Dict[str, str]:
        """Extract fire_event_name and product info from file path"""
        # Path format: stac/{fire_event_name}/{product_type}-{job_id}.json
        parts = path.split("/")
        if len(parts) >= 3 and parts[0] == "stac":
            fire_event_name = parts[1]
            filename = parts[2].replace(".json", "")
            if "-" in filename:
                product_type = filename.split("-")[0]
                return {"fire_event_name": fire_event_name, "product_type": product_type}
        return {}

    async def add_item(self, item_dict: Dict[str, Any], skip_validation: bool = False) -> str:
        """
        Add a STAC item to storage using pystac validation

        Args:
            item_dict: STAC item dictionary
            skip_validation: If True, skip validation (useful for testing)

        Returns:
            Storage URL for the saved item
        """
        # Create pystac Item from dict and validate
        item = pystac.Item.from_dict(item_dict)
        if not skip_validation:
            item.validate()

        # Generate file path
        file_path = self._generate_item_path(item.properties, item.id)

        # Save as JSON using storage interface
        return await self.storage.save_json(self.item_to_dict(item, skip_validation), file_path)

    async def get_item_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID
        
        Note: This requires searching through all files since we organize by fire_event_name
        For better performance, use get_item_by_fire_event_and_id if you know the fire event
        """
        # List all STAC files
        all_files = await self.storage.list_files("stac/")
        
        for file_path in all_files:
            if file_path.endswith(".json"):
                try:
                    item_data = await self.storage.get_json(file_path)
                    if item_data.get("id") == item_id:
                        return item_data
                except Exception:
                    continue
        
        return None

    async def get_item_by_fire_event_and_id(self, fire_event_name: str, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by fire event name and ID (more efficient)
        """
        # List files for this fire event
        fire_event_files = await self.storage.list_files(f"stac/{fire_event_name}/")
        
        for file_path in fire_event_files:
            if file_path.endswith(".json"):
                try:
                    item_data = await self.storage.get_json(file_path)
                    if item_data.get("id") == item_id:
                        return item_data
                except Exception:
                    continue
        
        return None

    async def get_items_by_fire_event(self, fire_event_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all STAC items for a fire event
        """
        items = []
        fire_event_files = await self.storage.list_files(f"stac/{fire_event_name}/")
        
        for file_path in fire_event_files:
            if file_path.endswith(".json"):
                try:
                    item_data = await self.storage.get_json(file_path)
                    items.append(item_data)
                except Exception:
                    continue
        
        return items

    async def get_items_by_id_and_coarseness(
        self, item_id: str, boundary_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID and boundary type
        """
        # This requires searching through files - could be optimized with indexing later
        all_files = await self.storage.list_files("stac/")
        
        for file_path in all_files:
            if file_path.endswith(".json"):
                try:
                    item_data = await self.storage.get_json(file_path)
                    properties = item_data.get("properties", {})
                    if (item_data.get("id") == item_id and 
                        properties.get("boundary_type") == boundary_type):
                        return item_data
                except Exception:
                    continue
        
        return None

    async def get_items_by_id_and_classification_breaks(
        self, item_id: str, classification_breaks: Optional[List[float]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID and classification breaks
        """
        all_files = await self.storage.list_files("stac/")
        
        for file_path in all_files:
            if file_path.endswith(".json"):
                try:
                    item_data = await self.storage.get_json(file_path)
                    properties = item_data.get("properties", {})
                    
                    # Check ID match
                    if item_data.get("id") != item_id:
                        continue
                        
                    # Check classification breaks if provided
                    if classification_breaks is not None:
                        item_breaks = properties.get("classification_breaks")
                        if item_breaks != classification_breaks:
                            continue
                    
                    return item_data
                    
                except Exception:
                    continue
        
        return None

    async def search_items(
        self,
        fire_event_name: str,
        product_type: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for STAC items using filters
        
        Note: bbox and datetime_range filtering not implemented yet (as per your requirements)
        """
        # Get all items for the fire event
        items = await self.get_items_by_fire_event(fire_event_name)
        
        # Filter by product type if specified
        if product_type:
            filtered_items = []
            for item in items:
                properties = item.get("properties", {})
                if properties.get("product_type") == product_type:
                    filtered_items.append(item)
            return filtered_items
        
        return items