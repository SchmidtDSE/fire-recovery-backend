import asyncio
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import rustac
import json
import geopandas as gpd
from shapely.geometry import mapping, shape
from pathlib import Path
from stac_pydantic import Item as StacItem
from stac_pydantic.shared import Asset
from pydantic import ValidationError

class STACGeoParquetManager:
    """
    Manages STAC items stored in GeoParquet format
    """
    def __init__(self, base_url: str, storage_dir: str):
        """
        Initialize the STAC GeoParquet manager
        
        Args:
            base_url: Base URL for STAC assets
            storage_dir: Local directory for storing parquet files
        """
        self.base_url = base_url
        self.storage_dir = storage_dir
        self.parquet_path = os.path.join(storage_dir, "fire_recovery_stac.parquet")
        Path(storage_dir).mkdir(parents=True, exist_ok=True)
        
    def get_parquet_path(self, fire_event_name: str) -> str:
        """Get path to the GeoParquet file for a fire event"""
        return os.path.join(self.storage_dir, f"{fire_event_name}.parquet")
    
    def get_parquet_url(self, fire_event_name: str) -> str:
        """Get the URL to the GeoParquet file for a fire event"""
        return f"{self.base_url}/{fire_event_name}.parquet"

    def validate_stac_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a STAC item against the STAC specification using stac-pydantic.
        
        Args:
            item: The STAC item to validate
            
        Returns:
            The validated STAC item
            
        Raises:
            ValidationError: If the STAC item is invalid
        """
        try:
            StacItem.model_validate(item)
        except ValidationError as e:
            raise ValidationError(f"STAC item validation failed: {str(e)}", StacItem)

    async def create_fire_severity_item(
        self,
        fire_event_name: str,
        job_id: str,
        cog_url: str,
        bbox: List[float],
        datetime_str: str
    ) -> Dict[str, Any]:
        """
        Create a STAC item for fire severity analysis and add it to the GeoParquet file
        """
        item_id = f"{fire_event_name}-severity-{job_id}"
        
        # Create the STAC item
        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "properties": {
                "datetime": datetime_str,
                "fire_event_name": fire_event_name,
                "job_id": job_id,
                "product_type": "fire_severity"
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [bbox[0], bbox[1]],
                        [bbox[2], bbox[1]],
                        [bbox[2], bbox[3]],
                        [bbox[0], bbox[3]],
                        [bbox[0], bbox[1]]
                    ]
                ]
            },
            "assets": {
                "rbr": {
                    "href": cog_url,
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "title": "Relativized Burn Ratio (RBR)",
                    "roles": ["data"]
                }
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{fire_event_name}/items/{item_id}.json",
                    "type": "application/json"
                }
            ]
        }
        
        # Validate the STAC item
        self.validate_stac_item(stac_item)

        # Add item to the fire event's GeoParquet file
        await self.add_items_to_parquet(fire_event_name, [stac_item])
        
        return stac_item
    
    async def create_boundary_item(
        self,
        fire_event_name: str,
        job_id: str,
        geojson_url: str,
        cog_url: str,
        bbox: List[float],
        datetime_str: str
    ) -> Dict[str, Any]:
        """
        Create a STAC item for boundary refinement and add it to the GeoParquet file
        """
        item_id = f"{fire_event_name}-boundary-{job_id}"
        
        # Create the STAC item
        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "properties": {
                "datetime": datetime_str,
                "fire_event_name": fire_event_name,
                "job_id": job_id,
                "product_type": "refined_boundary"
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [bbox[0], bbox[1]],
                        [bbox[2], bbox[1]],
                        [bbox[2], bbox[3]],
                        [bbox[0], bbox[3]],
                        [bbox[0], bbox[1]]
                    ]
                ]
            },
            "assets": {
                "refined_boundary": {
                    "href": geojson_url,
                    "type": "application/geo+json",
                    "title": "Refined Fire Boundary",
                    "roles": ["data"]
                },
                "refined_severity": {
                    "href": cog_url,
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "title": "Refined Fire Severity",
                    "roles": ["data"]
                }
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{fire_event_name}/items/{item_id}.json",
                    "type": "application/json"
                }
            ]
        }
        
        # Validate the STAC item
        self.validate_stac_item(stac_item)

        # Add item to the fire event's GeoParquet file
        await self.add_items_to_parquet(fire_event_name, [stac_item])
        
        return stac_item
    
    async def add_items_to_parquet(self, fire_event_name: str, items: List[Dict[str, Any]]) -> str:
        """
        Add STAC items to the consolidated GeoParquet file
        
        Returns:
            Path to the updated GeoParquet file
        """
        # Validate all items before adding
        for item in items:
            self.validate_stac_item(item)
        
        # If the parquet file doesn't exist yet, just write the items directly
        if not os.path.exists(self.parquet_path):
            await rustac.write(self.parquet_path, items, format="geoparquet")
            return self.parquet_path
        
        # For existing files, we need to handle deduplication
        # Read existing items first
        existing_items = await rustac.search(self.parquet_path, use_duckdb=True)
        
        # Get IDs of new items for deduplication
        new_item_ids = {item["id"] for item in items}
        
        # Filter out existing items with the same IDs
        filtered_items = [item for item in existing_items if item["id"] not in new_item_ids]
        
        # Combine with new items
        all_items = filtered_items + items
        
        # Write back to parquet file
        await rustac.write(self.parquet_path, all_items, format="geoparquet")
        
        # In a production environment, you'd upload this file to blob storage here
        # Example: upload_to_blob_storage(self.parquet_path, "fire_recovery_stac.parquet")
        
        return self.parquet_path

    async def get_items_by_fire_event(self, fire_event_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all STAC items for a fire event from the GeoParquet file
        """
        if not os.path.exists(self.parquet_path):
            return []
        
        # Use rustac's native search with fire_event_name filter
        return await rustac.search(
            self.parquet_path,
            filter={
                "op": "=", 
                "args": [{"property": "properties.fire_event_name"}, fire_event_name]
            },
            use_duckdb=True
        )

    async def get_item_by_id(self, fire_event_name: str, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID from the GeoParquet file
        """
        if not os.path.exists(self.parquet_path):
            return None
        
        # Use rustac's native search with combined filters
        items = await rustac.search(
            self.parquet_path,
            ids=[item_id],
            filter={
                "op": "=", 
                "args": [{"property": "properties.fire_event_name"}, fire_event_name]
            },
            use_duckdb=True
        )
        
        return items[0] if items else None

    async def search_items(
        self, 
        fire_event_name: str,
        product_type: Optional[str] = None,
        bbox: Optional[List[float]] = None,
        datetime_range: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for STAC items using filters
        """
        if not os.path.exists(self.parquet_path):
            return []
        
        # Build filter for fire_event_name
        fire_event_filter = {
            "op": "=", 
            "args": [{"property": "properties.fire_event_name"}, fire_event_name]
        }
        
        # Build search parameters
        search_params = {"use_duckdb": True}
        
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
                "args": [{"property": "properties.product_type"}, product_type]
            }
            search_params["filter"] = {
                "op": "and",
                "args": [fire_event_filter, product_filter]
            }
        else:
            search_params["filter"] = fire_event_filter
        
        return await rustac.search(self.parquet_path, **search_params)