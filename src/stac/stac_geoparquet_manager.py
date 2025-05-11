import asyncio
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
import rustac
import json
import pyarrow as pa
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
        Add STAC items to the GeoParquet file for a fire event
        
        Returns:
            Path to the updated GeoParquet file
        """
        parquet_path = self.get_parquet_path(fire_event_name)
        
        # Validate all items before adding
        validated_items = [self.validate_stac_item(item) for item in items]
        
        # Convert items to arrow table using rustac
        arrow_table = rustac.to_arrow(items)
        
        # If the parquet file already exists, append to it
        if os.path.exists(parquet_path):
            # Read existing table
            existing_table = rustac.read_geoparquet(parquet_path)
            
            # Get unique item IDs from new items
            new_ids = set(item["id"] for item in items)
            
            # Filter existing table to remove items with the same ID
            item_id_indices = existing_table.column("id").index_in(pa.array(list(new_ids)))
            mask = item_id_indices.is_null()  # Keep rows where the ID is NOT in new_ids
            filtered_table = existing_table.filter(mask)
            
            # Combine tables
            arrow_table = pa.concat_tables([filtered_table, arrow_table])

        # Write to parquet
        rustac.write_geoparquet(arrow_table, parquet_path)
        
        # In a production environment, you'd upload this file to blob storage here
        # Example: upload_to_blob_storage(parquet_path, f"{fire_event_name}.parquet")
        
        return parquet_path
    
    async def get_items_by_fire_event(self, fire_event_name: str) -> List[Dict[str, Any]]:
        """
        Retrieve all STAC items for a fire event from the GeoParquet file
        """
        parquet_path = self.get_parquet_path(fire_event_name)
        
        if not os.path.exists(parquet_path):
            return []
        
        # Read the GeoParquet file
        arrow_table = rustac.read_geoparquet(parquet_path)
        
        # Convert back to STAC items
        items = rustac.from_arrow(arrow_table)
        return items
    
    async def get_item_by_id(self, fire_event_name: str, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID from the GeoParquet file
        """
        parquet_path = self.get_parquet_path(fire_event_name)
        
        if not os.path.exists(parquet_path):
            return None
        
        # Read the GeoParquet file
        arrow_table = rustac.read_geoparquet(parquet_path)
        
        # Convert to GeoPandas DataFrame for easier filtering
        gdf = gpd.GeoDataFrame.from_arrow(arrow_table)
        
        # Filter by item_id
        filtered = gdf[gdf['id'] == item_id]
        
        if len(filtered) == 0:
            return None
            
        # Convert back to STAC item
        item_row = filtered.iloc[0]
        stac_item = rustac.from_arrow(pa.Table.from_pandas(filtered))[0]
        
        return stac_item
        
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
        parquet_path = self.get_parquet_path(fire_event_name)
        
        if not os.path.exists(parquet_path):
            return []
            
        # Read the GeoParquet file
        arrow_table = rustac.read_geoparquet(parquet_path)
        
        # Convert to GeoPandas DataFrame for filtering
        gdf = gpd.GeoDataFrame.from_arrow(arrow_table)
        
        # Apply filters
        if product_type:
            gdf = gdf[gdf['properties.product_type'] == product_type]
            
        if bbox:
            from shapely.geometry import box
            search_box = box(bbox[0], bbox[1], bbox[2], bbox[3])
            gdf = gdf[gdf.geometry.intersects(search_box)]
            
        if datetime_range and len(datetime_range) == 2:
            start_date, end_date = datetime_range
            if start_date:
                gdf = gdf[gdf['properties.datetime'] >= start_date]
            if end_date:
                gdf = gdf[gdf['properties.datetime'] <= end_date]
                
        # Convert filtered results back to STAC items
        if len(gdf) > 0:
            return rustac.from_arrow(pa.Table.from_pandas(gdf))
        else:
            return []