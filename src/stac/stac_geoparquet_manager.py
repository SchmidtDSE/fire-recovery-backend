import asyncio
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from geojson_pydantic import Polygon
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
        item_id = f"{fire_event_name}-severity-{job_id}"

        # Get stac compliant bbox from the geometry
        geom_shape = shape(geometry)
        bbox = geom_shape.bounds  # (minx, miny, maxx, maxy)

        # Create assets dictionary with all three metrics
        assets = {}

        # Add RBR asset if available
        if "rbr" in cog_urls:
            assets["rbr"] = {
                "href": cog_urls["rbr"],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": "Relativized Burn Ratio (RBR)",
                "roles": ["data"],
            }

        # Add dNBR asset if available
        if "dnbr" in cog_urls:
            assets["dnbr"] = {
                "href": cog_urls["dnbr"],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": "Differenced Normalized Burn Ratio (dNBR)",
                "roles": ["data"],
            }

        # Add RdNBR asset if available
        if "rdnbr" in cog_urls:
            assets["rdnbr"] = {
                "href": cog_urls["rdnbr"],
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "title": "Relativized Differenced Normalized Burn Ratio (RdNBR)",
                "roles": ["data"],
            }

        # Create the STAC item
        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "properties": {
                "datetime": datetime_str,
                "fire_event_name": fire_event_name,
                "job_id": job_id,
                "product_type": "fire_severity",
                "boundary_type": boundary_type,
            },
            "geometry": geometry,
            "bbox": bbox,
            "assets": assets,
            "links": [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{fire_event_name}/items/{item_id}.json",
                    "type": "application/json",
                },
                {
                    "rel": "related",
                    "href": f"{self.base_url}/{fire_event_name}/items/{fire_event_name}-boundary-{job_id}.json",
                    "type": "application/json",
                    "title": "Related fire boundary product",
                },
            ],
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
        boundary_geojson_url: str,
        bbox: List[float],
        datetime_str: str,
        boundary_type: str = "coarse",
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
                "product_type": "fire_boundary",
                "boundary_type": boundary_type,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [bbox[0], bbox[1]],
                        [bbox[2], bbox[1]],
                        [bbox[2], bbox[3]],
                        [bbox[0], bbox[3]],
                        [bbox[0], bbox[1]],
                    ]
                ],
            },
            "bbox": bbox,  # Make sure bbox is included in the root level
            "assets": {
                "refined_boundary": {
                    "href": boundary_geojson_url,
                    "type": "application/geo+json",
                    "title": f"{boundary_type.capitalize()} Fire Boundary",
                    "roles": ["data"],
                },
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{fire_event_name}/items/{item_id}.json",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": f"{self.base_url}/{fire_event_name}/collection.json",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": f"{self.base_url}/catalog.json",
                    "type": "application/json",
                },
            ],
        }

        # Add title to make the item more descriptive
        stac_item["properties"]["title"] = f"{fire_event_name} {boundary_type} boundary"

        # Add a related link to the severity item
        stac_item["links"].append(
            {
                "rel": "related",
                "href": f"{self.base_url}/{fire_event_name}/items/{fire_event_name}-severity-{job_id}.json",
                "type": "application/json",
                "title": "Related fire severity product",
            }
        )

        try:
            # Validate the STAC item
            self.validate_stac_item(stac_item)

            # Add item to the fire event's GeoParquet file
            await self.add_items_to_parquet(fire_event_name, [stac_item])

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
        datetime_str: str,
    ) -> Dict[str, Any]:
        """
        Create a STAC item for a vegetation/fire severity matrix.

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            matrix_url: URL to the CSV matrix file
            geometry: GeoJSON geometry object
            bbox: Bounding box [minx, miny, maxx, maxy]
            datetime_str: Timestamp for the item

        Returns:
            The created STAC item
        """
        item_id = f"{fire_event_name}-veg-matrix-{job_id}"

        # Create the STAC item
        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": item_id,
            "properties": {
                "title": f"Vegetation Fire Matrix for {fire_event_name}",
                "description": "Matrix of vegetation types affected by different fire severity classes",
                "datetime": datetime_str,
                "fire_event_name": fire_event_name,
                "job_id": job_id,
                "product_type": "vegetation_fire_matrix",
            },
            "geometry": geometry,
            "bbox": bbox,
            "assets": {
                "fire_veg_matrix_csv": {
                    "href": fire_veg_matrix_csv_url,
                    "type": "text/csv",
                    "title": "Vegetation Fire Severity Matrix",
                    "description": "CSV showing hectares of each vegetation type affected by fire severity classes",
                    "roles": ["data"],
                },
                "fire_veg_matrix_json": {
                    "href": fire_veg_matrix_json_url,
                    "type": "application/json",
                    "title": "Vegetation Fire Severity Matrix (JSON)",
                    "description": "JSON representation of the vegetation fire severity matrix (for easier integration with frontend)",
                    "roles": ["data"],
                },
            },
            "links": [
                {
                    "rel": "self",
                    "href": f"{self.base_url}/{fire_event_name}/items/{item_id}.json",
                    "type": "application/json",
                },
                {
                    "rel": "collection",
                    "href": f"{self.base_url}/{fire_event_name}/collection.json",
                    "type": "application/json",
                },
                {
                    "rel": "root",
                    "href": f"{self.base_url}/catalog.json",
                    "type": "application/json",
                },
                {
                    "rel": "related",
                    "href": f"{self.base_url}/{fire_event_name}/items/{fire_event_name}-severity-{job_id}.json",
                    "type": "application/json",
                    "title": "Related fire severity product",
                },
            ],
        }

        # Validate the STAC item
        self.validate_stac_item(stac_item)

        # Add item to the fire event's GeoParquet file
        await self.add_items_to_parquet(fire_event_name, [stac_item])

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

        # If the parquet file doesn't exist yet, just write the items directly
        if not os.path.exists(self.parquet_path):
            await rustac.write(self.parquet_path, items, format="geoparquet")
            return self.parquet_path

        # Read existing items first
        all_items = await rustac.read(self.parquet_path)
        all_items = all_items["features"]

        # Combine with new items
        all_items.extend(items)

        # Write back to parquet file
        await rustac.write(self.parquet_path, all_items, format="geoparquet")

        return self.parquet_path

    async def get_items_by_fire_event(
        self, fire_event_name: str
    ) -> List[Dict[str, Any]]:
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
                "args": [{"property": "fire_event_name"}, fire_event_name],
            },
        )

    async def get_item_by_id(self, item_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID from the GeoParquet file
        """
        if not os.path.exists(self.parquet_path):
            return None

        # Use rustac's native search with combined filters
        items = await rustac.search(self.parquet_path, ids=[item_id])

        return items[0] if items else None

    async def get_items_by_id_and_coarseness(
        self, item_id: str, boundary_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific STAC item by ID and boundary type from the GeoParquet file
        """
        if not os.path.exists(self.parquet_path):
            return None

        # Use rustac's native search with combined filters
        items = await rustac.search(
            self.parquet_path,
            filter={
                "op": "and",
                "args": [
                    {"op": "=", "args": [{"property": "id"}, item_id]},
                    {"op": "=", "args": [{"property": "boundary_type"}, boundary_type]},
                ],
            },
        )

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
        if not os.path.exists(self.parquet_path):
            return []

        # Build filter for fire_event_name
        fire_event_filter = {
            "op": "=",
            "args": [{"property": "fire_event_name"}, fire_event_name],
        }

        # Build search parameters
        search_params = {}

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

        return await rustac.search(self.parquet_path, **search_params)
