from typing import Dict, List, Any, Optional
from geojson_pydantic import Polygon
import rustac
from shapely.geometry import shape
from stac_pydantic import Item as StacItem
from pydantic import ValidationError

from src.core.storage.interface import StorageInterface
from src.config.storage import get_temp_storage, get_final_storage


class STACGeoParquetManager:
    """
    Manages STAC items stored in GeoParquet format using MinIO storage
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


    async def _get_existing_items(self) -> List[Dict[str, Any]]:
        """Get existing items from parquet file, return empty list if file doesn't exist"""
        try:
            parquet_data = await self.storage.get_bytes(self.parquet_path)
            temp_buffer = self.storage.create_file_like_buffer(parquet_data, "temp.parquet")
            
            result = await rustac.read(temp_buffer)
            return result["features"]
        except Exception:
            # File doesn't exist or other error
            return []

    async def _write_items_to_storage(self, items: List[Dict[str, Any]]) -> str:
        """Write items to parquet format and save to storage"""
        output_buffer = self.storage.create_output_buffer("output.parquet")
        
        # Write parquet data to buffer
        await rustac.write(output_buffer, items, format="geoparquet")
        
        # Save buffer contents to storage
        output_buffer.seek(0)
        parquet_bytes = output_buffer.read()
        await self.storage.save_bytes(parquet_bytes, self.parquet_path, temporary=False)
        
        return self.parquet_path

    async def _search_parquet(self, **search_params) -> List[Dict[str, Any]]:
        """Search parquet file with given parameters"""
        try:
            parquet_data = await self.storage.get_bytes(self.parquet_path)
            temp_buffer = self.storage.create_file_like_buffer(parquet_data, "temp.parquet")
            
            return await rustac.search(temp_buffer, **search_params)
        except Exception:
            # File doesn't exist or other error
            return []

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
        classification_breaks: List[float],
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
                "classification_breaks": classification_breaks,
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
        id_filter = {"op": "=", "args": [{"property": "id"}, item_id]}

        # If classification_breaks is provided, add it to the filter
        if classification_breaks is not None:
            classification_filter = {
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

        return await self._search_parquet(**search_params)

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
