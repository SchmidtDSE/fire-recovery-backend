from typing import Dict, List, Any
from geojson_pydantic import Polygon
from shapely.geometry import shape
from stac_pydantic import Item as StacItem
from pydantic import ValidationError


class STACItemFactory:
    """Factory class for creating STAC items with proper validation"""

    def __init__(self, base_url: str):
        """
        Initialize the STAC item factory
        
        Args:
            base_url: Base URL for STAC assets and links
        """
        self.base_url = base_url

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

    def create_fire_severity_item(
        self,
        fire_event_name: str,
        job_id: str,
        cog_urls: Dict[str, str],
        geometry: Polygon,
        datetime_str: str,
        boundary_type: str = "coarse",
    ) -> Dict[str, Any]:
        """
        Create a STAC item for fire severity analysis

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
        stac_item: Dict[str, Any] = {
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

        return stac_item

    def create_boundary_item(
        self,
        fire_event_name: str,
        job_id: str,
        boundary_geojson_url: str,
        bbox: List[float],
        datetime_str: str,
        boundary_type: str = "coarse",
    ) -> Dict[str, Any]:
        """
        Create a STAC item for boundary refinement
        """
        item_id = f"{fire_event_name}-boundary-{job_id}"

        # Create the STAC item
        stac_item: Dict[str, Any] = {
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
            "bbox": bbox,
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

        # Validate the STAC item
        self.validate_stac_item(stac_item)

        return stac_item

    def create_veg_matrix_item(
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
        item_id = f"{fire_event_name}-veg-matrix-{job_id}"

        # Create the STAC item
        stac_item: Dict[str, Any] = {
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

        return stac_item