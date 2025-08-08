from typing import Dict, List, Any
from geojson_pydantic import Polygon
from shapely.geometry import shape
import pystac
from datetime import datetime


class STACItemFactory:
    """Factory class for creating STAC items with proper validation"""

    def __init__(self, base_url: str):
        """
        Initialize the STAC item factory
        
        Args:
            base_url: Base URL for STAC assets and links
        """
        self.base_url = base_url

    def validate_stac_item(self, item: pystac.Item) -> None:
        """
        Validate a STAC item using pystac built-in validation

        Args:
            item: The pystac Item to validate

        Raises:
            Exception: If the STAC item is invalid
        """
        item.validate()

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
        Create a STAC item for fire severity analysis using pystac

        Args:
            fire_event_name: Name of the fire event
            job_id: Job ID for the processing task
            cog_urls: Dictionary of COG URLs for each metric {'rbr': url, 'dnbr': url, 'rdnbr': url}
            geometry: GeoJSON geometry object
            datetime_str: Timestamp for the item
            boundary_type: Type of boundary ('coarse' or 'refined')

        Returns:
            The created STAC item as dictionary
        """
        item_id = f"{fire_event_name}-severity-{job_id}"

        # Get bbox from geometry
        geom_shape = shape(geometry)
        bbox = list(geom_shape.bounds)  # (minx, miny, maxx, maxy)

        # Parse datetime
        dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))

        # Create pystac Item
        item = pystac.Item(
            id=item_id,
            geometry=geometry,
            bbox=bbox,
            datetime=dt,
            properties={
                "fire_event_name": fire_event_name,
                "job_id": job_id,
                "product_type": "fire_severity",
                "boundary_type": boundary_type,
            },
        )

        # Add assets using pystac
        if "rbr" in cog_urls:
            item.add_asset(
                "rbr",
                pystac.Asset(
                    href=cog_urls["rbr"],
                    media_type=pystac.MediaType.COG,
                    roles=["data"],
                    title="Relativized Burn Ratio (RBR)",
                ),
            )

        if "dnbr" in cog_urls:
            item.add_asset(
                "dnbr",
                pystac.Asset(
                    href=cog_urls["dnbr"],
                    media_type=pystac.MediaType.COG,
                    roles=["data"],
                    title="Differenced Normalized Burn Ratio (dNBR)",
                ),
            )

        if "rdnbr" in cog_urls:
            item.add_asset(
                "rdnbr",
                pystac.Asset(
                    href=cog_urls["rdnbr"],
                    media_type=pystac.MediaType.COG,
                    roles=["data"],
                    title="Relativized Differenced Normalized Burn Ratio (RdNBR)",
                ),
            )

        # Add links
        item.add_link(
            pystac.Link(
                rel="self",
                target=f"{self.base_url}/{fire_event_name}/items/{item_id}.json",
                media_type="application/json",
            )
        )

        item.add_link(
            pystac.Link(
                rel="related",
                target=f"{self.base_url}/{fire_event_name}/items/{fire_event_name}-boundary-{job_id}.json",
                media_type="application/json",
                title="Related fire boundary product",
            )
        )

        # Validate using pystac
        self.validate_stac_item(item)

        return item.to_dict()

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
        Create a STAC item for boundary refinement using pystac
        """
        item_id = f"{fire_event_name}-boundary-{job_id}"

        # Parse datetime
        dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))

        # Create geometry from bbox
        geometry = {
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
        }

        # Create pystac Item
        item = pystac.Item(
            id=item_id,
            geometry=geometry,
            bbox=bbox,
            datetime=dt,
            properties={
                "title": f"{fire_event_name} {boundary_type} boundary",
                "fire_event_name": fire_event_name,
                "job_id": job_id,
                "product_type": "fire_boundary",
                "boundary_type": boundary_type,
            },
        )

        # Add boundary asset
        item.add_asset(
            "refined_boundary",
            pystac.Asset(
                href=boundary_geojson_url,
                media_type="application/geo+json",
                roles=["data"],
                title=f"{boundary_type.capitalize()} Fire Boundary",
            ),
        )

        # Add links
        item.add_link(
            pystac.Link(
                rel="self",
                target=f"{self.base_url}/{fire_event_name}/items/{item_id}.json",
                media_type="application/json",
            )
        )

        item.add_link(
            pystac.Link(
                rel="collection",
                target=f"{self.base_url}/{fire_event_name}/collection.json",
                media_type="application/json",
            )
        )

        item.add_link(
            pystac.Link(
                rel="root",
                target=f"{self.base_url}/catalog.json",
                media_type="application/json",
            )
        )

        item.add_link(
            pystac.Link(
                rel="related",
                target=f"{self.base_url}/{fire_event_name}/items/{fire_event_name}-severity-{job_id}.json",
                media_type="application/json",
                title="Related fire severity product",
            )
        )

        # Validate using pystac
        self.validate_stac_item(item)

        return item.to_dict()

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
        Create a STAC item for a vegetation/fire severity matrix using pystac

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
            The created STAC item as dictionary
        """
        item_id = f"{fire_event_name}-veg-matrix-{job_id}"

        # Parse datetime
        dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))

        # Create pystac Item
        item = pystac.Item(
            id=item_id,
            geometry=geometry,
            bbox=bbox,
            datetime=dt,
            properties={
                "title": f"Vegetation Fire Matrix for {fire_event_name}",
                "description": "Matrix of vegetation types affected by different fire severity classes",
                "fire_event_name": fire_event_name,
                "job_id": job_id,
                "product_type": "vegetation_fire_matrix",
                "classification_breaks": classification_breaks,
            },
        )

        # Add CSV asset
        item.add_asset(
            "fire_veg_matrix_csv",
            pystac.Asset(
                href=fire_veg_matrix_csv_url,
                media_type="text/csv",
                roles=["data"],
                title="Vegetation Fire Severity Matrix",
                description="CSV showing hectares of each vegetation type affected by fire severity classes",
            ),
        )

        # Add JSON asset
        item.add_asset(
            "fire_veg_matrix_json",
            pystac.Asset(
                href=fire_veg_matrix_json_url,
                media_type="application/json",
                roles=["data"],
                title="Vegetation Fire Severity Matrix (JSON)",
                description="JSON representation of the vegetation fire severity matrix (for easier integration with frontend)",
            ),
        )

        # Add links
        item.add_link(
            pystac.Link(
                rel="self",
                target=f"{self.base_url}/{fire_event_name}/items/{item_id}.json",
                media_type="application/json",
            )
        )

        item.add_link(
            pystac.Link(
                rel="collection",
                target=f"{self.base_url}/{fire_event_name}/collection.json",
                media_type="application/json",
            )
        )

        item.add_link(
            pystac.Link(
                rel="root",
                target=f"{self.base_url}/catalog.json",
                media_type="application/json",
            )
        )

        item.add_link(
            pystac.Link(
                rel="related",
                target=f"{self.base_url}/{fire_event_name}/items/{fire_event_name}-severity-{job_id}.json",
                media_type="application/json",
                title="Related fire severity product",
            )
        )

        # Validate using pystac
        self.validate_stac_item(item)

        return item.to_dict()