import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from geojson_pydantic import Feature, FeatureCollection, Polygon
from pydantic import ValidationError
from shapely import Polygon as ShapelyPolygon
from shapely import from_geojson, to_geojson


def validate_polygon(polygon_data: Dict[str, Any]) -> ShapelyPolygon:
    """
    Validates a GeoJSON polygon using shapely.

    Args:
        polygon_data: Dictionary containing polygon data

    Returns:
        Validated Shapely Polygon object

    Raises:
        ValueError: If the polygon is invalid
    """
    try:
        # Convert the input data to JSON string if it's a dict
        if isinstance(polygon_data, dict):
            # If we have a nested geometry
            if (
                "geometry" in polygon_data
                and polygon_data["geometry"].get("type") == "Polygon"
            ):
                polygon_json = json.dumps(polygon_data["geometry"])
            # If we have a direct geometry
            elif (
                polygon_data.get("type") == "Polygon" and "coordinates" in polygon_data
            ):
                polygon_json = json.dumps(polygon_data)
            else:
                raise ValueError(
                    "Invalid polygon data. Expected either a Polygon geometry "
                    "or an object with a geometry property containing a Polygon"
                )

            # Parse the GeoJSON with shapely
            shapely_geom = from_geojson(polygon_json)

            # Verify it's a polygon
            if shapely_geom.geom_type != "Polygon":
                raise ValueError(
                    f"Expected Polygon geometry, got {shapely_geom.geom_type}"
                )

            return shapely_geom
        else:
            raise ValueError("Input must be a dictionary")
    except Exception as e:
        raise ValueError(f"Failed to validate polygon: {str(e)}")


def polygon_to_feature(
    polygon: Union[ShapelyPolygon, Dict[str, Any]],
    properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Converts a polygon to a GeoJSON Feature.

    Args:
        polygon: Shapely Polygon object or dict with polygon data
        properties: Optional properties for the Feature

    Returns:
        Feature as dictionary
    """
    # Convert dict to Shapely object if needed
    if not isinstance(polygon, ShapelyPolygon):
        polygon = validate_polygon(polygon)

    # Default properties
    if properties is None:
        properties = {
            "created": datetime.utcnow().isoformat(),
        }

    # Get GeoJSON representation of the polygon
    geojson_str = to_geojson(polygon)
    geojson_dict = json.loads(geojson_str)

    # Create feature
    feature = {"type": "Feature", "geometry": geojson_dict, "properties": properties}

    return feature


def polygon_to_valid_geojson(
    polygon_data: Dict[str, Any],
    properties: Optional[Dict[str, Any]] = None,
    collection_properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Validates a polygon, constructs a FeatureCollection containing it,
    validates the FeatureCollection, and returns it as a dictionary.

    Args:
        polygon_data: Dictionary containing polygon data
        properties: Optional properties for the Feature
        collection_properties: Optional properties for the FeatureCollection

    Returns:
        Dictionary containing valid GeoJSON FeatureCollection
    """
    try:
        # Step 1: Validate the polygon using shapely
        shapely_polygon = validate_polygon(polygon_data)

        # Step 2: Create a Feature from the polygon
        feature = polygon_to_feature(shapely_polygon, properties)

        # Step 3: Create a FeatureCollection containing the Feature
        feature_collection = {"type": "FeatureCollection", "features": [feature]}

        # Step 4: Add collection properties if provided
        if collection_properties:
            # Some GeoJSON implementations support collection-level properties
            # Not in the core spec, but often used
            feature_collection.update(collection_properties)

        # Optional: Validate with geojson_pydantic to ensure compliance
        FeatureCollection.model_validate(feature_collection)

        return feature_collection

    except Exception as e:
        raise ValueError(f"GeoJSON validation failed: {str(e)}")


if __name__ == "__main__":
    # Example usage
    test_polygon = {
        "type": "Polygon",
        "coordinates": [
            [
                [-116.036096, 33.92397],
                [-116.049657, 33.91841],
                [-116.057038, 33.90572],
                [-116.057553, 33.889463],
                [-116.05721, 33.884186],
                [-116.049657, 33.883901],
                [-116.040215, 33.892031],
                [-116.033521, 33.902156],
                [-116.032319, 33.910711],
                [-116.032662, 33.917412],
                [-116.032491, 33.922687],
                [-116.036096, 33.92397],
            ]
        ],
    }

    result = polygon_to_valid_geojson(
        test_polygon, properties={"name": "Fire Boundary", "id": "fire-123"}
    )

    print(json.dumps(result, indent=2))
