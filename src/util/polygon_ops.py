import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Union

from geojson_pydantic import FeatureCollection
from shapely import Polygon as ShapelyPolygon, MultiPolygon as ShapelyMultiPolygon
from shapely import from_geojson, to_geojson
from geojson_pydantic import Polygon, MultiPolygon, Feature


def validate_polygon(
    polygon_data: Union[Polygon, MultiPolygon, Feature, Dict[str, Any]],
) -> Union[ShapelyPolygon, ShapelyMultiPolygon]:
    """
    Validates a GeoJSON Polygon or MultiPolygon using shapely.

    Args:
        polygon_data: Polygon, MultiPolygon, Feature (geojson-pydantic), or dictionary containing polygon data

    Returns:
        Validated Shapely Polygon or MultiPolygon object

    Raises:
        ValueError: If the polygon/multipolygon is invalid
    """
    try:
        polygon_json: str

        # Handle geojson-pydantic Feature objects
        if isinstance(polygon_data, Feature):
            if polygon_data.geometry is None:
                raise ValueError("Feature has no geometry")
            if polygon_data.geometry.type not in ["Polygon", "MultiPolygon"]:
                raise ValueError(
                    f"Expected Polygon or MultiPolygon geometry in Feature, got {polygon_data.geometry.type}"
                )
            polygon_json = polygon_data.geometry.model_dump_json()

        # Handle geojson-pydantic Polygon objects
        elif isinstance(polygon_data, Polygon):
            polygon_json = polygon_data.model_dump_json()

        # Handle geojson-pydantic MultiPolygon objects
        elif isinstance(polygon_data, MultiPolygon):
            polygon_json = polygon_data.model_dump_json()

        # Handle dictionary inputs (for backward compatibility)
        elif isinstance(polygon_data, dict):
            # If we have a nested geometry
            if (
                "geometry" in polygon_data
                and isinstance(polygon_data["geometry"], dict)
                and polygon_data["geometry"].get("type") in ["Polygon", "MultiPolygon"]
            ):
                polygon_json = json.dumps(polygon_data["geometry"])
            # If we have a direct geometry
            elif (
                polygon_data.get("type") in ["Polygon", "MultiPolygon"]
                and "coordinates" in polygon_data
            ):
                polygon_json = json.dumps(polygon_data)
            else:
                raise ValueError(
                    "Invalid polygon data. Expected either a Polygon or MultiPolygon geometry "
                    "or an object with a geometry property containing a Polygon or MultiPolygon"
                )
        else:
            raise ValueError(
                f"Input must be a Polygon, MultiPolygon, Feature, or dictionary, got {type(polygon_data)}"
            )

        # Parse the GeoJSON with shapely
        shapely_geom = from_geojson(polygon_json)

        # Verify it's a polygon or multipolygon
        if shapely_geom.geom_type not in ["Polygon", "MultiPolygon"]:
            raise ValueError(
                f"Expected Polygon or MultiPolygon geometry, got {shapely_geom.geom_type}"
            )

        return shapely_geom
    except Exception as e:
        raise ValueError(f"Failed to validate polygon: {str(e)}")


def polygon_to_feature(
    polygon: Union[
        ShapelyPolygon,
        ShapelyMultiPolygon,
        Polygon,
        MultiPolygon,
        Feature,
        Dict[str, Any],
    ],
    properties: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Converts a polygon or multipolygon to a GeoJSON Feature.

    Args:
        polygon: Shapely Polygon/MultiPolygon, geojson-pydantic Polygon/MultiPolygon/Feature, or dict with polygon data
        properties: Optional properties for the Feature

    Returns:
        Feature as dictionary
    """
    # Convert to Shapely object if needed
    if not isinstance(polygon, (ShapelyPolygon, ShapelyMultiPolygon)):
        polygon = validate_polygon(polygon)

    # Default properties
    if properties is None:
        properties = {
            "created": datetime.now(timezone.utc).isoformat(),
        }

    # Get GeoJSON representation of the polygon
    geojson_str = to_geojson(polygon)
    geojson_dict = json.loads(geojson_str)

    # Create feature
    feature = {"type": "Feature", "geometry": geojson_dict, "properties": properties}

    return feature


def polygon_to_valid_geojson(
    polygon_data: Union[Polygon, MultiPolygon, Feature, Dict[str, Any]],
    properties: Optional[Dict[str, Any]] = None,
    collection_properties: Optional[Dict[str, Any]] = None,
) -> FeatureCollection:
    """
    Validates a polygon or multipolygon, constructs a FeatureCollection containing it,
    validates the FeatureCollection, and returns it as a geojson-pydantic FeatureCollection.

    Args:
        polygon_data: Polygon, MultiPolygon, Feature (geojson-pydantic), or dictionary containing polygon data
        properties: Optional properties for the Feature
        collection_properties: Optional properties for the FeatureCollection

    Returns:
        Valid GeoJSON FeatureCollection (geojson-pydantic object)
    """
    try:
        # Step 1: Handle different input types
        if isinstance(polygon_data, Feature):
            # If it's already a Feature, use it directly (with optional property override)
            if properties is not None:
                # Create new feature with merged properties
                existing_properties = polygon_data.properties or {}
                merged_properties = {**existing_properties, **properties}
                if polygon_data.geometry is None:
                    raise ValueError("Feature has no geometry")
                feature_dict = {
                    "type": "Feature",
                    "geometry": polygon_data.geometry.model_dump(),
                    "properties": merged_properties,
                }
            else:
                feature_dict = polygon_data.model_dump()
        else:
            # For Polygon or dict inputs, validate and create feature
            shapely_polygon = validate_polygon(polygon_data)
            feature_dict = polygon_to_feature(shapely_polygon, properties)

        # Step 2: Create FeatureCollection
        feature_collection_dict = {
            "type": "FeatureCollection",
            "features": [feature_dict],
        }

        # Step 3: Add collection properties if provided
        if collection_properties:
            # Some GeoJSON implementations support collection-level properties
            # Not in the core spec, but often used
            feature_collection_dict.update(collection_properties)

        # Step 4: Validate and return as geojson-pydantic FeatureCollection
        return FeatureCollection.model_validate(feature_collection_dict)

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

    print(json.dumps(result.model_dump(), indent=2))
