"""
Vegetation GeoPackage schema configurations for different park units.

This module defines the schema mappings for vegetation data from different parks,
allowing the VegetationResolveCommand to handle varying GeoPackage structures
without hardcoding field names.
"""

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import geopandas as gpd


@dataclass
class VegetationSchema:
    """Schema configuration for a vegetation GeoPackage."""

    # Layer name in the GeoPackage (None means use default/first layer)
    layer_name: Optional[str] = None

    # Field that contains vegetation type/class name
    vegetation_type_field: str = "veg_type"

    # Field that contains vegetation description (optional)
    description_field: Optional[str] = None

    # Field that contains vegetation color (optional, for visualization)
    color_field: Optional[str] = None

    # Geometry column name (usually 'geometry' but can vary)
    geometry_column: str = "geometry"

    # Additional metadata fields to preserve
    preserve_fields: Optional[list[str]] = None


# Note: Park-specific schema configurations are now loaded from JSON
# via VegetationSchemaLoader to maintain single source of truth


def detect_vegetation_schema(
    gdf: "gpd.GeoDataFrame", park_code: Optional[str] = None
) -> VegetationSchema:
    """
    Detect the appropriate vegetation schema based on GeoDataFrame columns and park code.

    Args:
        gdf: GeoDataFrame to analyze
        park_code: Optional park code hint (e.g., "JOTR", "MOJN")

    Returns:
        Appropriate VegetationSchema configuration
    """
    # Import here to avoid circular imports
    from src.config.vegetation_schema_loader import VegetationSchemaLoader

    loader = VegetationSchemaLoader.get_instance()

    # If park code provided and known, use it
    if park_code and loader.has_schema(park_code):
        return loader.get_schema(park_code)

    # Try to auto-detect based on column names
    columns = set(gdf.columns)

    # JOTR detection
    if "MapUnit_Name" in columns:
        if loader.has_schema("JOTR"):
            return loader.get_schema("JOTR")

    # MOJN detection
    if "MAP_DESC" in columns:
        if loader.has_schema("MOJN"):
            return loader.get_schema("MOJN")

    # Check for common vegetation field names
    common_veg_fields = [
        "veg_type",
        "vegetation",
        "VEG_TYPE",
        "VEGETATION",
        "veg_class",
        "VEG_CLASS",
        "class",
        "CLASS",
    ]
    for field in common_veg_fields:
        if field in columns:
            return VegetationSchema(
                vegetation_type_field=field,
                geometry_column=gdf.geometry.name
                if hasattr(gdf, "geometry")
                else "geometry",
            )

    # Return default schema if available, otherwise create minimal schema
    try:
        return loader.get_default_schema()
    except ValueError:
        # If no default schema is configured, create a minimal one
        return VegetationSchema(
            vegetation_type_field="veg_type",
            geometry_column=gdf.geometry.name if hasattr(gdf, "geometry") else "geometry"
        )
