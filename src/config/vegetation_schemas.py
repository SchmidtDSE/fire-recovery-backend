"""
Vegetation GeoPackage schema configurations for different park units.

This module defines the schema mappings for vegetation data from different parks,
allowing the VegetationResolveCommand to handle varying GeoPackage structures
without hardcoding field names.
"""

from dataclasses import dataclass
from typing import Dict, Optional, TYPE_CHECKING

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


# Park-specific schema configurations
VEGETATION_SCHEMAS: Dict[str, VegetationSchema] = {
    "JOTR": VegetationSchema(
        layer_name="JOTR_VegPolys",
        vegetation_type_field="MapUnit_Name",
        description_field="MapUnit_Name",  # Using same field for now
        geometry_column="geometry",
        preserve_fields=["OBJECTID", "Shape_Area", "Shape_Length"]
    ),
    "MOJN": VegetationSchema(
        layer_name=None,  # Uses default layer
        vegetation_type_field="MAP_DESC",
        description_field="MAP_DESC",
        geometry_column="geometry",
        preserve_fields=["FID", "AREA", "PERIMETER"]
    ),
    # Add more park schemas as needed
    "DEFAULT": VegetationSchema(
        # Fallback schema for unknown parks
        layer_name=None,
        vegetation_type_field="veg_type",
        description_field=None,
        geometry_column="geometry",
        preserve_fields=None
    )
}


def detect_vegetation_schema(gdf: "gpd.GeoDataFrame", park_code: Optional[str] = None) -> VegetationSchema:
    """
    Detect the appropriate vegetation schema based on GeoDataFrame columns and park code.
    
    Args:
        gdf: GeoDataFrame to analyze
        park_code: Optional park code hint (e.g., "JOTR", "MOJN")
        
    Returns:
        Appropriate VegetationSchema configuration
    """
    import geopandas as gpd
    
    # If park code provided and known, use it
    if park_code and park_code in VEGETATION_SCHEMAS:
        return VEGETATION_SCHEMAS[park_code]
    
    # Try to auto-detect based on column names
    columns = set(gdf.columns)
    
    # JOTR detection
    if "MapUnit_Name" in columns:
        return VEGETATION_SCHEMAS["JOTR"]
    
    # MOJN detection  
    if "MAP_DESC" in columns:
        return VEGETATION_SCHEMAS["MOJN"]
    
    # Check for common vegetation field names
    common_veg_fields = ["veg_type", "vegetation", "VEG_TYPE", "VEGETATION", 
                         "veg_class", "VEG_CLASS", "class", "CLASS"]
    for field in common_veg_fields:
        if field in columns:
            return VegetationSchema(
                vegetation_type_field=field,
                geometry_column=gdf.geometry.name if hasattr(gdf, 'geometry') else "geometry"
            )
    
    # Return default schema
    return VEGETATION_SCHEMAS["DEFAULT"]