from typing import List, Union, Optional
from pydantic import BaseModel, ConfigDict, Field
from geojson_pydantic import Polygon, MultiPolygon, Feature


class ProcessingRequest(BaseModel):
    """Request model for fire severity analysis."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "coarse_geojson": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-116.9, 33.8],
                            [-116.7, 33.8],
                            [-116.7, 34.0],
                            [-116.9, 34.0],
                            [-116.9, 33.8],
                        ]
                    ],
                },
                "prefire_date_range": ["2024-01-01", "2024-01-15"],
                "postfire_date_range": ["2024-09-15", "2024-09-30"],
            }
        }
    )

    fire_event_name: str = Field(..., description="Name of the fire event")
    coarse_geojson: Union[Polygon, MultiPolygon, Feature] = Field(
        ...,
        description="GeoJSON of bounding box AOI (Polygon, MultiPolygon, or Feature)",
    )
    prefire_date_range: list[str] = Field(
        ...,
        description="Date range for prefire imagery [start, end] (2-3 weeks before ignition)",
    )
    postfire_date_range: list[str] = Field(
        ...,
        description="Date range for postfire imagery [start, end] (2-3 weeks after containment)",
    )


class RefineRequest(BaseModel):
    """Request model for boundary refinement."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "refined_geojson": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-116.85, 33.85],
                            [-116.75, 33.85],
                            [-116.75, 33.95],
                            [-116.85, 33.95],
                            [-116.85, 33.85],
                        ]
                    ],
                },
            }
        }
    )

    fire_event_name: str = Field(..., description="Name of the fire event")
    refined_geojson: Union[Polygon, MultiPolygon, Feature] = Field(
        ...,
        description="User-drawn refined boundary (Polygon, MultiPolygon, or Feature)",
    )
    job_id: str = Field(
        ..., description="Job ID from the original fire severity analysis"
    )


class VegMapResolveRequest(BaseModel):
    """Request model for vegetation impact analysis."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "veg_gpkg_url": "https://storage.googleapis.com/nps-veg-data/JOTR_vegetation.gpkg",
                "fire_cog_url": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../refined_rbr.tif",
                "geojson_url": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../refined_boundary.geojson",
                "severity_breaks": [0.1, 0.27, 0.44, 0.66],
                "park_unit_id": "JOTR",
            }
        }
    )

    fire_event_name: str = Field(..., description="Name of the fire event")
    veg_gpkg_url: str = Field(
        ..., description="URL to the NPS vegetation map GeoPackage"
    )
    fire_cog_url: str = Field(
        ..., description="URL to the fire severity COG (typically RBR for best results)"
    )
    job_id: str = Field(..., description="Job ID from the fire severity analysis")
    severity_breaks: List[float] = Field(
        ...,
        description="Classification thresholds for severity categories (e.g., [0.1, 0.27, 0.44, 0.66] for unburned/low/moderate/high/very-high)",
    )
    geojson_url: str = Field(..., description="URL to the fire boundary GeoJSON")
    park_unit_id: Optional[str] = Field(
        None,
        description="Park unit code for vegetation schema (e.g., 'JOTR', 'MOJN'). Auto-detected if omitted.",
    )


class GeoJSONUploadRequest(BaseModel):
    """Request model for GeoJSON boundary upload."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "geojson": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-116.9, 33.8],
                            [-116.7, 33.8],
                            [-116.7, 34.0],
                            [-116.9, 34.0],
                            [-116.9, 33.8],
                        ]
                    ],
                },
                "boundary_type": "refined",
            }
        }
    )

    fire_event_name: str = Field(..., description="Name of the fire event")
    geojson: Union[Polygon, MultiPolygon, Feature] = Field(
        ..., description="GeoJSON boundary (Polygon, MultiPolygon, or Feature)"
    )
    boundary_type: str = Field(
        default="coarse", description="Boundary type: 'coarse' or 'refined'"
    )


class HealthCheckRequest(BaseModel):
    """Request model for health check endpoint"""

    # Health check is typically a GET request with no body,
    # but we'll include this for consistency with command pattern
    pass
