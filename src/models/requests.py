from typing import List, Union, Optional
from pydantic import BaseModel, Field
from geojson_pydantic import Polygon, MultiPolygon, Feature


class ProcessingRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    coarse_geojson: Union[Polygon, MultiPolygon, Feature] = Field(
        ..., description="GeoJSON of bounding box AOI"
    )
    prefire_date_range: list[str] = Field(
        ...,
        description="Date range for prefire imagery (e.g. ['2023-01-01', '2023-12-31'])",
    )
    postfire_date_range: list[str] = Field(
        ...,
        description="Date range for postfire imagery (e.g. ['2024-01-01', '2024-12-31'])",
    )


class RefineRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    refined_geojson: Union[Polygon, MultiPolygon, Feature] = Field(
        ..., description="GeoJSON of refined AOI"
    )
    job_id: str = Field(
        ..., description="Job ID of the original fire severity analysis"
    )


class VegMapResolveRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    veg_gpkg_url: str = Field(..., description="URL to the vegetation map GeoPackage")
    fire_cog_url: str = Field(..., description="URL to the fire severity COG")
    job_id: str = Field(
        ..., description="Job ID of the original fire severity analysis"
    )
    severity_breaks: List[float] = Field(
        ...,
        description="List of classifation breaks for discrete fire severity classification (e.g. [0, .2, .4, .8])",
    )
    geojson_url: str = Field(..., description="URL to the GeoJSON of the fire boundary")
    park_unit_id: Optional[str] = Field(
        None,
        description="Park unit identifier for vegetation schema selection (e.g., 'JOTR', 'MOJN'). If not provided, auto-detection will be used.",
    )


class GeoJSONUploadRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    geojson: Union[Polygon, MultiPolygon, Feature] = Field(
        ..., description="GeoJSON data to upload"
    )
    boundary_type: str = Field(
        default="coarse", description="Type of boundary - 'coarse' or 'refined'"
    )


class HealthCheckRequest(BaseModel):
    """Request model for health check endpoint"""

    # Health check is typically a GET request with no body,
    # but we'll include this for consistency with command pattern
    pass
