from typing import Dict, Optional, Any
from pydantic import BaseModel, Field


class BaseResponse(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    status: str
    job_id: str


class TaskPendingResponse(BaseResponse):
    """Response for when a task is still being processed"""

    pass


class ProcessingStartedResponse(BaseResponse):
    pass


class RefinedBoundaryResponse(BaseResponse):
    refined_boundary_geojson_url: str
    refined_severity_cog_urls: Dict[str, str] = Field(
        ..., description="URLs to the refined COGs for each metric"
    )


class FireSeverityResponse(BaseResponse):
    coarse_severity_cog_urls: Dict[str, str] = Field(
        ..., description="URLs to the COGs for each metric"
    )


class VegMapMatrixResponse(BaseResponse):
    fire_veg_matrix_csv_url: Optional[str] = None
    fire_veg_matrix_json_url: Optional[str] = None


class UploadedGeoJSONResponse(BaseResponse):
    refined_boundary_geojson_url: str
    boundary_type: str = Field(..., description="Type of boundary (coarse or refined)")


class UploadedShapefileZipResponse(BaseResponse):
    shapefile_url: str = Field(..., description="URL to the uploaded shapefile zip")
    boundary_geojson_url: str = Field(
        ..., description="URL to extracted boundary GeoJSON"
    )
    boundary_type: str = Field(..., description="Type of boundary (coarse or refined)")


class HealthCheckResponse(BaseResponse):
    """Response model for health check endpoint"""

    overall_status: str = Field(..., description="Overall system health status")
    timestamp: float = Field(..., description="Unix timestamp of the health check")
    checks: Dict[str, Dict[str, Any]] = Field(
        ..., description="Individual component health checks"
    )
    unhealthy_components: int = Field(..., description="Number of unhealthy components")
