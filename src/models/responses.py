from typing import Dict, Optional, Any
from pydantic import BaseModel, ConfigDict, Field


class BaseResponse(BaseModel):
    """Base response model for all API responses."""

    fire_event_name: str = Field(..., description="Name of the fire event")
    status: str = Field(..., description="Status of the operation")
    job_id: str = Field(..., description="Unique job identifier for tracking")


class TaskPendingResponse(BaseResponse):
    """Response for when a task is still being processed."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "status": "pending",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
            }
        }
    )


class ProcessingStartedResponse(BaseResponse):
    """Response when async processing has been initiated."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "status": "Processing started",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
            }
        }
    )


class RefinedBoundaryResponse(BaseResponse):
    """Response containing refined boundary and cropped severity COGs."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "status": "complete",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "refined_boundary_geojson_url": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../refined_boundary.geojson",
                "refined_severity_cog_urls": {
                    "dnbr": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../refined_dnbr.tif",
                    "rdnbr": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../refined_rdnbr.tif",
                    "rbr": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../refined_rbr.tif",
                },
            }
        }
    )

    refined_boundary_geojson_url: str = Field(
        ..., description="URL to the refined boundary GeoJSON"
    )
    refined_severity_cog_urls: Dict[str, str] = Field(
        ..., description="URLs to the refined COGs for each severity metric"
    )


class FireSeverityResponse(BaseResponse):
    """Response containing fire severity analysis results."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "status": "complete",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "coarse_severity_cog_urls": {
                    "prefire_nbr": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../prefire_nbr.tif",
                    "postfire_nbr": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../postfire_nbr.tif",
                    "dnbr": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../dnbr.tif",
                    "rdnbr": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../rdnbr.tif",
                    "rbr": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../rbr.tif",
                },
            }
        }
    )

    coarse_severity_cog_urls: Dict[str, str] = Field(
        ..., description="URLs to the COGs for each severity metric (prefire_nbr, postfire_nbr, dnbr, rdnbr, rbr)"
    )


class VegMapMatrixResponse(BaseResponse):
    """Response containing vegetation impact analysis results."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "status": "complete",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "fire_veg_matrix_csv_url": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../veg_matrix.csv",
                "fire_veg_matrix_json_url": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../veg_matrix.json",
            }
        }
    )

    fire_veg_matrix_csv_url: Optional[str] = Field(
        None, description="URL to vegetation impact matrix CSV"
    )
    fire_veg_matrix_json_url: Optional[str] = Field(
        None, description="URL to vegetation impact matrix JSON (for dashboard visualization)"
    )


class UploadedGeoJSONResponse(BaseResponse):
    """Response for successful GeoJSON upload."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "status": "complete",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "refined_boundary_geojson_url": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../boundary.geojson",
                "boundary_type": "refined",
            }
        }
    )

    refined_boundary_geojson_url: str = Field(
        ..., description="URL to the uploaded boundary GeoJSON"
    )
    boundary_type: str = Field(..., description="Type of boundary ('coarse' or 'refined')")


class UploadedShapefileZipResponse(BaseResponse):
    """Response for successful shapefile upload."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Bridge_Fire_2024",
                "status": "complete",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "shapefile_url": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../boundary.zip",
                "boundary_geojson_url": "https://storage.googleapis.com/fire-recovery-temp/Bridge_Fire_2024/550e8400.../boundary.geojson",
                "boundary_type": "refined",
            }
        }
    )

    shapefile_url: str = Field(..., description="URL to the uploaded shapefile zip")
    boundary_geojson_url: str = Field(
        ..., description="URL to the extracted boundary GeoJSON"
    )
    boundary_type: str = Field(..., description="Type of boundary ('coarse' or 'refined')")


class HealthCheckResponse(BaseResponse):
    """Response model for health check endpoint."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "health-check",
                "status": "healthy",
                "job_id": "550e8400-e29b-41d4-a716-446655440000",
                "overall_status": "healthy",
                "timestamp": 1706486400.123,
                "checks": {
                    "storage": {"status": "healthy", "latency_ms": 45},
                    "stac": {"status": "healthy", "latency_ms": 12},
                },
                "unhealthy_components": 0,
            }
        }
    )

    overall_status: str = Field(..., description="Overall system health status ('healthy' or 'unhealthy')")
    timestamp: float = Field(..., description="Unix timestamp of the health check")
    checks: Dict[str, Dict[str, Any]] = Field(
        ..., description="Individual component health check results"
    )
    unhealthy_components: int = Field(..., description="Count of unhealthy components")
