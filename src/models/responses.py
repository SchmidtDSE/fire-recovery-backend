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
                "fire_event_name": "Geology_Fire",
                "status": "pending",
                "job_id": "223c86f1-377f-4640-ba88-ced1277f3831",
            }
        }
    )


class TaskFailedResponse(BaseResponse):
    """Response when a job has failed."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Geology_Fire",
                "status": "failed",
                "job_id": "223c86f1-377f-4640-ba88-ced1277f3831",
                "error_message": "Failed to retrieve satellite imagery: no scenes found for date range",
                "error_details": {
                    "stage": "satellite_data_retrieval",
                    "exception_type": "NoScenesFoundError",
                    "inputs": {
                        "prefire_date_range": ["2023-01-01", "2023-01-05"],
                        "postfire_date_range": ["2023-01-20", "2023-01-25"],
                    },
                },
                "execution_time_ms": 1234.56,
                "command_name": "fire_severity_analysis",
            }
        }
    )

    error_message: str = Field(..., description="Human-readable error description")
    error_details: Optional[Dict[str, Any]] = Field(
        None, description="Detailed error information including stage, exception type, and inputs"
    )
    execution_time_ms: Optional[float] = Field(
        None, description="Time taken before failure in milliseconds"
    )
    command_name: Optional[str] = Field(
        None, description="Name of the command that failed"
    )


class ProcessingStartedResponse(BaseResponse):
    """Response when async processing has been initiated."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Geology_Fire",
                "status": "Processing started",
                "job_id": "223c86f1-377f-4640-ba88-ced1277f3831",
            }
        }
    )


class RefinedBoundaryResponse(BaseResponse):
    """Response containing refined boundary and cropped severity COGs."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Geology_Fire",
                "status": "complete",
                "job_id": "223c86f1-377f-4640-ba88-ced1277f3831",
                "refined_boundary_geojson_url": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/boundary/refined_boundary.geojson",
                "refined_severity_cog_urls": {
                    "rbr": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/fire_severity/refined_rbr.tif",
                    "dnbr": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/fire_severity/refined_dnbr.tif",
                    "rdnbr": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/fire_severity/refined_rdnbr.tif",
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
                "fire_event_name": "Geology_Fire",
                "status": "complete",
                "job_id": "223c86f1-377f-4640-ba88-ced1277f3831",
                "coarse_severity_cog_urls": {
                    "rbr": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/fire_severity/rbr.tif",
                    "dnbr": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/fire_severity/dnbr.tif",
                    "rdnbr": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/fire_severity/rdnbr.tif",
                },
            }
        }
    )

    coarse_severity_cog_urls: Dict[str, str] = Field(
        ..., description="URLs to the COGs for each severity metric (dnbr, rdnbr, rbr)"
    )


class VegMapMatrixResponse(BaseResponse):
    """Response containing vegetation impact analysis results."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Geology_Fire",
                "status": "complete",
                "job_id": "223c86f1-377f-4640-ba88-ced1277f3831",
                "fire_veg_matrix_csv_url": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/veg_analysis/fire_veg_matrix.csv",
                "fire_veg_matrix_json_url": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/veg_analysis/fire_veg_matrix.json",
            }
        }
    )

    fire_veg_matrix_csv_url: Optional[str] = Field(
        None, description="URL to vegetation impact matrix CSV"
    )
    fire_veg_matrix_json_url: Optional[str] = Field(
        None,
        description="URL to vegetation impact matrix JSON (for dashboard visualization)",
    )


class UploadedGeoJSONResponse(BaseResponse):
    """Response for successful GeoJSON upload."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Geology_Fire",
                "status": "complete",
                "job_id": "223c86f1-377f-4640-ba88-ced1277f3831",
                "refined_boundary_geojson_url": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/boundary/refined_boundary.geojson",
                "boundary_type": "refined",
            }
        }
    )

    refined_boundary_geojson_url: str = Field(
        ..., description="URL to the uploaded boundary GeoJSON"
    )
    boundary_type: str = Field(
        ..., description="Type of boundary ('coarse' or 'refined')"
    )


class UploadedShapefileZipResponse(BaseResponse):
    """Response for successful shapefile upload."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "fire_event_name": "Geology_Fire",
                "status": "complete",
                "job_id": "223c86f1-377f-4640-ba88-ced1277f3831",
                "shapefile_url": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/boundary/boundary.zip",
                "boundary_geojson_url": "https://storage.googleapis.com/fire-recovery-store/assets/223c86f1-377f-4640-ba88-ced1277f3831/boundary/boundary.geojson",
                "boundary_type": "refined",
            }
        }
    )

    shapefile_url: str = Field(..., description="URL to the uploaded shapefile zip")
    boundary_geojson_url: str = Field(
        ..., description="URL to the extracted boundary GeoJSON"
    )
    boundary_type: str = Field(
        ..., description="Type of boundary ('coarse' or 'refined')"
    )


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

    overall_status: str = Field(
        ..., description="Overall system health status ('healthy' or 'unhealthy')"
    )
    timestamp: float = Field(..., description="Unix timestamp of the health check")
    checks: Dict[str, Dict[str, Any]] = Field(
        ..., description="Individual component health check results"
    )
    unhealthy_components: int = Field(..., description="Count of unhealthy components")
