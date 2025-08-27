"""
TypedDict definitions for fire recovery backend data structures.

This module provides precise type definitions for dictionary-based data structures
used throughout the fire recovery processing pipeline. TypedDicts improve type safety,
IDE support, and help resolve mypy errors while maintaining runtime compatibility
with regular dictionaries.

Design Principles:
- Single source of truth for data structure types
- Backward compatible with existing Dict[str, Any] usage
- Follows existing naming conventions from the codebase
- Supports both required and optional fields where appropriate
"""

from typing import Any, Union
from typing_extensions import TypedDict, Literal, NotRequired
import xarray as xr
from geojson_pydantic import Polygon, Feature, FeatureCollection


# =============================================================================
# GeoJSON and Geometry Types
# =============================================================================
# Custom GeoJSON types have been replaced with geojson-pydantic imports:
# - Polygon: Raw polygon geometry
# - Feature: GeoJSON Feature with geometry and properties  
# - FeatureCollection: GeoJSON FeatureCollection with multiple features


# =============================================================================
# Satellite Data Processing Types
# =============================================================================


class STACDataPayload(TypedDict):
    """
    Data structure returned from satellite data fetching operations.

    Resolves mypy errors in:
    - src/commands/impl/fire_severity_command.py:233-234
    - Tests expecting specific data array vs string types
    """

    prefire_data: xr.DataArray
    postfire_data: xr.DataArray
    nir_band: str
    swir_band: str


class BandConfiguration(TypedDict):
    """Band mapping configuration for spectral index calculations"""

    nir: str  # Near-infrared band identifier (e.g., "B08")
    swir: str  # Shortwave-infrared band identifier (e.g., "B12")


class SpectralIndicesResult(TypedDict):
    """Results from spectral indices calculations"""

    prefire_nbr: xr.DataArray
    postfire_nbr: xr.DataArray
    dnbr: xr.DataArray
    rdnbr: xr.DataArray
    rbr: xr.DataArray


# =============================================================================
# COG (Cloud Optimized GeoTIFF) Types
# =============================================================================


class COGUrls(TypedDict):
    """
    URLs to fire severity COG files.

    Used in API responses and STAC item creation.
    Matches the structure in models/responses.py
    """

    prefire_nbr: NotRequired[str]  # Optional - not always generated
    postfire_nbr: NotRequired[str]  # Optional - not always generated
    dnbr: str
    rdnbr: str
    rbr: str


class COGProcessingResult(TypedDict):
    """Result of COG processing operations"""

    cog_url: str
    file_size_bytes: int
    crs: str
    bounds: tuple[float, float, float, float]  # (minx, miny, maxx, maxy)


# =============================================================================
# STAC (SpatioTemporal Asset Catalog) Types
# =============================================================================


class FireSeveritySTACItem(TypedDict):
    """
    Parameters for creating fire severity STAC items.

    Resolves mypy errors in:
    - src/commands/impl/fire_severity_command.py:360 (parameter unpacking)
    - src/stac/stac_json_manager.py:38-47 (parameter type mismatches)
    """

    fire_event_name: str
    job_id: str
    cog_urls: dict[str, str]  # Flexible for different COG combinations
    geometry: Polygon | Feature  # Use geojson-pydantic types
    datetime_str: str
    boundary_type: NotRequired[str]  # Optional, defaults to "coarse"
    skip_validation: NotRequired[bool]  # Optional, defaults to False


class BoundarySTACItem(TypedDict):
    """Parameters for creating boundary STAC items"""

    fire_event_name: str
    job_id: str
    boundary_url: str
    geometry: dict[str, Any]
    boundary_type: Literal["coarse", "refined"]
    datetime_str: str
    skip_validation: NotRequired[bool]


class VegetationMatrixSTACItem(TypedDict):
    """Parameters for creating vegetation analysis STAC items"""

    fire_event_name: str
    job_id: str
    csv_url: str
    json_url: str
    geometry: dict[str, Any]
    datetime_str: str
    skip_validation: NotRequired[bool]


# =============================================================================
# Command Result Types
# =============================================================================


class FireSeverityCommandResult(TypedDict):
    """
    Result data from fire severity analysis command.

    Used in command results and API responses.
    """

    cog_urls: COGUrls
    stac_item_url: str
    job_id: str
    fire_event_name: str
    processing_metadata: dict[str, Any]


class BoundaryRefinementResult(TypedDict):
    """Result data from boundary refinement operations"""

    refined_cog_urls: COGUrls
    refined_boundary_url: str
    stac_item_url: str
    job_id: str
    fire_event_name: str


class VegetationAnalysisResult(TypedDict):
    """Result data from vegetation impact analysis"""

    csv_url: str
    json_url: str
    stac_item_url: str
    job_id: str
    fire_event_name: str
    analysis_metadata: dict[str, Any]


# =============================================================================
# Health Check Types
# =============================================================================


class ComponentHealthCheck(TypedDict):
    """Individual component health check result"""

    status: Literal["healthy", "unhealthy", "degraded"]
    response_time_ms: NotRequired[float]
    details: NotRequired[dict[str, Any]]
    error_message: NotRequired[str]


class HealthCheckData(TypedDict):
    """
    Complete health check response data.

    Resolves mypy errors in:
    - tests/api/test_health_endpoint.py:193 (missing fields)
    - src/commands/impl/health_check_command.py (response structure)
    """

    overall_status: Literal["healthy", "unhealthy", "degraded"]
    timestamp: float
    checks: dict[str, ComponentHealthCheck]
    unhealthy_components: int


# =============================================================================
# Upload and File Processing Types
# =============================================================================


class UploadResult(TypedDict):
    """Result from file upload operations"""

    uploaded_url: str
    file_size_bytes: int
    content_type: str
    filename: str


class ShapefileUploadResult(TypedDict):
    """Result from shapefile upload and processing"""

    shapefile_url: str
    extracted_geojson: Polygon | Feature
    boundary_stac_item_url: str
    job_id: str


# =============================================================================
# Utility Types
# =============================================================================


class BufferedBounds(TypedDict):
    """Buffered bounding box coordinates"""

    minx: float
    miny: float
    maxx: float
    maxy: float
    buffer_meters: float


class ProcessingMetadata(TypedDict):
    """Metadata about processing operations"""

    processing_time_seconds: float
    input_data_sources: list[str]
    algorithm_version: str
    parameters_used: dict[str, Any]
    warnings: NotRequired[list[str]]
    errors: NotRequired[list[str]]


# =============================================================================
# Type Aliases for Common Patterns
# =============================================================================

# Common geometry representations used across the codebase
GeometryInput = Union[Polygon, Feature, dict[str, Any]]

# Date range format used in API requests
DateRange = list[str]  # ["YYYY-MM-DD", "YYYY-MM-DD"] format

# Generic URL mapping for various asset types
AssetUrls = dict[str, str]

# STAC endpoint configuration
STACConfig = dict[str, Any]
