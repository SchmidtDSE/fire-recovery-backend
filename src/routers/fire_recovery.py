import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from fastapi import (
    APIRouter,
    BackgroundTasks,
    Depends,
    File,
    Form,
    HTTPException,
    UploadFile,
)
from geojson_pydantic import Polygon, MultiPolygon, Feature

from src.config.constants import FINAL_BUCKET_NAME
from src.stac.stac_json_manager import STACJSONManager
from src.stac.stac_catalog_manager import STACCatalogManager
from src.util.boundary_utils import process_and_upload_geojson
from src.commands.impl.upload_aoi_command import UploadAOICommand
from src.commands.impl.health_check_command import HealthCheckCommand
from src.commands.impl.fire_severity_command import FireSeverityAnalysisCommand
from src.commands.impl.boundary_refinement_command import BoundaryRefinementCommand
from src.commands.impl.vegetation_resolve_command import VegetationResolveCommand
from src.commands.interfaces.command_context import CommandContext
from src.core.storage.storage_factory import StorageFactory
from src.computation.registry.index_registry import IndexRegistry
from src.models.requests import (
    ProcessingRequest,
    RefineRequest,
    VegMapResolveRequest,
    GeoJSONUploadRequest,
)
from src.models.responses import (
    TaskPendingResponse,
    ProcessingStartedResponse,
    RefinedBoundaryResponse,
    FireSeverityResponse,
    VegMapMatrixResponse,
    UploadedGeoJSONResponse,
    UploadedShapefileZipResponse,
    HealthCheckResponse,
)


def convert_geometry_to_pydantic(
    geometry: dict[str, Any],
) -> Polygon | MultiPolygon | Feature:
    """Convert dict geometry to geojson-pydantic type"""
    if geometry.get("type") == "Feature":
        return Feature.model_validate(geometry)
    elif geometry.get("type") == "MultiPolygon":
        return MultiPolygon.model_validate(geometry)
    else:
        return Polygon.model_validate(geometry)


# Dictionary to track when job requests were first received
job_timestamps = {}

# Initialize router
router = APIRouter(
    prefix="/fire-recovery",
    tags=["Fire Recovery"],
    responses={404: {"description": "Not found"}},
)


# Dependency functions
def get_storage_factory() -> StorageFactory:
    """Get storage factory instance with lifecycle-based configuration"""
    from src.config.constants import FINAL_BUCKET_NAME

    # Get S3-compatible credentials from environment
    # Only S3_* variables are supported
    access_key = os.environ.get("S3_ACCESS_KEY_ID")
    secret_key = os.environ.get("S3_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        raise ValueError(
            "S3 credentials are required for persistent storage. "
            "Set S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY environment variables."
        )

    # Allow test environment to override bucket and endpoint
    # Only S3_* variables are supported
    bucket_name = os.environ.get("S3_BUCKET") or FINAL_BUCKET_NAME
    endpoint = os.environ.get("S3_ENDPOINT") or "storage.googleapis.com"
    secure_str = os.environ.get("S3_SECURE") or "True"
    secure = secure_str.lower() == "true"
    protocol = "https" if secure else "http"
    base_url = f"{protocol}://{endpoint}/{bucket_name}"

    # Configure storage based on lifecycle semantics:
    # - temp_storage: Memory for ephemeral processing files (fast, auto-cleanup)
    # - final_storage: MinIO for persistent assets that outlive the request
    return StorageFactory(
        temp_storage_type="memory",
        temp_storage_config={"base_url": "memory://temp/"},
        final_storage_type="minio",
        final_storage_config={
            "bucket_name": bucket_name,
            "endpoint": endpoint,
            "access_key": access_key,
            "secret_key": secret_key,
            "region": "auto",
            "secure": secure,
            "base_url": base_url,
        },
    )


async def get_stac_manager(
    storage_factory: StorageFactory = Depends(get_storage_factory),
) -> STACJSONManager:
    """Get STAC JSON manager instance and ensure catalog exists"""
    final_storage = storage_factory.get_final_storage()

    # Use environment-aware base URL for STAC (supports test overrides)
    # Only S3_* variables are supported
    bucket_name = os.environ.get("S3_BUCKET") or FINAL_BUCKET_NAME
    endpoint = os.environ.get("S3_ENDPOINT") or "storage.googleapis.com"
    secure_str = os.environ.get("S3_SECURE") or "True"
    secure = secure_str.lower() == "true"
    protocol = "https" if secure else "http"
    stac_base_url = f"{protocol}://{endpoint}/{bucket_name}/stac"

    stac_manager = STACJSONManager.for_production(
        base_url=stac_base_url,
        storage=final_storage,
    )

    # Ensure catalog exists by initializing it
    catalog_manager = STACCatalogManager.for_production(
        base_url=stac_base_url,
        storage=final_storage,
    )

    # Check if catalog exists, if not initialize it
    __root_catalog = await catalog_manager.get_catalog()
    if __root_catalog is None:
        # Catalog doesn't exist, initialize it
        await catalog_manager.initialize_catalog()

    return stac_manager


def get_index_registry() -> IndexRegistry:
    """Get index registry instance"""
    return IndexRegistry()


@router.get("/", tags=["Root"])
async def root() -> Dict[str, str]:
    return {"message": "Welcome to the Fire Recovery Backend API"}


@router.get("/healthz", response_model=HealthCheckResponse, tags=["Health"])
async def health_check(
    stac_manager: STACJSONManager = Depends(get_stac_manager),
    storage_factory: StorageFactory = Depends(get_storage_factory),
    index_registry: IndexRegistry = Depends(get_index_registry),
) -> HealthCheckResponse:
    """
    System health check endpoint.

    Verifies connectivity to storage, STAC catalog, and other dependencies.

    See [docs/ENDPOINTS.md#get-healthz](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#get-healthz) for details.
    """
    # Generate a unique job ID for the health check
    job_id = str(uuid.uuid4())

    try:
        # Create command context for health check
        # Note: We use minimal geometry for health checks
        context = CommandContext(
            job_id=job_id,
            fire_event_name="health-check",
            geometry=convert_geometry_to_pydantic(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                }
            ),
            storage=storage_factory.get_temp_storage(),
            storage_factory=storage_factory,
            stac_manager=stac_manager,
            index_registry=index_registry,
            metadata={"check_type": "health"},
        )

        # Execute health check command
        command = HealthCheckCommand()
        result = await command.execute(context)

        if result.is_failure():
            raise HTTPException(
                status_code=503,  # Service Unavailable
                detail=f"Health check failed: {result.error_message}",
            )

        # Extract health data from command result
        if not result.data:
            raise HTTPException(
                status_code=500,
                detail="Health check succeeded but no health data returned",
            )

        # Map command result to response model
        return HealthCheckResponse(
            fire_event_name="health-check",
            status="healthy"
            if result.data["overall_status"] == "healthy"
            else "unhealthy",
            job_id=job_id,
            overall_status=result.data["overall_status"],
            timestamp=result.data["timestamp"],
            checks=result.data["checks"],
            unhealthy_components=result.data["unhealthy_components"],
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check error: {str(e)}")


@router.post(
    "/process/analyze_fire_severity",
    response_model=ProcessingStartedResponse,
    tags=["Fire Severity"],
)
async def analyze_fire_severity(
    request: ProcessingRequest,
    background_tasks: BackgroundTasks,
    stac_manager: STACJSONManager = Depends(get_stac_manager),
    storage_factory: StorageFactory = Depends(get_storage_factory),
    index_registry: IndexRegistry = Depends(get_index_registry),
) -> ProcessingStartedResponse:
    """
    Initiate fire severity analysis using Sentinel-2 satellite imagery.

    Calculates NBR, dNBR, RdNBR, and RBR indices. Returns immediately with a job ID for polling.

    See [docs/ENDPOINTS.md#post-processanalyze_fire_severity](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#post-processanalyze_fire_severity) for details.
    """
    job_id = str(uuid.uuid4())
    job_timestamps[job_id] = time.time()

    # Start the processing task in the background
    background_tasks.add_task(
        process_fire_severity,
        job_id=job_id,
        fire_event_name=request.fire_event_name,
        geometry=request.coarse_geojson,
        prefire_date_range=request.prefire_date_range,
        postfire_date_range=request.postfire_date_range,
        stac_manager=stac_manager,
        storage_factory=storage_factory,
        index_registry=index_registry,
    )

    return ProcessingStartedResponse(
        fire_event_name=request.fire_event_name,
        status="Processing started",
        job_id=job_id,
    )


async def process_fire_severity(
    job_id: str,
    fire_event_name: str,
    geometry: Polygon | MultiPolygon | Feature,
    prefire_date_range: list[str],
    postfire_date_range: list[str],
    stac_manager: STACJSONManager,
    storage_factory: StorageFactory,
    index_registry: IndexRegistry,
) -> None:
    try:
        # Create command context for fire severity analysis
        context = CommandContext(
            job_id=job_id,
            fire_event_name=fire_event_name,
            geometry=geometry,
            storage=storage_factory.get_final_storage(),
            storage_factory=storage_factory,
            stac_manager=stac_manager,
            index_registry=index_registry,
            computation_config={
                "prefire_date_range": prefire_date_range,
                "postfire_date_range": postfire_date_range,
                "collection": "sentinel-2-l2a",
                "buffer_meters": 100,
                "indices": ["dnbr", "rdnbr", "rbr"],
            },
        )

        # Execute the fire severity analysis command
        command = FireSeverityAnalysisCommand()
        result = await command.execute(context)

        if result.is_failure():
            raise HTTPException(
                status_code=500,
                detail=f"Error processing fire severity: {result.error_message}",
            )

        # Process and upload the boundary GeoJSON
        boundary_geojson_url, _, bbox = await process_and_upload_geojson(
            geometry=geometry,
            fire_event_name=fire_event_name,
            job_id=job_id,
            filename="coarse_boundary",
            storage_factory=storage_factory,
        )

        # Create a STAC item for the coarse boundary
        datetime_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        await stac_manager.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            boundary_geojson_url=boundary_geojson_url,
            bbox=bbox,
            datetime_str=datetime_str,
            boundary_type="coarse",
        )

    except Exception as e:
        # Log error with proper logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error processing fire severity: {str(e)}")
        # Set job status to failed


@router.get(
    "/result/analyze_fire_severity/{fire_event_name}/{job_id}",
    response_model=Union[TaskPendingResponse, FireSeverityResponse],
    tags=["Fire Severity"],
)
async def get_fire_severity_result(
    fire_event_name: str,
    job_id: str,
    stac_manager: STACJSONManager = Depends(get_stac_manager),
) -> Union[TaskPendingResponse, FireSeverityResponse]:
    """
    Retrieve fire severity analysis results.

    Poll until status is 'complete'. Returns URLs to severity COGs.

    See [docs/ENDPOINTS.md#get-resultanalyze_fire_severityfire_event_namejob_id](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#get-resultanalyze_fire_severityfire_event_namejob_id) for details.
    """
    # Look up the STAC item
    stac_item = await stac_manager.get_item_by_id(
        f"{fire_event_name}-severity-{job_id}"
    )

    if not stac_item:
        # Item not found, still processing
        return TaskPendingResponse(
            fire_event_name=fire_event_name, status="pending", job_id=job_id
        )

    # Extract all metric URLs from the assets
    response_cog_urls = {}
    for metric, asset in stac_item["assets"].items():
        response_cog_urls[metric] = asset["href"]

    # Return the completed response with all available metric URLs
    return FireSeverityResponse(
        fire_event_name=fire_event_name,
        status="complete",
        job_id=job_id,
        coarse_severity_cog_urls=response_cog_urls,
    )


@router.post(
    "/process/refine",
    response_model=ProcessingStartedResponse,
    tags=["Boundary Refinement"],
)
# @cache(
#     key_builder=request_key_builder,
#     namespace="root",
#     expire=60 * 60,  # Cache for 1 hour
# )
async def refine_fire_boundary(
    request: RefineRequest,
    background_tasks: BackgroundTasks,
    storage_factory: StorageFactory = Depends(get_storage_factory),
    stac_manager: STACJSONManager = Depends(get_stac_manager),
    index_registry: IndexRegistry = Depends(get_index_registry),
) -> Dict[str, Any]:
    """
    Refine fire boundary and crop severity COGs to new geometry.

    Uses user-drawn refined boundary to crop existing coarse COGs.

    See [docs/ENDPOINTS.md#post-processrefine](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#post-processrefine) for details.
    """

    # Start the processing task in the background
    background_tasks.add_task(
        process_boundary_refinement,
        job_id=request.job_id,
        fire_event_name=request.fire_event_name,
        refined_geojson=request.refined_geojson,
        storage_factory=storage_factory,
        stac_manager=stac_manager,
        index_registry=index_registry,
    )

    return {
        "fire_event_name": request.fire_event_name,
        "status": "Processing started",
        "job_id": request.job_id,
    }


async def process_boundary_refinement(
    job_id: str,
    fire_event_name: str,
    refined_geojson: Polygon | MultiPolygon | Feature,
    storage_factory: StorageFactory,
    stac_manager: STACJSONManager,
    index_registry: IndexRegistry,
) -> None:
    """
    Process boundary refinement using command pattern.
    """
    try:
        # Create command context for boundary refinement
        context = CommandContext(
            job_id=job_id,
            fire_event_name=fire_event_name,
            geometry=refined_geojson,
            storage=storage_factory.get_final_storage(),
            storage_factory=storage_factory,
            stac_manager=stac_manager,
            index_registry=index_registry,
            metadata={"operation": "boundary_refinement"},
        )

        # Execute boundary refinement command
        command = BoundaryRefinementCommand()
        result = await command.execute(context)

        if result.is_failure():
            raise HTTPException(
                status_code=500,
                detail=f"Error processing boundary refinement: {result.error_message}",
            )

    except HTTPException:
        # Re-raise HTTP exceptions as-is
        raise
    except Exception as e:
        # Log error with proper logging
        logger = logging.getLogger(__name__)
        logger.error(f"Error processing boundary refinement: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal error during boundary refinement: {str(e)}",
        )


@router.get(
    "/result/refine/{fire_event_name}/{job_id}",
    response_model=Union[TaskPendingResponse, RefinedBoundaryResponse],
    tags=["Boundary Refinement"],
)
async def get_refine_result(
    fire_event_name: str,
    job_id: str,
    stac_manager: STACJSONManager = Depends(get_stac_manager),
) -> Union[TaskPendingResponse, RefinedBoundaryResponse]:
    """
    Retrieve boundary refinement results.

    Poll until status is 'complete'. Returns URLs to refined boundary and cropped COGs.

    See [docs/ENDPOINTS.md#get-resultrefinefire_event_namejob_id](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#get-resultrefinefire_event_namejob_id) for details.
    """
    # Look up the STAC item
    boundary_stac_item = await stac_manager.get_items_by_id_and_coarseness(
        f"{fire_event_name}-boundary-{job_id}",
        "refined",
    )

    if not boundary_stac_item:
        # Item not found, still processing
        return TaskPendingResponse(
            fire_event_name=fire_event_name, status="pending", job_id=job_id
        )

    # If multiple items are found, we take the most recent one
    # TODO: This is kind of a hacky workaround, to allow for mutliple retries
    # on refining a boundary. Ideally we have 1 coarse 1 refined item, but this
    # seems less annoying and error prone than having to delete the old item
    # before creating a new one.
    if isinstance(boundary_stac_item, list):
        boundary_stac_item = sorted(
            boundary_stac_item, key=lambda x: x["properties"]["datetime"], reverse=True
        )[0]

    # Item found, extract the URLs
    geojson_url = boundary_stac_item["assets"]["refined_boundary"]["href"]

    severity_stac_item = await stac_manager.get_items_by_id_and_coarseness(
        f"{fire_event_name}-severity-{job_id}",
        "refined",
    )

    if not severity_stac_item:
        # Item not found, still processing
        return TaskPendingResponse(
            fire_event_name=fire_event_name, status="pending", job_id=job_id
        )

    # If multiple items are found, we take the most recent one
    if isinstance(severity_stac_item, list):
        severity_stac_item = sorted(
            severity_stac_item, key=lambda x: x["properties"]["datetime"], reverse=True
        )[0]

    cog_url = severity_stac_item["assets"]
    response_cog_urls = {}
    for metric, cog in cog_url.items():
        response_cog_urls[metric] = cog["href"]

    # Return the completed response
    return RefinedBoundaryResponse(
        fire_event_name=fire_event_name,
        status="complete",
        job_id=job_id,
        refined_boundary_geojson_url=geojson_url,
        refined_severity_cog_urls=response_cog_urls,
    )


@router.post("/upload/geojson", response_model=UploadedGeoJSONResponse, tags=["Upload"])
async def upload_geojson(
    request: GeoJSONUploadRequest,
    stac_manager: STACJSONManager = Depends(get_stac_manager),
    storage_factory: StorageFactory = Depends(get_storage_factory),
    index_registry: IndexRegistry = Depends(get_index_registry),
) -> UploadedGeoJSONResponse:
    """
    Upload a GeoJSON boundary for a fire event.

    See [docs/ENDPOINTS.md#post-uploadgeojson](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#post-uploadgeojson) for details.
    """
    # Validate boundary_type from request model
    if request.boundary_type not in ["coarse", "refined"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid boundary_type '{request.boundary_type}'. Must be 'coarse' or 'refined'",
        )

    # Generate a unique job ID
    job_id = str(uuid.uuid4())

    try:
        # Create command context for GeoJSON upload
        context = CommandContext(
            job_id=job_id,
            fire_event_name=request.fire_event_name,
            geometry=request.geojson,
            storage=storage_factory.get_final_storage(),
            storage_factory=storage_factory,
            stac_manager=stac_manager,
            index_registry=index_registry,
            metadata={"upload_type": "geojson", "boundary_type": request.boundary_type},
        )

        # Execute upload command
        command = UploadAOICommand()
        result = await command.execute(context)

        if result.is_failure():
            raise HTTPException(
                status_code=500,
                detail=f"Error uploading GeoJSON: {result.error_message}",
            )

        # Extract the boundary URL from command result
        if not result.data:
            raise HTTPException(
                status_code=500, detail="Upload succeeded but no result data returned"
            )

        boundary_url = result.data.get("boundary_geojson_url")
        if not boundary_url:
            raise HTTPException(
                status_code=500, detail="Upload succeeded but no boundary URL returned"
            )

        return UploadedGeoJSONResponse(
            fire_event_name=request.fire_event_name,
            status="complete",
            job_id=job_id,
            refined_boundary_geojson_url=boundary_url,
            boundary_type=request.boundary_type,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error uploading GeoJSON: {str(e)}"
        )


@router.post(
    "/upload/shapefile", response_model=UploadedShapefileZipResponse, tags=["Upload"]
)
async def upload_shapefile(
    fire_event_name: str = Form(..., description="Name of the fire event"),
    shapefile: UploadFile = File(
        ..., description="Zipped shapefile (.zip containing .shp, .shx, .dbf, .prj)"
    ),
    boundary_type: str = Form(
        default="refined", description="Boundary type: 'coarse' or 'refined'"
    ),
    stac_manager: STACJSONManager = Depends(get_stac_manager),
    storage_factory: StorageFactory = Depends(get_storage_factory),
    index_registry: IndexRegistry = Depends(get_index_registry),
) -> UploadedShapefileZipResponse:
    """
    Upload a zipped shapefile boundary for a fire event.

    Accepts multipart/form-data with shapefile zip and extracts boundary GeoJSON.

    See [docs/ENDPOINTS.md#post-uploadshapefile](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#post-uploadshapefile) for details.
    """
    # Validate boundary_type
    if boundary_type not in ["coarse", "refined"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid boundary_type '{boundary_type}'. Must be 'coarse' or 'refined'",
        )

    job_id = str(uuid.uuid4())

    try:
        # Create command context for shapefile upload
        # Note: For shapefile uploads, we provide a minimal geometry to satisfy context validation
        # The actual upload data is in metadata
        context = CommandContext(
            job_id=job_id,
            fire_event_name=fire_event_name,
            geometry=convert_geometry_to_pydantic(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                }
            ),  # Placeholder
            storage=storage_factory.get_final_storage(),
            storage_factory=storage_factory,
            stac_manager=stac_manager,
            index_registry=index_registry,
            metadata={
                "upload_type": "shapefile",
                "upload_data": shapefile,
                "boundary_type": boundary_type,
            },
        )

        # Execute upload command
        command = UploadAOICommand()
        result = await command.execute(context)

        if result.is_failure():
            raise HTTPException(
                status_code=500,
                detail=f"Error uploading shapefile: {result.error_message}",
            )

        # Extract the shapefile URL from command result
        if not result.data:
            raise HTTPException(
                status_code=500, detail="Upload succeeded but no result data returned"
            )

        shapefile_url = result.data.get("shapefile_url")
        if not shapefile_url:
            raise HTTPException(
                status_code=500, detail="Upload succeeded but no shapefile URL returned"
            )

        boundary_geojson_url = result.data.get("boundary_geojson_url")
        if not boundary_geojson_url:
            raise HTTPException(
                status_code=500,
                detail="Upload succeeded but no boundary GeoJSON URL returned",
            )

        return UploadedShapefileZipResponse(
            fire_event_name=fire_event_name,
            status="complete",
            job_id=job_id,
            shapefile_url=shapefile_url,
            boundary_geojson_url=boundary_geojson_url,
            boundary_type=boundary_type,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error uploading shapefile: {str(e)}"
        )


@router.post(
    "/process/resolve_against_veg_map",
    response_model=ProcessingStartedResponse,
    tags=["Vegetation Map Analysis"],
)
async def resolve_against_veg_map(
    request: VegMapResolveRequest,
    background_tasks: BackgroundTasks,
    storage_factory: StorageFactory = Depends(get_storage_factory),
    stac_manager: STACJSONManager = Depends(get_stac_manager),
    index_registry: IndexRegistry = Depends(get_index_registry),
) -> ProcessingStartedResponse:
    """
    Analyze fire severity impacts on vegetation communities.

    Performs zonal statistics to calculate area affected by severity class per vegetation type.

    See [docs/ENDPOINTS.md#post-processresolve_against_veg_map](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#post-processresolve_against_veg_map) for details.
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Received resolve_against_veg_map request for job {request.job_id}")
    logger.debug(f"Request details: {request}")

    # Start processing in background using command pattern
    background_tasks.add_task(
        execute_vegetation_resolution_command,
        job_id=request.job_id,
        fire_event_name=request.fire_event_name,
        veg_gpkg_url=request.veg_gpkg_url,
        fire_cog_url=request.fire_cog_url,
        severity_breaks=request.severity_breaks,
        geojson_url=request.geojson_url,
        park_unit_id=request.park_unit_id,
        storage_factory=storage_factory,
        stac_manager=stac_manager,
        index_registry=index_registry,
    )

    return ProcessingStartedResponse(
        fire_event_name=request.fire_event_name,
        status="Processing started",
        job_id=request.job_id,
    )


async def execute_vegetation_resolution_command(
    job_id: str,
    fire_event_name: str,
    veg_gpkg_url: str,
    fire_cog_url: str,
    severity_breaks: List[float],
    geojson_url: str,
    park_unit_id: Optional[str],
    storage_factory: StorageFactory,
    stac_manager: STACJSONManager,
    index_registry: IndexRegistry,
) -> None:
    """
    Execute vegetation resolution using command pattern with complete storage abstraction.

    This replaces the previous process_veg_map_resolution function and eliminates
    all filesystem dependencies by using the VegetationResolveCommand.
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Starting execute_vegetation_resolution_command for job {job_id}")
    logger.debug(
        f"Parameters: veg_gpkg_url={veg_gpkg_url}, fire_cog_url={fire_cog_url}"
    )
    logger.debug(
        f"severity_breaks={severity_breaks}, geojson_url={geojson_url}, park_unit_id={park_unit_id}"
    )

    try:
        # Create command context with vegetation resolution parameters
        # Note: geometry is None here as the actual boundary comes from geojson_url
        logger.debug("Creating CommandContext for vegetation resolution")
        context = CommandContext(
            job_id=job_id,
            fire_event_name=fire_event_name,
            geometry=None,
            storage=storage_factory.get_temp_storage(),
            storage_factory=storage_factory,
            stac_manager=stac_manager,
            index_registry=index_registry,
            severity_breaks=severity_breaks,
            metadata={
                "veg_gpkg_url": veg_gpkg_url,
                "fire_cog_url": fire_cog_url,
                "geojson_url": geojson_url,
                "park_unit_id": park_unit_id,
            },
        )

        # Execute vegetation resolution command
        logger.debug("Creating VegetationResolveCommand instance")
        command = VegetationResolveCommand()
        logger.debug("Executing vegetation resolution command")
        result = await command.execute(context)
        logger.debug(
            f"Command execution completed with status: success={result.is_success()}, failure={result.is_failure()}"
        )

        if result.is_success():
            result_data = result.data or {}
            logger.info(
                f"Vegetation resolution completed successfully for job {job_id}. "
                f"Analyzed {result_data.get('vegetation_types_analyzed', 0)} vegetation types "
                f"covering {result_data.get('total_area_hectares', 0):.1f} hectares."
            )
        elif result.is_failure():
            logger.error(
                f"Vegetation resolution failed for job {job_id}: {result.error_message}"
            )
            if result.error_details:
                logger.error(f"Error details: {result.error_details}")
        else:
            logger.warning(
                f"Vegetation resolution completed with partial success for job {job_id}: "
                f"{result.error_message}"
            )

    except Exception as e:
        logger.error(
            f"Error executing vegetation resolution command for job {job_id}: {str(e)}",
            exc_info=True,
        )


@router.get(
    "/result/resolve_against_veg_map/{fire_event_name}/{job_id}",
    response_model=Union[TaskPendingResponse, VegMapMatrixResponse],
    tags=["Vegetation Map Analysis"],
)
async def get_veg_map_result(
    fire_event_name: str,
    job_id: str,
    severity_breaks: Optional[List[float]] = None,
    stac_manager: STACJSONManager = Depends(get_stac_manager),
) -> Union[TaskPendingResponse, VegMapMatrixResponse]:
    """
    Retrieve vegetation impact analysis results.

    Poll until status is 'complete'. Returns URLs to CSV and JSON impact matrices.

    See [docs/ENDPOINTS.md#get-resultresolve_against_veg_mapfire_event_namejob_id](https://github.com/YOUR_ORG/fire-recovery-backend/blob/main/docs/ENDPOINTS.md#get-resultresolve_against_veg_mapfire_event_namejob_id) for details.
    """
    # Look up the STAC item
    stac_item = await stac_manager.get_items_by_id_and_classification_breaks(
        f"{fire_event_name}-veg-matrix-{job_id}",
        classification_breaks=severity_breaks,
    )

    if not stac_item:
        # Item not found, still processing
        return TaskPendingResponse(
            fire_event_name=fire_event_name, status="pending", job_id=job_id
        )

    # Item found, extract the matrix URL
    matrix_csv_url = stac_item["assets"]["fire_veg_matrix_csv"]["href"]
    matrix_json_url = stac_item["assets"]["fire_veg_matrix_json"]["href"]

    # Return the completed response
    return VegMapMatrixResponse(
        fire_event_name=fire_event_name,
        status="complete",
        job_id=job_id,
        fire_veg_matrix_csv_url=matrix_csv_url,
        fire_veg_matrix_json_url=matrix_json_url,
    )
