import json
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    Form,
    HTTPException,
    UploadFile,
)
from geojson_pydantic import Polygon, Feature
from shapely.geometry import shape

from src.config.constants import FINAL_BUCKET_NAME
from src.process.resolve_veg import process_veg_map
from src.process.spectral_indices import (
    process_remote_sensing_data,
)
from src.stac.stac_endpoint_handler import StacEndpointHandler
from src.stac.stac_json_manager import STACJSONManager
from src.util.cog_ops import (
    create_cog_bytes,
    crop_cog_with_geometry,
    download_cog_to_temp,
)
from src.util.polygon_ops import polygon_to_valid_geojson
from src.util.upload_blob import upload_to_gcs
from src.commands.impl.upload_aoi_command import UploadAOICommand
from src.commands.impl.health_check_command import HealthCheckCommand
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
from src.config.storage import get_temp_storage
from typing import Any


def convert_geometry_to_pydantic(geometry: dict[str, Any]) -> Polygon | Feature:
    """Convert dict geometry to geojson-pydantic type"""
    if geometry.get("type") == "Feature":
        return Feature.model_validate(geometry)
    else:
        return Polygon.model_validate(geometry)


async def process_and_upload_geojson(
    geometry: Polygon | Feature | dict, fire_event_name: str, job_id: str, filename: str
) -> Tuple[str, Dict[str, Any], List[float]]:
    """
    Validate, save and upload a GeoJSON boundary

    Args:
        geometry: The geometry or GeoJSON to process
        fire_event_name: Name of the fire event
        job_id: Job ID for the processing task
        filename: Base filename for the GeoJSON (without extension)

    Returns:
        Tuple containing:
        - URL to the uploaded GeoJSON
        - Validated GeoJSON object
        - Bounding box coordinates [minx, miny, maxx, maxy]
    """
    # Convert the Polygon/geometry to a valid GeoJSON object
    valid_geojson = polygon_to_valid_geojson(geometry)

    # Save directly to temp storage and upload
    temp_storage = get_temp_storage()
    geojson_bytes = json.dumps(valid_geojson.model_dump()).encode("utf-8")

    # Generate temp path for intermediate storage
    temp_path = f"{job_id}/{filename}.geojson"
    await temp_storage.save_bytes(geojson_bytes, temp_path, temporary=True)

    # Upload to GCS
    blob_name = f"{fire_event_name}/{job_id}/{filename}.geojson"
    geojson_url = await upload_to_gcs(geojson_bytes, blob_name)

    # Extract bbox from geometry for STAC
    valid_geojson_dict = valid_geojson.model_dump()
    geom_shape = shape(valid_geojson_dict["features"][0]["geometry"])
    bbox = geom_shape.bounds  # (minx, miny, maxx, maxy)

    return geojson_url, valid_geojson_dict, list(bbox)


async def process_cog_with_boundary(
    original_cog_url: str,
    valid_geojson: Dict[str, Any],
    fire_event_name: str,
    job_id: str,
    output_filename: str,
) -> str:
    """
    Process a COG with a boundary: download, crop, create new COG, and upload

    Args:
        original_cog_url: URL to the original COG
        valid_geojson: The validated GeoJSON to crop with
        fire_event_name: Name of the fire event
        job_id: Job ID for the processing task
        output_filename: Filename for the output COG (without extension)

    Returns:
        URL to the uploaded processed COG
    """
    # Download the original COG to a temporary file
    tmp_cog_path = await download_cog_to_temp(original_cog_url)

    # Crop the COG with the refined boundary
    cropped_data = crop_cog_with_geometry(tmp_cog_path, valid_geojson)

    # Create a new COG from the cropped data as bytes
    cog_bytes = await create_cog_bytes(cropped_data)

    # Upload the refined COG to GCS
    cog_blob_name = f"{fire_event_name}/{job_id}/{output_filename}.tif"
    cog_url = await upload_to_gcs(cog_bytes, cog_blob_name)

    return cog_url


# Dictionary to track when job requests were first received
job_timestamps = {}

# Initialize router
router = APIRouter(
    prefix="/fire-recovery",
    tags=["Fire Recovery"],
    responses={404: {"description": "Not found"}},
)
# Initialize STAC manager
stac_manager = STACJSONManager.for_production(
    base_url=f"https://storage.googleapis.com/{FINAL_BUCKET_NAME}/stac"
)

# Initialize storage factory and index registry for commands
storage_factory = StorageFactory()
index_registry = IndexRegistry()


@router.get("/", tags=["Root"])
async def root() -> Dict[str, str]:
    return {"message": "Welcome to the Fire Recovery Backend API"}


@router.get("/healthz", response_model=HealthCheckResponse, tags=["Health"])
async def health_check() -> HealthCheckResponse:
    """
    Health check endpoint using command pattern.

    This endpoint demonstrates the command pattern for a simple operation
    and serves as a template for other endpoints.
    """
    # Generate a unique job ID for the health check
    job_id = str(uuid.uuid4())

    try:
        # Create command context for health check
        # Note: We use minimal geometry for health checks
        context = CommandContext(
            job_id=job_id,
            fire_event_name="health-check",
            geometry=convert_geometry_to_pydantic({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {},
            }),
            storage=storage_factory.get_temp_storage(),
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
    request: ProcessingRequest, background_tasks: BackgroundTasks
) -> ProcessingStartedResponse:
    """
    Analyze fire severity using remote sensing data.
    """
    job_id = str(uuid.uuid4())
    job_timestamps[job_id] = time.time()

    # Start the processing task in the background
    background_tasks.add_task(
        process_fire_severity,
        job_id=job_id,
        fire_event_name=request.fire_event_name,
        geometry=convert_geometry_to_pydantic(request.geometry),
        prefire_date_range=request.prefire_date_range,
        postfire_date_range=request.postfire_date_range,
    )

    return ProcessingStartedResponse(
        fire_event_name=request.fire_event_name,
        status="Processing started",
        job_id=job_id,
    )


async def process_fire_severity(
    job_id: str,
    fire_event_name: str,
    geometry: Polygon | Feature,
    prefire_date_range: list[str],
    postfire_date_range: list[str],
) -> None:
    try:
        # Create STAC endpoint handler
        stac_handler = StacEndpointHandler()

        # 1. Process the data
        result = await process_remote_sensing_data(
            job_id=job_id,
            geometry=geometry,
            stac_endpoint_handler=stac_handler,
            prefire_date_range=prefire_date_range,
            postfire_date_range=postfire_date_range,
        )

        # 2. Upload the COGs to GCS
        cog_urls = {}  # Store all COG URLs in a dictionary
        for key, value in result["output_files"].items():
            cog_path = value
            blob_name = f"{fire_event_name}/{job_id}/{key}.tif"
            uploaded_url = await upload_to_gcs(cog_path, blob_name)

            # Store the URL in the dictionary
            cog_urls[key] = uploaded_url

        # 3. Create a STAC item for the fire severity
        datetime_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        await stac_manager.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_urls=cog_urls,  # Pass the dictionary of COG URLs
            geometry=geometry,
            datetime_str=datetime_str,
        )

        # 4. Process and upload the boundary GeoJSON
        boundary_geojson_url, valid_geojson, bbox = await process_and_upload_geojson(
            geometry=geometry,
            fire_event_name=fire_event_name,
            job_id=job_id,
            filename="coarse_boundary",
        )

        # 5. Create a STAC item for the coarse boundary
        await stac_manager.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            boundary_geojson_url=boundary_geojson_url,
            bbox=bbox,
            datetime_str=datetime_str,
            boundary_type="coarse",
        )

    except Exception as e:
        # Log error
        print(f"Error processing fire severity: {str(e)}")
        # Set job status to failed


@router.get(
    "/result/analyze_fire_severity/{fire_event_name}/{job_id}",
    response_model=Union[TaskPendingResponse, FireSeverityResponse],
    tags=["Fire Severity"],
)
async def get_fire_severity_result(
    fire_event_name: str, job_id: str
) -> Union[TaskPendingResponse, FireSeverityResponse]:
    """
    Get the result of the fire severity analysis.
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
    request: RefineRequest, background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """
    Refine the fire boundary to the provided GeoJSON.
    """

    # Start the processing task in the background
    background_tasks.add_task(
        process_boundary_refinement,
        job_id=request.job_id,
        fire_event_name=request.fire_event_name,
        refine_geojson=request.refine_geojson,
    )

    return {
        "fire_event_name": request.fire_event_name,
        "status": "Processing started",
        "job_id": request.job_id,
    }


async def process_boundary_refinement(
    job_id: str, fire_event_name: str, refine_geojson: dict
) -> None:
    """
    Process boundary refinement, upload results, and create STAC assets
    """
    try:
        # 1. Process and upload the boundary GeoJSON
        geojson_url, valid_geojson, bbox = await process_and_upload_geojson(
            geometry=refine_geojson,
            fire_event_name=fire_event_name,
            job_id=job_id,
            filename="refined_boundary",
        )

        # 2. Get the original/coarse fire severity COG URL
        stac_id = f"{fire_event_name}-severity-{job_id}"
        original_cog_item = await stac_manager.get_item_by_id(stac_id)
        if not original_cog_item:
            raise HTTPException(
                status_code=404,
                detail=f"Original COG not found for job ID {job_id}",
            )

        refined_cog_urls = {}
        for metric, cog_url in original_cog_item["assets"].items():
            coarse_cog_url = cog_url["href"]
            # 3. Process the COG with the refined boundary
            cog_url = await process_cog_with_boundary(
                original_cog_url=coarse_cog_url,
                valid_geojson=valid_geojson,
                fire_event_name=fire_event_name,
                job_id=job_id,
                output_filename=f"refined_{metric}",
            )
            refined_cog_urls[metric] = cog_url

        # 4. Create the STAC item for this cropped COG
        polygon_json = valid_geojson["features"][0]["geometry"]
        await stac_manager.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_urls=refined_cog_urls,
            geometry=polygon_json,
            datetime_str=original_cog_item["properties"]["datetime"],
            boundary_type="refined",
        )

        # 5. Create the STAC item for the refined boundary
        datetime_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        await stac_manager.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            boundary_geojson_url=geojson_url,
            bbox=bbox,
            datetime_str=datetime_str,
            boundary_type="refined",
        )

    except Exception as e:
        # Log error
        print(f"Error processing boundary refinement: {str(e)}")
        raise e


@router.get(
    "/result/refine/{fire_event_name}/{job_id}",
    response_model=Union[TaskPendingResponse, RefinedBoundaryResponse],
    tags=["Boundary Refinement"],
)
async def get_refine_result(
    fire_event_name: str, job_id: str
) -> Union[TaskPendingResponse, RefinedBoundaryResponse]:
    """
    Get the result of the fire boundary refinement.
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
async def upload_geojson(request: GeoJSONUploadRequest) -> UploadedGeoJSONResponse:
    """
    Upload GeoJSON data for a fire event.
    """
    # Generate a unique job ID
    job_id = str(uuid.uuid4())

    try:
        # Create command context for GeoJSON upload
        context = CommandContext(
            job_id=job_id,
            fire_event_name=request.fire_event_name,
            geometry=convert_geometry_to_pydantic(request.geojson),
            storage=storage_factory.get_temp_storage(),
            stac_manager=stac_manager,
            index_registry=index_registry,
            metadata={"upload_type": "geojson"},
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
    fire_event_name: str = Form(...), shapefile: UploadFile = File(...)
) -> UploadedShapefileZipResponse:
    """Upload a zipped shapefile for a fire event."""
    job_id = str(uuid.uuid4())

    try:
        # Create command context for shapefile upload
        # Note: For shapefile uploads, we provide a minimal geometry to satisfy context validation
        # The actual upload data is in metadata
        context = CommandContext(
            job_id=job_id,
            fire_event_name=fire_event_name,
            geometry=convert_geometry_to_pydantic({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
                "properties": {},
            }),  # Placeholder
            storage=storage_factory.get_temp_storage(),
            stac_manager=stac_manager,
            index_registry=index_registry,
            metadata={"upload_type": "shapefile", "upload_data": shapefile},
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

        return UploadedShapefileZipResponse(
            fire_event_name=fire_event_name,
            status="complete",
            job_id=job_id,
            shapefile_url=shapefile_url,
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
    request: VegMapResolveRequest, background_tasks: BackgroundTasks
) -> ProcessingStartedResponse:
    """
    Resolve fire severity against vegetation map to create a matrix of affected areas.
    """

    # Start processing in background
    background_tasks.add_task(
        process_veg_map_resolution,
        job_id=request.job_id,
        fire_event_name=request.fire_event_name,
        veg_gpkg_url=request.veg_gpkg_url,
        fire_cog_url=request.fire_cog_url,
        severity_breaks=request.severity_breaks,
        geojson_url=request.geojson_url,
    )

    return ProcessingStartedResponse(
        fire_event_name=request.fire_event_name,
        status="Processing started",
        job_id=request.job_id,
    )


async def process_veg_map_resolution(
    job_id: str,
    fire_event_name: str,
    veg_gpkg_url: str,
    fire_cog_url: str,
    severity_breaks: List[float],
    geojson_url: str,
) -> None:
    """
    Process vegetation map against fire severity COG to create area matrix.
    """
    try:
        # Set up output directory
        output_dir = f"tmp/{job_id}"
        os.makedirs(output_dir, exist_ok=True)

        # Process the vegetation map against fire severity
        result = await process_veg_map(
            veg_gpkg_url=veg_gpkg_url,
            fire_cog_url=fire_cog_url,
            output_dir=output_dir,
            job_id=job_id,
            severity_breaks=severity_breaks,
            geojson_url=geojson_url,
        )

        if result["status"] != "completed":
            print(f"Error processing vegetation map: {result.get('error_message')}")
            return

        print("Vegetation map processing completed successfully.")

        # Upload both CSV and JSON to GCS
        csv_blob_name = f"{fire_event_name}/{job_id}/veg_fire_matrix.csv"
        json_blob_name = f"{fire_event_name}/{job_id}/veg_fire_matrix.json"

        csv_url = await upload_to_gcs(result["output_csv"], csv_blob_name)
        json_url = await upload_to_gcs(result["output_json"], json_blob_name)

        print(f"Vegetation fire matrix CSV uploaded to {csv_url}")
        print(f"Vegetation fire matrix JSON uploaded to {json_url}")

        # Get geometry from the fire severity COG
        stac_item = await stac_manager.get_items_by_id_and_coarseness(
            f"{fire_event_name}-severity-{job_id}", "refined"
        )
        if not stac_item:
            raise HTTPException(
                status_code=404,
                detail=f"Fire severity COG not found for job ID {job_id}",
            )
        else:
            geometry = stac_item["geometry"]
            bbox = stac_item["bbox"]

            # Get datetime from the fire severity COG
            datetime_str = stac_item["properties"]["datetime"]

            print(
                f"Resolved vegetation map against fire severity for {fire_event_name}..."
            )

            await stac_manager.create_veg_matrix_item(
                fire_event_name=fire_event_name,
                job_id=job_id,
                fire_veg_matrix_csv_url=csv_url,
                fire_veg_matrix_json_url=json_url,
                geometry=geometry,
                bbox=bbox,
                classification_breaks=severity_breaks,
                datetime_str=datetime_str,
            )

    except Exception as e:
        # Log error
        print(f"Error processing vegetation map against fire severity: {str(e)}")


@router.get(
    "/result/resolve_against_veg_map/{fire_event_name}/{job_id}",
    response_model=Union[TaskPendingResponse, VegMapMatrixResponse],
    tags=["Vegetation Map Analysis"],
)
async def get_veg_map_result(
    fire_event_name: str, job_id: str, severity_breaks: Optional[List[float]] = None
) -> Union[TaskPendingResponse, VegMapMatrixResponse]:
    """
    Get the result of the vegetation map resolution against fire severity.
    Use query parameter: ?severity_breaks=0.1&severity_breaks=0.2&severity_breaks=0.3
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
