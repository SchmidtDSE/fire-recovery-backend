from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    UploadFile,
    Form,
    HTTPException,
    Depends,
)
from pydantic import BaseModel, Field
import uuid
import time
import json
import os
import tempfile
from typing import Union, Optional, List
from datetime import datetime
from geojson_pydantic import FeatureCollection, Feature, Polygon, MultiPolygon
from shapely.geometry import shape
from src.process.spectral_indices import process_remote_sensing_data
from src.util.upload_blob import upload_to_gcs
from src.stac.stac_geoparquet_manager import STACGeoParquetManager
from src.config.constants import BUCKET_NAME, STAC_STORAGE_DIR
from src.util.polygon_ops import polygon_to_valid_geojson
from src.util.cog_ops import (
    download_cog_to_temp,
    crop_cog_with_geometry,
    create_cog,
)

# Dictionary to track when job requests were first received
job_timestamps = {}

# Initialize router
router = APIRouter(
    prefix="/fire-recovery",
    tags=["Fire Recovery"],
    responses={404: {"description": "Not found"}},
)
# Initialize STAC manager
stac_manager = STACGeoParquetManager(
    base_url=f"https://storage.googleapis.com/{BUCKET_NAME}/stac",
    storage_dir=STAC_STORAGE_DIR,
)


# Request models
class ProcessingRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    geometry: dict = Field(..., description="GeoJSON of bounding box AOI")
    prefire_date_range: list[str] = Field(
        None,
        description="Date range for prefire imagery (e.g. ['2023-01-01', '2023-12-31'])",
    )
    postfire_date_range: list[str] = Field(
        None,
        description="Date range for postfire imagery (e.g. ['2024-01-01', '2024-12-31'])",
    )


class RefineRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    refine_geojson: dict = Field(..., description="GeoJSON to be refined")
    job_id: str = Field(
        ..., description="Job ID of the original fire severity analysis"
    )


class VegMapResolveRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    veg_cog_url: str = Field(..., description="URL to the vegetation map COG")
    fire_cog_url: str = Field(..., description="URL to the fire severity COG")


class GeoJSONUploadRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    geojson: dict = Field(..., description="GeoJSON data to upload")


# Response models
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
    refined_geojson_url: str
    cog_url: str


class FireSeverityResponse(BaseResponse):
    cog_url: Optional[str] = None


class VegMapMatrixResponse(BaseResponse):
    fire_veg_matrix: Optional[str] = None


class UploadedGeoJSONResponse(BaseResponse):
    uploaded_geojson: str


@router.get("/", tags=["Root"])
async def root():
    return {"message": "Welcome to the Fire Recovery Backend API"}


@router.post(
    "/process/analyze_fire_severity",
    response_model=ProcessingStartedResponse,
    tags=["Fire Severity"],
)
async def analyze_fire_severity(
    request: ProcessingRequest, background_tasks: BackgroundTasks
):
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
        geometry=request.geometry,
        prefire_date_range=request.prefire_date_range,
        postfire_date_range=request.postfire_date_range,
    )

    return {
        "fire_event_name": request.fire_event_name,
        "status": "Processing started",
        "job_id": job_id,
    }


async def process_fire_severity(
    job_id: str,
    fire_event_name: str,
    geometry: Polygon,
    prefire_date_range: list[str],
    postfire_date_range: list[str],
):
    """
    Process fire severity, upload results, and create STAC assets
    """
    try:
        # 1. Process the data
        result = process_remote_sensing_data(
            job_id=job_id,
            geometry=geometry,
            prefire_date_range=prefire_date_range,
            postfire_date_range=postfire_date_range,
        )

        if result["status"] != "completed":
            # Handle error case
            return

        # 2. Upload the COGs to GCS
        for key, value in result["output_files"].items():
            cog_path = value
            blob_name = f"{fire_event_name}/{job_id}/{key}.tif"
            cog_url = upload_to_gcs(cog_path, BUCKET_NAME, blob_name)

        # 3. Create a STAC item
        datetime_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        await stac_manager.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_url=cog_url,
            geometry=geometry,
            datetime_str=datetime_str,
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
async def get_fire_severity_result(fire_event_name: str, job_id: str):
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

    # Item found, extract the COG URL
    cog_url = stac_item["assets"]["rbr"]["href"]

    # Return the completed response
    return FireSeverityResponse(
        fire_event_name=fire_event_name,
        status="complete",
        job_id=job_id,
        cog_url=cog_url,
    )


@router.post(
    "/process/refine",
    response_model=ProcessingStartedResponse,
    tags=["Boundary Refinement"],
)
async def refine_fire_boundary(
    request: RefineRequest, background_tasks: BackgroundTasks
):
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
):
    """
    Process boundary refinement, upload results, and create STAC assets
    """
    temp_files = []  # Track temp files to ensure cleanup

    try:
        # 0. Convert the Polygon to a GeoJSON object
        valid_geojson = polygon_to_valid_geojson(refine_geojson)

        # 1. Save the GeoJSON to a file
        with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
            tmp.write(json.dumps(valid_geojson).encode("utf-8"))
            geojson_path = tmp.name
            temp_files.append(geojson_path)

        # 2. Upload the GeoJSON to GCS
        blob_name = f"{fire_event_name}/{job_id}/refined_boundary.geojson"
        geojson_url = upload_to_gcs(geojson_path, BUCKET_NAME, blob_name)

        # 3. Get the original/coarse fire severity COG URL
        stac_id = f"{fire_event_name}-severity-{job_id}"
        original_cog_item = await stac_manager.get_item_by_id(stac_id)
        if not original_cog_item:
            raise HTTPException(
                status_code=404,
                detail=f"Original COG not found for job ID {job_id}",
            )

        for key, value in original_cog_item["assets"].items():
            original_cog_url = value["href"]
            # 4. Download the original COG to a temporary file
            original_cog_path = await download_cog_to_temp(original_cog_url)
            temp_files.append(original_cog_path)

            # 5. Crop the COG with the refined boundary
            cropped_data = crop_cog_with_geometry(original_cog_path, valid_geojson)

            # 6. Create a new COG from the cropped data
            refined_cog_path = tempfile.mktemp(suffix=".tif")
            temp_files.append(refined_cog_path)

            cog_result = create_cog(cropped_data, refined_cog_path)
            if not cog_result["is_valid"]:
                raise Exception("Failed to create a valid COG from cropped data")

            # 7. Upload the refined COG to GCS
            cog_blob_name = f"{fire_event_name}/{job_id}/refined_rbr.tif"
            cog_url = upload_to_gcs(refined_cog_path, BUCKET_NAME, cog_blob_name)

            # 8. Extract bbox from geometry for STAC
            geom_shape = shape(valid_geojson["geometry"])
            bbox = geom_shape.bounds  # (minx, miny, maxx, maxy)

            # 9. Create the STAC item for this cropped COG
            await stac_manager.create_fire_severity_item(
                fire_event_name=fire_event_name,
                job_id=job_id,
                cog_url=cog_url,
                geometry=valid_geojson,
                datetime_str=original_cog_item["properties"]["datetime"],
                boundary_type="refined",
            )

        # 10. Create the STAC item for the refined boundary
        datetime_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        await stac_manager.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            geojson_url=geojson_url,
            cog_url=cog_url,
            bbox=list(bbox),
            datetime_str=datetime_str,
        )

    except Exception as e:
        # Log error
        print(f"Error processing boundary refinement: {str(e)}")
        raise e
    finally:
        # Clean up all temporary files
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                try:
                    os.unlink(temp_file)
                except Exception as e:
                    print(f"Failed to remove temporary file {temp_file}: {str(e)}")


@router.get(
    "/result/refine/{fire_event_name}/{job_id}",
    response_model=Union[TaskPendingResponse, RefinedBoundaryResponse],
    tags=["Boundary Refinement"],
)
async def get_refine_result(fire_event_name: str, job_id: str):
    """
    Get the result of the fire boundary refinement.
    """
    # Look up the STAC item
    stac_item = await stac_manager.get_item_by_id(
        f"{fire_event_name}-boundary-{job_id}"
    )

    if not stac_item:
        # Item not found, still processing
        return TaskPendingResponse(
            fire_event_name=fire_event_name, status="pending", job_id=job_id
        )

    # Item found, extract the URLs
    geojson_url = stac_item["assets"]["refined_boundary"]["href"]
    cog_url = stac_item["assets"]["refined_severity"]["href"]

    # Return the completed response
    return RefinedBoundaryResponse(
        fire_event_name=fire_event_name,
        status="complete",
        job_id=job_id,
        refined_geojson_url=geojson_url,
        cog_url=cog_url,
    )


@router.post("/upload/geojson", response_model=UploadedGeoJSONResponse, tags=["Upload"])
async def upload_geojson(request: GeoJSONUploadRequest):
    """
    Upload GeoJSON data for a fire event.
    """
    # Generate a unique job ID
    job_id = str(uuid.uuid4())

    # Validate the GeoJSON using geojson_pydantic
    try:
        if request.geojson.get("type") == "FeatureCollection":
            validated_geojson = FeatureCollection.model_validate(request.geojson)
        elif request.geojson.get("type") == "Feature":
            validated_geojson = Feature.model_validate(request.geojson)
        else:
            raise ValueError(f"Unsupported GeoJSON type: {request.geojson.get('type')}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid GeoJSON: {str(e)}")

    # Save the validated GeoJSON to a temporary file
    with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
        tmp.write(json.dumps(request.geojson).encode("utf-8"))
        geojson_path = tmp.name

    try:
        # Upload to GCS
        blob_name = f"{request.fire_event_name}/{job_id}/uploaded.geojson"
        geojson_url = upload_to_gcs(geojson_path, BUCKET_NAME, blob_name)

        # Clean up
        os.unlink(geojson_path)

        # Return response
        return {
            "fire_event_name": request.fire_event_name,
            "status": "complete",
            "job_id": job_id,
            "uploaded_geojson": geojson_url,
        }
    except Exception as e:
        # Clean up
        os.unlink(geojson_path)
        raise HTTPException(
            status_code=500, detail=f"Error uploading GeoJSON: {str(e)}"
        )
