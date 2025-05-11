from fastapi import APIRouter, BackgroundTasks, File, UploadFile, Form, HTTPException, Depends
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

# Dictionary to track when job requests were first received
job_timestamps = {}

# Initialize router
router = APIRouter(
    prefix="/fire-recovery",
    tags=["Fire Recovery"],
    responses={404: {"description": "Not found"}},
)

# Constants
BUCKET_NAME = "national_park_service"
STAC_URL = "https://earth-search.aws.element84.com/v1/"
STAC_STORAGE_DIR = "/tmp/stac_geoparquet"

# Initialize STAC manager
stac_manager = STACGeoParquetManager(
    base_url=f"https://storage.googleapis.com/{BUCKET_NAME}/stac",
    storage_dir=STAC_STORAGE_DIR
)

# Request models
class ProcessingRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    geometry: dict = Field(..., description="GeoJSON of bounding box AOI")
    prefire_date_range: list[str] = Field(None, description="Date range for prefire imagery (e.g. ['2023-01-01', '2023-12-31'])")
    postfire_date_range: list[str] = Field(None, description="Date range for postfire imagery (e.g. ['2024-01-01', '2024-12-31'])")

class RefineRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    refine_geojson: dict = Field(..., description="GeoJSON to be refined")

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

@router.post("/process/analyze_fire_severity", response_model=ProcessingStartedResponse, tags=["Fire Severity"])
async def analyze_fire_severity(request: ProcessingRequest, background_tasks: BackgroundTasks):
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
        postfire_date_range=request.postfire_date_range
    )
    
    return {
        "fire_event_name": request.fire_event_name,
        "status": "Processing started", 
        "job_id": job_id
    }

async def process_fire_severity(
    job_id: str,
    fire_event_name: str,
    geometry: Polygon,
    prefire_date_range: list[str],
    postfire_date_range: list[str]
):
    """
    Process fire severity, upload results, and create STAC assets
    """
    try:
        # 1. Process the data
        result = process_remote_sensing_data(
            job_id=job_id,
            stac_url=STAC_URL,
            geometry=geometry,
            prefire_date_range=prefire_date_range,
            postfire_date_range=postfire_date_range
        )
        
        if result["status"] != "completed":
            # Handle error case
            return
        
        # 2. Upload the COG to GCS
        cog_path = result["output_files"]["rbr"]  # We'll use RBR as our primary severity metric
        blob_name = f"{fire_event_name}/{job_id}/rbr.tif"
        
        cog_url = upload_to_gcs(cog_path, BUCKET_NAME, blob_name)
        
        # 3. Create a STAC item
        # Extract bbox from geometry
        geom_shape = shape(geometry)
        bbox = geom_shape.bounds  # (minx, miny, maxx, maxy)
        
        # Create the STAC item
        datetime_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        await stac_manager.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_url=cog_url,
            bbox=list(bbox),
            datetime_str=datetime_str
        )
        
    except Exception as e:
        # Log error
        print(f"Error processing fire severity: {str(e)}")

@router.get("/result/analyze_fire_severity/{fire_event_name}/{job_id}", response_model=Union[TaskPendingResponse, FireSeverityResponse], tags=["Fire Severity"])
async def get_fire_severity_result(fire_event_name: str, job_id: str):
    """
    Get the result of the fire severity analysis.
    """
    # Look up the STAC item
    stac_item = await stac_manager.get_item_by_id(fire_event_name, f"{fire_event_name}-severity-{job_id}")
    
    if not stac_item:
        # Item not found, still processing
        return TaskPendingResponse(
            fire_event_name=fire_event_name,
            status="pending", 
            job_id=job_id
        )
    
    # Item found, extract the COG URL
    cog_url = stac_item["assets"]["rbr"]["href"]
    
    # Return the completed response
    return FireSeverityResponse(
        fire_event_name=fire_event_name,
        status="complete", 
        job_id=job_id, 
        cog_url=cog_url
    )

@router.post("/process/refine", response_model=ProcessingStartedResponse, tags=["Boundary Refinement"])
async def refine_fire_boundary(request: RefineRequest, background_tasks: BackgroundTasks):
    """
    Refine the fire boundary to the provided GeoJSON.
    """
    job_id = str(uuid.uuid4())
    job_timestamps[job_id] = time.time()
    
    # Start the processing task in the background
    background_tasks.add_task(
        process_boundary_refinement,
        job_id=job_id,
        fire_event_name=request.fire_event_name,
        refine_geojson=request.refine_geojson
    )
    
    return {
        "fire_event_name": request.fire_event_name,
        "status": "Processing started", 
        "job_id": job_id
    }

async def process_boundary_refinement(
    job_id: str,
    fire_event_name: str,
    refine_geojson: dict
):
    """
    Process boundary refinement, upload results, and create STAC assets
    """
    try:
        # 1. Save the GeoJSON to a file
        with tempfile.NamedTemporaryFile(suffix=".geojson", delete=False) as tmp:
            tmp.write(json.dumps(refine_geojson).encode('utf-8'))
            geojson_path = tmp.name
            
        # 2. Upload the GeoJSON to GCS
        blob_name = f"{fire_event_name}/{job_id}/boundary.geojson"
        geojson_url = upload_to_gcs(geojson_path, BUCKET_NAME, blob_name)
        
        # 3. For demonstration purposes, we'll mock the creation of a COG
        # In a real implementation, you would process the GeoJSON to create a rasterized COG
        cog_blob_name = f"{fire_event_name}/{job_id}/refined_rbr.tif"
        # TODO: Create actual COG from boundary
        cog_url = f"https://storage.googleapis.com/{BUCKET_NAME}/{cog_blob_name}"
        
        # 4. Create a STAC item
        # Extract bbox from geometry
        if refine_geojson.get("type") == "Feature":
            geometry = refine_geojson["geometry"]
        elif refine_geojson.get("type") == "FeatureCollection" and refine_geojson.get("features"):
            # Use the first feature
            geometry = refine_geojson["features"][0]["geometry"]
        else:
            # Assume it's already a geometry object
            geometry = refine_geojson
            
        geom_shape = shape(geometry)
        bbox = geom_shape.bounds  # (minx, miny, maxx, maxy)
        
        # Create the STAC item
        datetime_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        await stac_manager.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            geojson_url=geojson_url,
            cog_url=cog_url,
            bbox=list(bbox),
            datetime_str=datetime_str
        )
        
        # Clean up the temporary file
        os.unlink(geojson_path)
        
    except Exception as e:
        # Log error
        print(f"Error processing boundary refinement: {str(e)}")

@router.get("/result/refine/{fire_event_name}/{job_id}", response_model=Union[TaskPendingResponse, RefinedBoundaryResponse], tags=["Boundary Refinement"])
async def get_refine_result(fire_event_name: str, job_id: str):
    """
    Get the result of the fire boundary refinement.
    """
    # Look up the STAC item
    stac_item = await stac_manager.get_item_by_id(fire_event_name, f"{fire_event_name}-boundary-{job_id}")
    
    if not stac_item:
        # Item not found, still processing
        return TaskPendingResponse(
            fire_event_name=fire_event_name,
            status="pending", 
            job_id=job_id
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
        cog_url=cog_url
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
        tmp.write(json.dumps(request.geojson).encode('utf-8'))
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
            "uploaded_geojson": geojson_url
        }
    except Exception as e:
        # Clean up
        os.unlink(geojson_path)
        raise HTTPException(status_code=500, detail=f"Error uploading GeoJSON: {str(e)}")