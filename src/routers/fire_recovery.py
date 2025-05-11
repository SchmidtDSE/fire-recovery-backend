from fastapi import APIRouter, BackgroundTasks, File, UploadFile, Form, HTTPException, Depends
from pydantic import BaseModel, Field
import uuid
import time
import json
from typing import Union, Optional
from ..process import process_remote_sensing_data
import src.msgspec_geojson

# Dictionary to track when job requests were first received, for testing
job_timestamps = {}

# Initialize router
router = APIRouter(
    prefix="/fire-recovery",
    tags=["Fire Recovery"],
    responses={404: {"description": "Not found"}},
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
    Analyze fire severity using remote sensing data. This involves:
    1. Fetching remote sensing data from STAC API according to the prefire and postfire date ranges.
    2. Applying an aggregation function to the data, to get a single value for each pixel in the prefire and postfire windows.
    3. Calculating NBR for fire severity for prefire and postfire data, per pixel. 
    4. Further using prefire and postfire NBR to calculate more fire indices (RBR, dNBR, RdNBR).
    5. Returning a COG of the fire severity data (for now we assume RBR).

    See https://dse-disturbance-toolbox.org/about/methodology/
    """
    # MOCK IMPLEMENTATION FOR TESTING
    job_id = str(uuid.uuid4())
    job_timestamps[job_id] = time.time()
    
    return {
        "fire_event_name": request.fire_event_name,
        "status": "Processing started", 
        "job_id": job_id
    }

@router.get("/result/analyze_fire_severity/{fire_event_name}/{job_id}", response_model=Union[TaskPendingResponse, FireSeverityResponse], tags=["Fire Severity"])
async def get_fire_severity_result(fire_event_name: str, job_id: str):
    """
    Get the result of the fire severity analysis.
    """
    # MOCK IMPLEMENTATION FOR TESTING
    # Record timestamp on first request
    if job_id not in job_timestamps:
        job_timestamps[job_id] = time.time()
    
    # Check elapsed time
    elapsed = time.time() - job_timestamps[job_id]
    
    # Return pending for first 2 seconds
    if elapsed < 2:
        return TaskPendingResponse(
            fire_event_name=fire_event_name,
            status="pending", 
            job_id=job_id
        )
    
    # After 2 seconds, return complete response
    return FireSeverityResponse(
        fire_event_name=fire_event_name,
        status="complete", 
        job_id=job_id, 
        cog_url=f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/intermediate_rbr.tif"
    )

@router.post("/process/refine", response_model=ProcessingStartedResponse, tags=["Boundary Refinement"])
async def refine_fire_boundary(request: RefineRequest, background_tasks: BackgroundTasks):
    """
    Refine the fire boundary to the provided GeoJSON. It is assumed that the user will have seen the 
    fire severity COG and will have drawn a new boundary around the presumed fire boundary based on the
    fire severity values.
    """
    job_id = str(uuid.uuid4())
    # Here you would add a background task to refine the fire boundary
    # background_tasks.add_task(process_refine_boundary, job_id, request.fire_event_name, request.refine_geojson)
    job_timestamps[job_id] = time.time()
    return {
        "fire_event_name": request.fire_event_name,
        "status": "Processing started", 
        "job_id": job_id
    }

@router.get("/result/refine/{fire_event_name}/{job_id}", response_model=Union[TaskPendingResponse, RefinedBoundaryResponse], tags=["Boundary Refinement"])
async def get_refine_result(fire_event_name: str, job_id: str):
    """
    Get the result of the fire boundary refinement.
    """
    # Check if job exists
    if job_id not in job_timestamps:
        return TaskPendingResponse(
            fire_event_name=fire_event_name,
            status="not_found", 
            job_id=job_id
        )
    
    # Check elapsed time
    elapsed = time.time() - job_timestamps[job_id]
    
    # Return pending for first 10 seconds
    if elapsed < 10:
        return TaskPendingResponse(
            fire_event_name=fire_event_name,
            status="pending", 
            job_id=job_id
        )
    
    # After 10 seconds, return complete response
    return RefinedBoundaryResponse(
        fire_event_name=fire_event_name,
        status="complete", 
        job_id=job_id, 
        refined_geojson_url=f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/refined.geojson",
        cog_url=f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/refined_rbr.tif"
    )

@router.post("/process/resolve_against_veg_map", response_model=ProcessingStartedResponse, tags=["Vegetation Impact"])
async def resolve_against_veg_map(request: VegMapResolveRequest, background_tasks: BackgroundTasks):
    """
    Process the vegetation map (as a COG) against fire severity COG, returning a matrix of fire severity values. For now, 
    we return just a CSV rather than a true matrix, where we collapse the range of severity values into a single mean and 
    standard deviation value for each cover type.
    """
    job_id = str(uuid.uuid4())
    # Here you would add a background task to process the vegetation map
    # background_tasks.add_task(process_veg_map, job_id, request.veg_cog_url, request.fire_cog_url)
    job_timestamps[job_id] = time.time()
    return {
        "fire_event_name": request.fire_event_name,
        "status": "Processing started", 
        "job_id": job_id
    }

@router.get("/result/resolve_against_veg_map/{fire_event_name}/{job_id}", response_model=Union[TaskPendingResponse, VegMapMatrixResponse], tags=["Vegetation Impact"])
async def get_veg_map_result(fire_event_name: str, job_id: str):
    """
    Get the result of the vegetation map processing.
    """
    # Check if job exists
    if job_id not in job_timestamps:
        return TaskPendingResponse(
            fire_event_name=fire_event_name,
            status="not_found", 
            job_id=job_id
        )
    
    # Check elapsed time
    elapsed = time.time() - job_timestamps[job_id]
    
    # Return pending for first 10 seconds
    if elapsed < 10:
        return TaskPendingResponse(
            fire_event_name=fire_event_name,
            status="pending", 
            job_id=job_id
        )
    
    # After 10 seconds, return complete response
    return VegMapMatrixResponse(
        fire_event_name=fire_event_name,
        status="complete", 
        job_id=job_id, 
        fire_veg_matrix=f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/fire_veg_matrix.csv"
    )

@router.post("/upload/shapefile", response_model=UploadedGeoJSONResponse, tags=["Upload"])
async def upload_shapefile(fire_event_name: str = Form(...), shapefile: UploadFile = File(...)):
    """
    Upload a shapefile for a fire event.
    
    The shapefile will be converted to GeoJSON and stored.
    
    Returns a URL to the GeoJSON version of the uploaded shapefile.
    """
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    
    # In a real implementation, you would:
    # 1. Save the uploaded shapefile temporarily
    # 2. Convert it to GeoJSON using a library like geopandas
    # 3. Upload the GeoJSON to your storage (e.g., S3)
    
    # For now, just return a mock response
    return {
        "fire_event_name": fire_event_name,
        "status": "complete", 
        "job_id": job_id, 
        "uploaded_geojson": f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/uploaded.geojson"
    }

@router.post("/upload/geojson", response_model=UploadedGeoJSONResponse, tags=["Upload"])
async def upload_geojson(request: GeoJSONUploadRequest):
    """
    Upload GeoJSON data for a fire event.
    
    Returns a URL to the stored GeoJSON.
    """
    # Generate a unique job ID
    job_id = str(uuid.uuid4())
    
    # In a real implementation, you would:
    # 1. Validate the GeoJSON
    # try:
    #     validated_geojson = msgspec_geojson.loads(json.dumps(request.geojson))
    # except Exception as e:
    #     raise HTTPException(status_code=400, detail=f"Invalid GeoJSON: {str(e)}")
    # 2. Upload it to your storage (e.g., S3)
    
    # For now, just return a mock response
    return {
        "fire_event_name": request.fire_event_name,
        "status": "complete", 
        "job_id": job_id, 
        "uploaded_geojson": f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{request.fire_event_name}/uploaded.geojson"
    }