from fastapi import FastAPI, BackgroundTasks, File, UploadFile, Form, HTTPException, Depends
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import coiled
import dask.distributed
import rioxarray
import stackstac
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles
import os
import uuid
import json
from .process import process_remote_sensing_data
import random
import time
import src.msgspec_geojson

# Dictionary to track when job requests were first received, for testing
job_timestamps = {}

app = FastAPI(
    title="Fire Recovery Backend",
    description="API for fire recovery analysis tools including fire severity analysis, boundary refinement, and vegetation impact assessment",
    version="1.0.0"
)
STAC_URL = "https://earth-search.aws.element84.com/v1/"

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5500"],  # Frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

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

class ProcessingStartedResponse(BaseResponse):
    pass

class RefinedBoundaryResponse(BaseResponse):
    refined_geojson_url: str
    cog_url: str

class FireSeverityResponse(BaseResponse):
    cog_url: str = None

class VegMapMatrixResponse(BaseResponse):
    fire_veg_matrix: str = None

class UploadedGeoJSONResponse(BaseResponse):
    uploaded_geojson: str

@app.get("/")
async def root():
    return {"message": "Welcome to the Fire Recovery Backend API"}

@app.post("/process/analyze_fire_severity", response_model=ProcessingStartedResponse, tags=["Fire Severity"])
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

    # REAL IMPLEMENTATION
    # try:
    #     geometry = msgspec_geojson.loads(json.dumps(request.geometry))
    # except Exception as e:
    #     raise HTTPException(status_code=400, detail=f"Invalid GeoJSON: {str(e)}")
        
    # job_id = str(uuid.uuid4())
    # background_tasks.add_task(
    #     process_remote_sensing_data,
    #     job_id,
    #     STAC_URL,
    #     request.geometry,
    #     request.prefire_date_range,
    #     request.postfire_date_range,
    # )
    # job_timestamps[job_id] = time.time()
    # return {
    #    "fire_event_name": request.fire_event_name,
    #    "status": "Processing started", 
    #    "job_id": job_id
    # }

@app.get("/result/analyze_fire_severity/{fire_event_name}/{job_id}", response_model=FireSeverityResponse, tags=["Fire Severity"])
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
        return {
            "fire_event_name": fire_event_name,
            "status": "pending", 
            "job_id": job_id
        }
    
    # After 2 seconds, return complete response
    return {
        "fire_event_name": fire_event_name,
        "status": "complete", 
        "job_id": job_id, 
        "cog_url": f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/intermediate_rbr.tif"
    }

@app.post("/process/refine", response_model=ProcessingStartedResponse, tags=["Boundary Refinement"])
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

@app.get("/result/refine/{fire_event_name}/{job_id}", response_model=RefinedBoundaryResponse, tags=["Boundary Refinement"])
async def get_refine_result(fire_event_name: str, job_id: str):
    """
    Get the result of the fire boundary refinement.
    """
    # Check if job exists
    if job_id not in job_timestamps:
        return {
            "fire_event_name": fire_event_name,
            "status": "not_found", 
            "job_id": job_id
        }
    
    # Check elapsed time
    elapsed = time.time() - job_timestamps[job_id]
    
    # Return pending for first 2 seconds
    if elapsed < 2:
        return {
            "fire_event_name": fire_event_name,
            "status": "pending", 
            "job_id": job_id
        }
    
    # After 2 seconds, return complete response
    return {
        "fire_event_name": fire_event_name,
        "status": "complete", 
        "job_id": job_id, 
        "refined_geojson_url": f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/refined.geojson",
        "cog_url": f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/refined_rbr.tif"
    }

@app.post("/process/resolve_against_veg_map", response_model=ProcessingStartedResponse, tags=["Vegetation Impact"])
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

@app.get("/result/resolve_against_veg_map/{fire_event_name}/{job_id}", response_model=VegMapMatrixResponse, tags=["Vegetation Impact"])
async def get_veg_map_result(fire_event_name: str, job_id: str):
    """
    Get the result of the vegetation map processing.
    """
    # Check if job exists
    if job_id not in job_timestamps:
        return {
            "fire_event_name": fire_event_name,
            "status": "not_found", 
            "job_id": job_id
        }
    
    # Check elapsed time
    elapsed = time.time() - job_timestamps[job_id]
    
    # Return pending for first 2 seconds
    if elapsed < 2:
        return {
            "fire_event_name": fire_event_name,
            "status": "pending", 
            "job_id": job_id
        }
    
    # After 2 seconds, return complete response
    return {
        "fire_event_name": fire_event_name,
        "status": "complete", 
        "job_id": job_id, 
        "fire_veg_matrix": f"https://storage.googleapis.com/national_park_service/mock_assets_frontend/{fire_event_name}/fire_veg_matrix.csv"
    }

@app.post("/upload/shapefile", response_model=UploadedGeoJSONResponse, tags=["Upload"])
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

@app.post("/upload/geojson", response_model=UploadedGeoJSONResponse, tags=["Upload"])
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