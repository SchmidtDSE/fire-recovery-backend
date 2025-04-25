from fastapi import FastAPI, BackgroundTasks, File, UploadFile
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import coiled
import dask.distributed
import rioxarray
import stackstac
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles
import os
import uuid
from .process import process_remote_sensing_data
import random
import time
import src.msgspec_geojson

# Dictionary to track when job requests were first received, for testing
job_timestamps = {}

app = FastAPI(title="Fire Recovery Backend")
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
    geometry: dict  # Geojson of bounding box AOI - to be validated later
    prefire_date_range: list[str] = None  # ["2023-01-01", "2023-12-31"]
    postfire_date_range: list[str] = None  # ["2024-01-01", "2024-12-31"]

@app.get("/")
async def root():
    return {"message": "Welcome to the Fire Recovery Backend API"}

@app.post("/process/")
async def process_data(request: ProcessingRequest, background_tasks: BackgroundTasks):

    try:
        geometry = msgspec_geojson.loads(json.dumps(request.geometry))
    except Exception as e:
        return {"error": "Invalid GeoJSON", "details": str(e)}
        
    job_id = str(uuid.uuid4())
    background_tasks.add_task(
        process_remote_sensing_data,
        job_id,
        STAC_URL,
        request.geometry,
        request.prefire_date_range,
        request.postfire_date_range,
    )
    return {"status": "Processing started", "job_id": job_id}

@app.post("/process-test/")
async def process_data_test(request: ProcessingRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    return {"status": "Processing started", "job_id": job_id}

@app.get("/result-test/{job_id}")
async def get_result_test(job_id: str):
    # Record timestamp on first request
    if job_id not in job_timestamps:
        job_timestamps[job_id] = time.time()
    
    # Check elapsed time
    elapsed = time.time() - job_timestamps[job_id]
    
    # Return pending for first 5 seconds
    if elapsed < 10:
        return {"status": "pending", "job_id": job_id}
    
    # After 5 seconds, return complete response
    cog_file = "test/assets/geology_intermediate_rbr.tif"
    return {
        "status": "complete", 
        "job_id": job_id, 
        "cog_url": "https://burn-severity-backend-prod.s3.us-east-2.amazonaws.com/public/dse/MN_Geo/intermediate_rbr.tif"
    }

# @app.get("/status/{job_id}")
# async def get_status(job_id: str):
#     status_file = f"/tmp/{job_id}/status.txt"
#     if os.path.exists(status_file):
#         with open(status_file, "r") as f:
#             status = f.read().strip()
#         return {"status": status, "job_id": job_id}
#     return {"status": "not_found", "job_id": job_id}

# @app.get("/validation/{job_id}")
# async def get_validation(job_id: str):
#     validation_file = f"/tmp/{job_id}/validation.txt"
#     if os.path.exists(validation_file):
#         with open(validation_file, "r") as f:
#             validation = f.read()
#         return {"validation": validation, "job_id": job_id}
#     return {"status": "not_found", "job_id": job_id}

# @app.get("/result/{job_id}")
# async def get_result(job_id: str):
#     cog_file = f"/tmp/{job_id}/result.tif"
#     if os.path.exists(cog_file):
#         return FileResponse(cog_file, media_type="image/tiff", filename=f"{job_id}.tif")
#     return {"status": "not_found", "job_id": job_id}
