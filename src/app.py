from fastapi import FastAPI, BackgroundTasks, File, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
import coiled
import dask.distributed
import rioxarray
import stackstac
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles
import os
import uuid
from geojson_pydantic.geometries import Geometry
from .process import process_remote_sensing_data

app = FastAPI(title="Fire Recovery Backend")

STAC_URL = "https://earth-search.aws.element84.com/v1/"

class ProcessingRequest(BaseModel):
    geometry: Geometry  # Geojson of bounding box AOI
    prefire_date_range: list[str] = None  # ["2023-01-01", "2023-12-31"]
    posfire_date_range: list[str] = None  # ["2024-01-01", "2024-12-31"]

@app.get("/")
async def root():
    return {"message": "Welcome to the Fire Recovery Backend API"}

@app.post("/process/")
async def process_data(request: ProcessingRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    background_tasks.add_task(
        process_remote_sensing_data,
        job_id,
        STAC_URL,
        request.geometry,
        request.prefire_date_range,
        request.posfire_date_range,
    )
    return {"status": "Processing started", "job_id": job_id}

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    status_file = f"/tmp/{job_id}/status.txt"
    if os.path.exists(status_file):
        with open(status_file, "r") as f:
            status = f.read().strip()
        return {"status": status, "job_id": job_id}
    return {"status": "not_found", "job_id": job_id}

@app.get("/validation/{job_id}")
async def get_validation(job_id: str):
    validation_file = f"/tmp/{job_id}/validation.txt"
    if os.path.exists(validation_file):
        with open(validation_file, "r") as f:
            validation = f.read()
        return {"validation": validation, "job_id": job_id}
    return {"status": "not_found", "job_id": job_id}

@app.get("/result/{job_id}")
async def get_result(job_id: str):
    cog_file = f"/tmp/{job_id}/result.tif"
    if os.path.exists(cog_file):
        return FileResponse(cog_file, media_type="image/tiff", filename=f"{job_id}.tif")
    return {"status": "not_found", "job_id": job_id}