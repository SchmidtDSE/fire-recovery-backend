from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import coiled
import dask.distributed
import rioxarray
import stackstac
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles
import os

from .routers import stac_server, fire_recovery

# Define STAC URL
STAC_URL = "https://earth-search.aws.element84.com/v1/"

app = FastAPI(
    title="Fire Recovery Backend",
    description="API for fire recovery analysis tools including fire severity analysis, boundary refinement, and vegetation impact assessment",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5500", "http://localhost:5500"],  # Frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

# Include routers
app.include_router(fire_recovery.router)
app.include_router(stac_server.router)


@app.get("/")
async def root():
    return {
        "message": "Welcome to the Fire Recovery Backend API",
        "docs_url": "/docs",
        "endpoints": {"fire_recovery": "/fire-recovery/", "stac": "/stac/"},
    }
