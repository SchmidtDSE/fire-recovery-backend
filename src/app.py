from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
from src.util.api_cache import request_key_builder
import hashlib
import json
from .routers import stac_server, fire_recovery
from contextlib import asynccontextmanager
import time


# Initialize cache with in-memory backend at app startup
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize FastAPICache
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
    yield


# Define STAC URL
STAC_URL = "https://earth-search.aws.element84.com/v1/"

app = FastAPI(
    lifespan=lifespan,
    title="Fire Recovery Backend",
    description="API for fire recovery analysis tools including fire severity analysis, boundary refinement, and vegetation impact assessment",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5500", "http://localhost:5500", "https://storage.googleapis.com/fire-recovery-web/*"],  # Frontend origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

# Include routers
app.include_router(fire_recovery.router)
app.include_router(stac_server.router)


@app.get("/")
@cache(
    key_builder=request_key_builder,
    namespace="root",
    expire=60 * 60 * 6,  # Cache for 6 hour
)
async def root():
    return {
        "message": "Welcome to the Fire Recovery Backend API",
        "docs_url": "/docs",
        "endpoints": {"fire_recovery": "/fire-recovery/", "stac": "/stac/"},
    }


@app.get("/cache_test")
@cache(
    key_builder=request_key_builder,
    namespace="root",
    expire=60 * 60,  # Cache for 1 hour
)
async def cache_test():
    time.sleep(10)
    return {"ping": "pong"}
