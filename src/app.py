from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
import hashlib
import json
from typing import Any
from .routers import fire_recovery
from contextlib import asynccontextmanager
import time


# Initialize cache with in-memory backend at app startup
@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    # Startup: initialize FastAPICache
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Fire Recovery Backend",
    description="API for fire recovery analysis tools including fire severity analysis, boundary refinement, and vegetation impact assessment",
    version="1.0.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    # allow_origins=["http://127.0.0.1:5500", "http://localhost:5500", "https://storage.googleapis.com"],
    allow_origins=["*"],  # Allow all origins for development; restrict in production
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers\
    expose_headers=["*"],  # Expose all headers to client
)

# Include routers
app.include_router(fire_recovery.router)


@app.get("/")
async def root() -> dict[str, Any]:
    return {
        "message": "Welcome to the Fire Recovery Backend API",
        "docs_url": "/docs",
        "endpoints": {"fire_recovery": "/fire-recovery/", "stac": "/stac/"},
    }


@app.get("/cache_test")
async def cache_test() -> dict[str, str]:
    time.sleep(10)
    return {"ping": "pong"}
