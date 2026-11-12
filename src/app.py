from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from typing import Any, AsyncGenerator
from .routers import fire_recovery
from contextlib import asynccontextmanager
import time
import os
import logging
from dotenv import load_dotenv

# Configure logging at module level
logging.basicConfig(
    level=logging.WARNING,  # Set default to WARNING for all loggers
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Set your application loggers to DEBUG
logging.getLogger("src").setLevel(logging.DEBUG)  # All src.* modules
logging.getLogger("__main__").setLevel(logging.DEBUG)  # Main module if needed

# Keep third-party loggers at INFO or WARNING to reduce noise
logging.getLogger("uvicorn.access").setLevel(logging.INFO)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)
logging.getLogger("rasterio").setLevel(logging.WARNING)
logging.getLogger("pystac").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logging.getLogger("asyncio").setLevel(logging.WARNING)


# Initialize cache with in-memory backend at app startup
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[Any, Any]:
    # Load environment variables at startup
    env_path = os.path.join(os.path.dirname(__file__), "..", ".devcontainer", ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
        print(f"Loaded environment from {env_path}")
    else:
        print(f"No .env file found at {env_path}, using system environment variables")

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
# Note: allow_credentials=True is incompatible with allow_origins=["*"]
# When credentials are needed, explicit origins must be specified
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",
        "http://localhost:5500",
        "http://localhost:3000",
        "https://storage.googleapis.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
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
