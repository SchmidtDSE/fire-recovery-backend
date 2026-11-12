"""The Fire Recovery Backend application.

The severity v2 routes are mounted here. The two endpoints defined in this
module do no I/O, so they answer even when storage or the cluster is degraded:
``/health`` for liveness and ``/version`` for provenance.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Literal, Optional

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from src.v2.service import api as severity_v2
from src.v2.constants import ENV_GIT_SHA, ENV_IMAGE, PIPELINE_VERSION

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logging.getLogger("src").setLevel(logging.DEBUG)
logging.getLogger("uvicorn.access").setLevel(logging.INFO)
logging.getLogger("uvicorn.error").setLevel(logging.INFO)

logger = logging.getLogger(__name__)

ENV_FILE = os.path.join(os.path.dirname(__file__), "..", ".devcontainer", ".env")

# Browser origins allowed to call the API: local frontend development and the
# frontend's bucket-hosted build.
CORS_ORIGINS = [
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "http://localhost:3000",
    "https://storage.googleapis.com",
]


class HealthResponse(BaseModel):
    """Liveness: the process is up and serving requests."""

    status: Literal["ok"] = Field(..., description="Always 'ok' when the API answers")


class VersionResponse(BaseModel):
    """What this deployment is running."""

    pipeline_version: int = Field(
        ..., description="Pipeline version; part of every job id (D3)"
    )
    git_sha: Optional[str] = Field(
        None, description="Commit the running image was built from, if known"
    )
    image: Optional[str] = Field(
        None, description="Image, by digest, that analysis Jobs run, if configured"
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Load local development settings, if present, before serving."""
    if os.path.exists(ENV_FILE):
        load_dotenv(ENV_FILE)
        logger.info(f"Loaded environment from {ENV_FILE}")
    yield


app = FastAPI(
    lifespan=lifespan,
    title="Fire Recovery Backend",
    description=(
        "Fire severity analysis: submit a perimeter and two date windows, poll a "
        "status URL, and get a Portolan catalog of dNBR, RdNBR and RBR."
    ),
    version="2.0.0",
)

# allow_credentials=True is incompatible with allow_origins=["*"], so origins
# are listed explicitly.
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

app.include_router(severity_v2.router)


@app.get("/", tags=["Root"])
async def root() -> dict[str, str]:
    return {
        "message": "Fire Recovery Backend",
        "docs_url": "/docs",
        "severity": "/v2/severity",
    }


# Not /healthz: Cloud Run's frontend answers some root paths ending in "z"
# itself, so /healthz never reaches the app there.
@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health() -> HealthResponse:
    """Liveness check. Touches no storage and no cluster."""
    return HealthResponse(status="ok")


@app.get("/version", response_model=VersionResponse, tags=["Health"])
async def version() -> VersionResponse:
    """The pipeline version and build this deployment runs. Does no I/O."""
    return VersionResponse(
        pipeline_version=PIPELINE_VERSION,
        git_sha=os.environ.get(ENV_GIT_SHA) or None,
        image=os.environ.get(ENV_IMAGE) or None,
    )
