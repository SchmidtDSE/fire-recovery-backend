"""The severity v2 API: submit an analysis, poll its status (design §6).

The API keeps no job state. A succeeded result record is a cache hit, served
from the record. Otherwise the job's own state answers, from the launcher, and
a submission with nothing running starts a job named after the job id.
"""

import logging
import os
from functools import lru_cache
from typing import Annotated, Any, Dict, Literal, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.v2.access import (
    RateLimiter,
    client_ip,
    load_key_hashes,
    valid_api_key,
)
from src.v2.constants import (
    ANONYMOUS_STARTS_PER_WINDOW,
    BUSY_RETRY_AFTER_SECONDS,
    ENV_API_URL,
    ENV_IMAGE,
    ENV_TARGET,
    LOCAL_TARGET,
    MAX_ACTIVE_JOBS,
    MAX_BUFFERED_MEGAPIXELS,
    RATE_WINDOW_SECONDS,
)
from src.v2.grid import Grid, TileRange, buffered_megapixels, raster_grid
from src.v2.job_id import buffered_box, canonical_request
from src.v2.k8s import KubernetesLauncher, TargetProfile
from src.v2.launcher import DispatchUnavailable, JobSpec, Launcher, SubprocessLauncher
from src.v2.record import record_key
from src.v2.request import SeverityRequest
from src.v2.sensors import SENSOR_GRIDS
from src.v2.store import Store

logger = logging.getLogger(__name__)

JOB_ID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

router = APIRouter(prefix="/v2", tags=["Severity v2"])


class GridInfo(BaseModel):
    """The output raster's grid and the tile range it came from."""

    epsg: int
    resolution: int
    bounds: tuple[int, int, int, int]
    width: int
    height: int
    tiles: TileRange

    @classmethod
    def of(cls, grid: Grid, tiles: TileRange) -> "GridInfo":
        return cls(**grid.model_dump(), tiles=tiles)


class SeverityAccepted(BaseModel):
    """The analysis is queued or running: poll the status URL."""

    job_id: str
    state: Literal["pending", "running"]
    grid: GridInfo
    status_url: str


class SeverityResult(BaseModel):
    """The analysis already exists: links to its catalog and assets."""

    job_id: str
    state: Literal["succeeded"]
    grid: GridInfo
    catalog_url: str
    assets: Dict[str, str]


class SeverityStatus(BaseModel):
    """Where an analysis stands."""

    job_id: str
    state: Literal["pending", "running", "succeeded", "failed"]
    fire_event_name: Optional[str] = None
    catalog_url: Optional[str] = None
    assets: Dict[str, str] = Field(default_factory=dict)
    scene_counts: Optional[Dict[str, int]] = None
    finished_at: Optional[str] = None
    pipeline_version: Optional[int] = None
    error: Optional[str] = Field(
        default=None,
        description="Why it failed: the job's own reason, or the platform's (e.g. OOMKilled)",
    )


class ErrorDetail(BaseModel):
    detail: str


@lru_cache(maxsize=1)
def configured_store() -> Store:
    return Store.from_env()


@lru_cache(maxsize=1)
def configured_launcher() -> Launcher:
    target = os.environ.get(ENV_TARGET) or LOCAL_TARGET
    if target == LOCAL_TARGET:
        return SubprocessLauncher()
    profile = TargetProfile.load(target)
    # The deploy pins the image by digest; otherwise the profile names one.
    return KubernetesLauncher(profile, os.environ.get(ENV_IMAGE) or profile.image)


def get_store() -> Store:
    """The bucket holding the result records."""
    try:
        return configured_store()
    except ValueError as e:
        logger.error(f"Storage is not configured: {e}")
        raise HTTPException(503, "The result store is not configured") from e


def get_launcher() -> Launcher:
    """The launcher for SEVERITY_TARGET: a target profile's name, or "local"."""
    try:
        return configured_launcher()
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Unusable {ENV_TARGET}: {e}")
        raise HTTPException(503, "Analysis dispatch is not configured") from e


@lru_cache(maxsize=1)
def get_rate_limiter() -> RateLimiter:
    return RateLimiter(ANONYMOUS_STARTS_PER_WINDOW, RATE_WINDOW_SECONDS)


def get_api_key_hashes() -> set[str]:
    return load_key_hashes()


def status_url(request: Request, job_id: str) -> str:
    base = os.environ.get(ENV_API_URL)
    if base:
        return f"{base.rstrip('/')}/v2/severity/{job_id}"
    return str(request.url_for("severity_status", job_id=job_id))


def read_record(store: Store, job_id: str) -> Optional[Dict[str, Any]]:
    try:
        return store.get_json(record_key(job_id))
    except Exception as e:
        logger.error(f"Reading the record for {job_id} failed: {e}")
        raise HTTPException(503, "The result store is unavailable") from e


def accepted(job_id: str, state: str, grid: GridInfo, url: str) -> JSONResponse:
    body = SeverityAccepted(job_id=job_id, state=state, grid=grid, status_url=url)  # type: ignore[arg-type]
    return JSONResponse(body.model_dump(mode="json"), status_code=202)


def busy(detail: str, retry_after: int) -> JSONResponse:
    return JSONResponse(
        {"detail": detail}, status_code=429, headers={"Retry-After": str(retry_after)}
    )


@router.post(
    "/severity",
    response_model=None,
    responses={
        200: {"model": SeverityResult, "description": "Already computed: the links"},
        202: {
            "model": SeverityAccepted,
            "description": "Queued or running: poll status_url",
        },
        422: {"description": "Invalid request, or an area beyond the limits"},
        429: {
            "model": ErrorDetail,
            "description": "Too many requests; see Retry-After",
        },
        503: {
            "model": ErrorDetail,
            "description": "Storage or job dispatch unavailable",
        },
    },
)
def submit_severity(
    body: SeverityRequest,
    request: Request,
    store: Store = Depends(get_store),
    launcher: Launcher = Depends(get_launcher),
    limiter: RateLimiter = Depends(get_rate_limiter),
    key_hashes: set[str] = Depends(get_api_key_hashes),
    x_api_key: Annotated[Optional[str], Header()] = None,
) -> JSONResponse:
    """Submit an analysis, or get the existing one for the same key.

    Requests whose perimeters snap to the same tiles, with the same name,
    windows and sensor, share one job id and one analysis. The status URL
    always works; the optional email reaches the requester whose submission
    starts the job.
    """
    resolution = SENSOR_GRIDS[body.sensor].resolution
    padded = buffered_box(body)
    megapixels = buffered_megapixels(padded, resolution)
    if megapixels > MAX_BUFFERED_MEGAPIXELS:
        raise HTTPException(
            422,
            f"The buffered area is {megapixels:.0f} Mpx at {resolution} m; the limit "
            f"is {MAX_BUFFERED_MEGAPIXELS:.0f} Mpx",
        )
    try:
        canonical = canonical_request(body, padded=padded)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    job_id = canonical.job_id()
    grid = GridInfo.of(raster_grid(canonical.tiles, canonical.sensor), canonical.tiles)
    url = status_url(request, job_id)

    record = read_record(store, job_id)
    if record and record.get("state") == "succeeded":
        result = SeverityResult(
            job_id=job_id,
            state="succeeded",
            grid=grid,
            catalog_url=record["catalog_url"],
            assets=record.get("assets", {}),
        )
        return JSONResponse(result.model_dump(mode="json"), status_code=200)

    try:
        status = launcher.status(job_id)
        if status.active:
            return accepted(job_id, status.phase, grid, url)

        if launcher.active_count() >= MAX_ACTIVE_JOBS:
            return busy(
                "The maximum number of analyses is running", BUSY_RETRY_AFTER_SECONDS
            )
        if not valid_api_key(x_api_key, key_hashes):
            allowed, retry_after = limiter.allow(client_ip(request))
            if not allowed:
                return busy("Too many new analyses from this address", retry_after)

        if status.finished:
            # A failed run, or a finished Job whose record says it failed: run again.
            launcher.delete(job_id)
        started = launcher.start(
            JobSpec(
                job_id=job_id,
                canonical_json=canonical.to_json(),
                status_url=url,
                notify_email=body.email,
            )
        )
    except DispatchUnavailable as e:
        logger.error(f"Dispatch unavailable for {job_id}: {e}")
        raise HTTPException(503, "Analysis dispatch is unavailable") from e
    if started:
        logger.info(f"Started job {job_id} for {canonical.fire_event_name}")
    return accepted(job_id, "pending", grid, url)


@router.get(
    "/severity/{job_id}",
    name="severity_status",
    response_model=SeverityStatus,
    responses={404: {"model": ErrorDetail}, 503: {"model": ErrorDetail}},
)
def get_severity_status(
    job_id: Annotated[str, Path(pattern=JOB_ID_PATTERN)],
    store: Store = Depends(get_store),
    launcher: Launcher = Depends(get_launcher),
) -> SeverityStatus:
    """An analysis's state, its links when done, and its error when failed."""
    record = read_record(store, job_id)
    if record and record.get("state") == "succeeded":
        return status_from_record(job_id, record)
    try:
        status = launcher.status(job_id)
    except DispatchUnavailable as e:
        if record:
            return status_from_record(job_id, record)
        raise HTTPException(503, "Analysis dispatch is unavailable") from e
    if status.active:
        return SeverityStatus(job_id=job_id, state=status.phase)  # type: ignore[arg-type]
    if record:
        return status_from_record(job_id, record)
    if status.phase == "failed":
        return SeverityStatus(job_id=job_id, state="failed", error=status.reason)
    if status.phase == "succeeded":
        return SeverityStatus(
            job_id=job_id, state="failed", error="The job ended without a result record"
        )
    raise HTTPException(404, f"No analysis {job_id}")


def status_from_record(job_id: str, record: Dict[str, Any]) -> SeverityStatus:
    return SeverityStatus(
        job_id=job_id,
        state=record["state"],
        fire_event_name=record.get("fire_event_name"),
        catalog_url=record.get("catalog_url"),
        assets=record.get("assets", {}),
        scene_counts=record.get("scene_counts"),
        finished_at=record.get("finished_at"),
        pipeline_version=record.get("pipeline_version"),
        error=record.get("error"),
    )
