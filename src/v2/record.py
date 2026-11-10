"""The result record a Job writes as it ends: ``v2/jobs/{job_id}.json`` (design §1).

One record per job id, written once per run: "succeeded" with links, or
"failed" with a short error. A rerun after a failure overwrites it. The API
reads it for status and for cache hits. It never holds the requester's email.
"""

from datetime import datetime, timezone
from typing import Dict, Literal, Optional

from pydantic import BaseModel, Field

from src.v2.grid import Grid

JOBS_PREFIX = "v2/jobs"


def record_key(job_id: str) -> str:
    return f"{JOBS_PREFIX}/{job_id}.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class ResultRecord(BaseModel):
    """How one run of a job ended."""

    job_id: str
    state: Literal["succeeded", "failed"]
    finished_at: str = Field(default_factory=utc_now)
    pipeline_version: int
    fire_event_name: str
    fire_id: str
    git_sha: Optional[str] = None
    image: Optional[str] = None
    catalog_url: Optional[str] = None
    assets: Dict[str, str] = Field(
        default_factory=dict, description="Asset name to public URL"
    )
    scene_counts: Optional[Dict[str, int]] = None
    provider: Optional[str] = None
    grid: Optional[Grid] = None
    rashid_warnings: Optional[int] = None
    peak_rss_mib: Optional[int] = Field(
        default=None, description="Peak resident memory of the job's own process"
    )
    peak_child_rss_mib: Optional[int] = Field(
        default=None, description="Peak resident memory of its largest subprocess"
    )
    error: Optional[str] = Field(default=None, description="Short reason, when failed")
