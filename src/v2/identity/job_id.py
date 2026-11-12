"""Deterministic job identity (design §6).

A job id is the uuid5 of a canonical JSON form of the request. The form holds
only what the product depends on: the operation, the pipeline version, the
name, the sensor, the two windows verbatim, and the WebMercatorQuad tile range
the buffered perimeter snaps to. Perimeters that snap to the same tiles share
an id, and so share one published analysis. The Job receives this form and
nothing else, so everything it publishes is a function of the key.

Name handling (D8, D20), "only success is a cache hit" (D6) and committed
vectors follow deterministic-job-id-2026-09.
"""

import json
import re
import uuid
from typing import Final, Literal, Optional

from pydantic import BaseModel, ConfigDict

from src.util.geo_ops import Bounds, buffered_bounds
from src.v2.constants import PIPELINE_VERSION
from src.v2.identity.grid import TileRange, snap_tiles
from src.v2.identity.request import SeverityRequest
from src.v2.sensors import Sensor

# Hardcoded, identical everywhere, never changed: every v2 id is derived under it.
NAMESPACE = uuid.UUID("231ee4f8-4e8e-4430-a29a-a947b57488bb")
OPERATION: Final = "fire_severity_v2"
FIRE_ID_JOB_PREFIX = 8


class CanonicalRequest(BaseModel):
    """The canonical request: the job id's preimage and the Job's only input."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    operation: Literal["fire_severity_v2"]
    pipeline_version: int
    fire_event_name: str
    sensor: Sensor
    prefire_window: tuple[str, str]
    postfire_window: tuple[str, str]
    tiles: TileRange

    def to_json(self) -> str:
        """Sorted keys and no whitespace: the exact string the id hashes."""
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )

    def job_id(self) -> str:
        return str(uuid.uuid5(NAMESPACE, self.to_json()))


def buffered_box(request: SeverityRequest) -> Bounds:
    """The request's normalized bounds with v1's metric buffer (geo_ops)."""
    padded, _ = buffered_bounds(request.bounds_geometry())
    return padded


def canonical_request(
    request: SeverityRequest,
    pipeline_version: int = PIPELINE_VERSION,
    padded: Optional[Bounds] = None,
) -> CanonicalRequest:
    """Canonicalize a validated request: buffer its bounds and snap them to tiles.

    Raises:
        ValueError: if the buffered box falls outside the tile grid.
    """
    padded = padded if padded is not None else buffered_box(request)
    return CanonicalRequest(
        operation=OPERATION,
        pipeline_version=pipeline_version,
        fire_event_name=request.fire_event_name,
        sensor=request.sensor,
        prefire_window=(request.prefire_window[0], request.prefire_window[1]),
        postfire_window=(request.postfire_window[0], request.postfire_window[1]),
        tiles=snap_tiles(padded),
    )


def fire_id(fire_event_name: str, job_id: str) -> str:
    """The analysis's catalog and collection id: ``{slug(name)}-{job_id[:8]}``.

    Deterministic and unique per job id. The slug is lowercase with '-' only,
    a safe id for STAC and object paths.
    """
    slug = re.sub(r"-+", "-", fire_event_name.lower().replace("_", "-")).strip("-")
    return f"{slug or 'fire'}-{job_id[:FIRE_ID_JOB_PREFIX]}"
