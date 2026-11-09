"""The committed job id vectors pin validation, normalization, snapping and hashing."""

import json
from pathlib import Path
from typing import Any, Dict, List

import pytest

from src.v2.constants import PIPELINE_VERSION
from src.v2.job_id import canonical_request
from src.v2.request import SeverityRequest

VECTORS_PATH = Path(__file__).parent.parent / "fixtures" / "v2_job_id_vectors.json"
FIXTURE: Dict[str, Any] = json.loads(VECTORS_PATH.read_text())
VECTORS: List[Dict[str, Any]] = FIXTURE["vectors"]
IDS = {v["name"]: v["job_id"] for v in VECTORS}


def test_vectors_match_the_current_pipeline_version() -> None:
    assert FIXTURE["pipeline_version"] == PIPELINE_VERSION, (
        "PIPELINE_VERSION changed: regenerate with "
        "`pixi run python -m scripts.generate_v2_job_id_vectors`"
    )


@pytest.mark.parametrize("vector", VECTORS, ids=[v["name"] for v in VECTORS])
def test_vector_reproduces(vector: Dict[str, Any]) -> None:
    canonical = canonical_request(
        SeverityRequest.model_validate(vector["request_body"])
    )
    assert json.loads(canonical.to_json()) == vector["canonical"]
    assert canonical.job_id() == vector["job_id"]


@pytest.mark.parametrize(
    "vector",
    [v for v in VECTORS if "same_as" in v or "differs_from" in v],
    ids=lambda v: v["name"],
)
def test_vector_relations(vector: Dict[str, Any]) -> None:
    if "same_as" in vector:
        assert vector["job_id"] == IDS[vector["same_as"]]
    if "differs_from" in vector:
        assert vector["job_id"] != IDS[vector["differs_from"]]
