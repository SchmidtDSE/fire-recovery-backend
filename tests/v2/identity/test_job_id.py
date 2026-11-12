"""Canonical form, job id and fire id."""

import json

import pytest
from pydantic import ValidationError

from src.v2.constants import PIPELINE_VERSION
from src.v2.identity.job_id import CanonicalRequest, canonical_request, fire_id
from src.v2.identity.request import SeverityRequest

BODY = {
    "fire_event_name": "Geology_Fire",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-116.098276, 33.929925],
                [-116.098276, 33.880794],
                [-116.019318, 33.880794],
                [-116.019318, 33.929925],
                [-116.098276, 33.929925],
            ]
        ],
    },
    "prefire_window": ["2023-06-01", "2023-06-09"],
    "postfire_window": ["2023-06-17", "2023-06-22"],
}


@pytest.fixture
def canonical() -> CanonicalRequest:
    return canonical_request(SeverityRequest.model_validate(BODY))


def test_canonical_json_is_sorted_and_compact(canonical: CanonicalRequest) -> None:
    text = canonical.to_json()
    assert " " not in text
    assert text == json.dumps(json.loads(text), sort_keys=True, separators=(",", ":"))
    assert json.loads(text)["operation"] == "fire_severity_v2"
    assert json.loads(text)["pipeline_version"] == PIPELINE_VERSION


def test_round_trip_through_json_keeps_the_id(canonical: CanonicalRequest) -> None:
    # The Job parses the canonical JSON it is given and must derive the same id.
    parsed = CanonicalRequest.model_validate_json(canonical.to_json())
    assert parsed == canonical
    assert parsed.job_id() == canonical.job_id()


def test_canonical_rejects_unknown_keys(canonical: CanonicalRequest) -> None:
    data = json.loads(canonical.to_json())
    data["geometry"] = {}
    with pytest.raises(ValidationError):
        CanonicalRequest.model_validate(data)


def test_pipeline_version_changes_the_id() -> None:
    request = SeverityRequest.model_validate(BODY)
    assert (
        canonical_request(request, PIPELINE_VERSION + 1).job_id()
        != canonical_request(request).job_id()
    )


def test_job_id_is_a_uuid5(canonical: CanonicalRequest) -> None:
    assert canonical.job_id()[14] == "5"


@pytest.mark.parametrize(
    "name, expected",
    [
        ("Geology_Fire", "geology-fire-0123abcd"),
        ("Dome-Fire_2024", "dome-fire-2024-0123abcd"),
        ("__Eureka__", "eureka-0123abcd"),
        ("a__-b", "a-b-0123abcd"),
        ("___", "fire-0123abcd"),
    ],
)
def test_fire_id(name: str, expected: str) -> None:
    assert fire_id(name, "0123abcd-0000-5000-8000-000000000000") == expected
