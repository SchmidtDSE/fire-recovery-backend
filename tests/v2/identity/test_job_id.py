"""Canonical form, job id and fire id."""

import json

import pytest
from pydantic import ValidationError

from src.v2.constants import PIPELINE_VERSION
from src.v2.identity.job_id import CanonicalRequest, canonical_request, fire_id
from src.v2.identity.request import SeverityRequest, quantize

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


def perimeter_body(ring: list[list[float]]) -> dict:
    return {
        **BODY,
        "geometry_kind": "perimeter",
        "geometry": {"type": "Polygon", "coordinates": [ring]},
    }


RING = BODY["geometry"]["coordinates"][0]  # type: ignore[index]


def job_id_of(body: dict) -> str:
    return canonical_request(SeverityRequest.model_validate(body)).job_id()


def test_an_aoi_form_has_no_perimeter_key(canonical: CanonicalRequest) -> None:
    # So every area-of-interest id is what it was before perimeters existed.
    assert canonical.perimeter is None
    assert "perimeter" not in json.loads(canonical.to_json())


def test_the_same_perimeter_gets_the_same_id() -> None:
    assert job_id_of(perimeter_body(RING)) == job_id_of(perimeter_body(RING))


def test_a_perimeter_and_an_aoi_of_one_shape_are_different_jobs(
    canonical: CanonicalRequest,
) -> None:
    assert job_id_of(perimeter_body(RING)) != canonical.job_id()


def test_perimeters_snapping_to_the_same_tiles_are_different_jobs() -> None:
    moved = [list(p) for p in RING]
    moved[1][1] += 0.0001
    moved[2][1] += 0.0001
    first = canonical_request(SeverityRequest.model_validate(perimeter_body(RING)))
    second = canonical_request(SeverityRequest.model_validate(perimeter_body(moved)))
    assert first.tiles == second.tiles
    assert first.job_id() != second.job_id()


def test_a_perimeter_is_normalized_like_the_bounds_and_otherwise_kept() -> None:
    with_z = [[x, y, 120.0] for x, y in RING]
    reversed_ring = list(reversed(RING))
    canonical = canonical_request(
        SeverityRequest.model_validate(perimeter_body(with_z))
    )
    assert canonical.perimeter == [[[(quantize(x), quantize(y)) for x, y in RING]]]
    # Z dropped and float noise below a micro-degree absorbed: the same id.
    noisy = [[x + 1e-9, y - 1e-9] for x, y in RING]
    assert canonical.job_id() == job_id_of(perimeter_body(noisy))
    # The ring is kept as sent, so another orientation is another perimeter.
    assert canonical.job_id() != job_id_of(perimeter_body(reversed_ring))


def test_a_perimeter_form_round_trips_to_the_same_id() -> None:
    canonical = canonical_request(SeverityRequest.model_validate(perimeter_body(RING)))
    again = CanonicalRequest.model_validate_json(canonical.to_json())
    assert again.job_id() == canonical.job_id()
    assert again.perimeter_geometry() == {
        "type": "MultiPolygon",
        "coordinates": [[RING]],
    }


def test_the_summary_counts_the_perimeter_instead_of_listing_it() -> None:
    canonical = canonical_request(SeverityRequest.model_validate(perimeter_body(RING)))
    summary = json.loads(canonical.summary())
    assert summary["perimeter"] == "5 vertices"
    assert summary["fire_event_name"] == "Geology_Fire"


def test_an_aoi_summary_is_its_form(canonical: CanonicalRequest) -> None:
    assert canonical.summary() == canonical.to_json()


def test_without_h3_resolutions_the_form_and_id_are_unchanged(
    canonical: CanonicalRequest,
) -> None:
    # So every id from before options existed, AOI or perimeter, still holds.
    assert "h3_resolutions" not in json.loads(canonical.to_json())
    no_options = canonical_request(
        SeverityRequest.model_validate({**BODY, "options": {}})
    )
    assert no_options.job_id() == canonical.job_id()
    perimeter = perimeter_body(RING)
    assert job_id_of({**perimeter, "options": {}}) == job_id_of(perimeter)


def test_h3_resolutions_are_part_of_the_id(canonical: CanonicalRequest) -> None:
    def with_levels(levels: list[int]) -> CanonicalRequest:
        body = {**BODY, "options": {"h3_resolutions": levels}}
        return canonical_request(SeverityRequest.model_validate(body))

    r13 = with_levels([13])
    assert json.loads(r13.to_json())["h3_resolutions"] == [13]
    assert r13.h3_resolutions == (13,)
    ids = {canonical.job_id(), r13.job_id(), with_levels([12, 13]).job_id()}
    assert len(ids) == 3
    # A set: order and repeats don't change the id.
    assert with_levels([13, 12, 12]).job_id() == with_levels([12, 13]).job_id()
    assert with_levels([13, 12]).h3_resolutions == (12, 13)
    assert CanonicalRequest.model_validate_json(r13.to_json()).job_id() == r13.job_id()


def test_a_canonical_form_holds_resolutions_sorted_and_unique(
    canonical: CanonicalRequest,
) -> None:
    for levels in ([13, 12], [12, 12]):
        form = {**json.loads(canonical.to_json()), "h3_resolutions": levels}
        with pytest.raises(ValidationError):
            CanonicalRequest.model_validate(form)
