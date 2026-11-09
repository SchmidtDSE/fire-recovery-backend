"""Request validation and geometry normalization."""

import copy
from typing import Any, Dict

import pytest
from pydantic import ValidationError

from src.v2.request import SeverityRequest, quantize

BODY: Dict[str, Any] = {
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


def body(**changes: Any) -> Dict[str, Any]:
    out = copy.deepcopy(BODY)
    out.update(changes)
    return out


def box(west: float, south: float, east: float, north: float) -> Dict[str, Any]:
    ring = [[west, south], [east, south], [east, north], [west, north], [west, south]]
    return {"type": "Polygon", "coordinates": [ring]}


def test_valid_request_defaults_sensor() -> None:
    request = SeverityRequest.model_validate(BODY)
    assert request.sensor == "sentinel-2"
    assert request.email is None


@pytest.mark.parametrize(
    "changes",
    [
        {"fire_event_name": "Geology Fire"},
        {"fire_event_name": ""},
        {"fire_event_name": "a/b"},
        {"fire_event_name": "x" * 129},
        {"sensor": "modis"},
        {"email": "not-an-address"},
        {"prefire_window": ["2023-06-01"]},
        {"prefire_window": ["2023-06-09", "2023-06-01"]},
        {"prefire_window": ["June 1", "2023-06-09"]},
        {"prefire_window": ["2023-01-01", "2023-04-01"]},
        {"postfire_window": ["2023-06-05", "2023-06-22"]},
        {"geometry": {"type": "Point", "coordinates": [-116.0, 33.9]}},
        {
            "geometry": {
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "Point", "coordinates": [-116.0, 33.9]},
            }
        },
        {"geometry": {"type": "Feature", "properties": {}, "geometry": None}},
        {"geometry": box(-116.1, 85.06, -116.0, 85.08)},
        {
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    box(179.9, 10.0, 180.0, 10.1)["coordinates"],
                    box(-180.0, 10.0, -179.9, 10.1)["coordinates"],
                ],
            }
        },
    ],
    ids=lambda c: next(iter(c)) + ":" + str(next(iter(c.values())))[:24],
)
def test_rejects(changes: Dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        SeverityRequest.model_validate(body(**changes))


def test_accepts_name_at_max_length_and_a_valid_email() -> None:
    request = SeverityRequest.model_validate(
        body(fire_event_name="x" * 128, email="someone@example.org")
    )
    assert request.email == "someone@example.org"


def test_accepts_one_day_and_ninety_day_windows() -> None:
    SeverityRequest.model_validate(
        body(
            prefire_window=["2023-03-01", "2023-05-29"],
            postfire_window=["2023-06-17", "2023-06-17"],
        )
    )


def test_windows_may_touch() -> None:
    SeverityRequest.model_validate(
        body(
            prefire_window=["2023-06-01", "2023-06-09"],
            postfire_window=["2023-06-10", "2023-06-22"],
        )
    )


@pytest.mark.parametrize(
    "degrees, micro",
    [
        (0.0078125, 7813),
        (-0.0078125, -7812),
        (180.0, 180000000),
        (-90.0, -90000000),
        (-116.0, -116000000),
        (1e-7, 0),
    ],
)
def test_quantize_rounds_half_up(degrees: float, micro: int) -> None:
    assert quantize(degrees) == micro


def test_bounds_are_quantized_and_drop_z() -> None:
    ring = [
        [-116.0000004, 33.0, 900.0],
        [-115.9, 33.0, 0.0],
        [-115.9, 33.1, 1.0],
        [-116.0000004, 33.1, 2.0],
        [-116.0000004, 33.0, 900.0],
    ]
    request = SeverityRequest.model_validate(
        body(geometry={"type": "Polygon", "coordinates": [ring]})
    )
    assert request.bounds() == (-116.0, 33.0, -115.9, 33.1)


def test_multipolygon_bounds_span_every_part() -> None:
    geometry = {
        "type": "MultiPolygon",
        "coordinates": [
            box(-116.1, 33.8, -116.0, 33.9)["coordinates"],
            box(-115.8, 34.0, -115.7, 34.2)["coordinates"],
        ],
    }
    request = SeverityRequest.model_validate(body(geometry=geometry))
    assert request.bounds() == (-116.1, 33.8, -115.7, 34.2)
