import pytest
from fastapi.testclient import TestClient
from src.app import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def valid_geometry():
    return {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
    }


@pytest.fixture
def valid_request_body(valid_geometry):
    return {
        "geometry": valid_geometry,
        "prefire_date_range": ["2023-01-01", "2023-12-31"],
        "postfire_date_range": ["2024-01-01", "2024-12-31"],
    }
