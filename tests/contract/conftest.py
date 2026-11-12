"""Test fixtures for contract testing."""

import json
from pathlib import Path
from typing import Any, Dict, Optional

import pytest
from fastapi.testclient import TestClient

from tests.contract.helpers import SchemaComparator, SchemaValidator


@pytest.fixture
def test_client() -> TestClient:
    """FastAPI test client for contract testing."""
    from src.app import app

    return TestClient(app)


@pytest.fixture
def openapi_schema(test_client: TestClient) -> Dict[str, Any]:
    """Fetch current OpenAPI schema from the API."""
    response = test_client.get("/openapi.json")
    assert response.status_code == 200, "Failed to fetch OpenAPI schema"
    return response.json()


@pytest.fixture
def baseline_schema_path() -> Path:
    """Path to baseline schema file."""
    return Path(__file__).parent / "baseline_schema.json"


@pytest.fixture
def baseline_schema(baseline_schema_path: Path) -> Optional[Dict[str, Any]]:
    """Load baseline schema if it exists."""
    if not baseline_schema_path.exists():
        return None
    with open(baseline_schema_path, "r") as f:
        return json.load(f)


@pytest.fixture
def schema_comparator() -> SchemaComparator:
    """Schema comparison helper."""
    return SchemaComparator()


@pytest.fixture
def schema_validator() -> SchemaValidator:
    """Schema validation helper."""
    return SchemaValidator()
