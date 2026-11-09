"""
OpenAPI schema validation tests.

These tests ensure the API's OpenAPI schema is valid, complete, and follows
the OpenAPI 3.x specification.
"""

import json
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from tests.contract.helpers import SchemaValidator

CRITICAL_ENDPOINTS = ["/healthz", "/version"]

DOCUMENTED_MODELS = ["HealthResponse", "VersionResponse"]


class TestOpenAPISchemaValidation:
    """Validate OpenAPI schema structure and completeness."""

    def test_openapi_endpoint_accessible(self, test_client: TestClient) -> None:
        """Test that /openapi.json endpoint is accessible."""
        response = test_client.get("/openapi.json")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"

    def test_schema_has_required_sections(self, openapi_schema: Dict[str, Any]) -> None:
        """Test schema has all required OpenAPI sections."""
        required_sections = ["openapi", "info", "paths", "components"]

        for section in required_sections:
            assert section in openapi_schema, f"Missing required section: {section}"

    def test_openapi_version_is_3x(self, openapi_schema: Dict[str, Any]) -> None:
        """Test that OpenAPI version is 3.x."""
        version = openapi_schema["openapi"]
        assert version.startswith("3."), f"Expected OpenAPI 3.x, got {version}"

    def test_api_metadata_complete(self, openapi_schema: Dict[str, Any]) -> None:
        """Test API metadata is complete."""
        info = openapi_schema["info"]

        assert "title" in info
        assert "version" in info
        assert info["title"] == "Fire Recovery Backend"

    def test_all_critical_endpoints_documented(
        self, openapi_schema: Dict[str, Any]
    ) -> None:
        """Test that all critical endpoints are documented."""
        paths = openapi_schema.get("paths", {})
        missing = [ep for ep in CRITICAL_ENDPOINTS if ep not in paths]
        assert not missing, f"Critical endpoints missing from schema: {missing}"

    def test_all_models_documented(self, openapi_schema: Dict[str, Any]) -> None:
        """Test that every request and response model is in components.schemas."""
        schemas = openapi_schema.get("components", {}).get("schemas", {})
        missing = [model for model in DOCUMENTED_MODELS if model not in schemas]
        assert not missing, f"Models missing from schema: {missing}"

    def test_schema_is_json_serializable(self, openapi_schema: Dict[str, Any]) -> None:
        """Test that schema can be serialized to JSON."""
        try:
            json_str = json.dumps(openapi_schema)
            assert len(json_str) > 0
        except (TypeError, ValueError) as e:
            pytest.fail(f"Schema is not JSON serializable: {e}")

    def test_schema_validation_passes(
        self, openapi_schema: Dict[str, Any], schema_validator: SchemaValidator
    ) -> None:
        """Test that schema passes validation checks."""
        is_valid, errors = schema_validator.validate_openapi_spec(openapi_schema)
        assert is_valid, f"Schema validation errors: {errors}"
