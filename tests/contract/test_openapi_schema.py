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

        critical_endpoints = [
            "/fire-recovery/process/analyze_fire_severity",
            "/fire-recovery/process/refine",
            "/fire-recovery/process/resolve_against_veg_map",
            "/fire-recovery/result/analyze_fire_severity/{fire_event_name}/{job_id}",
            "/fire-recovery/result/refine/{fire_event_name}/{job_id}",
            "/fire-recovery/result/resolve_against_veg_map/{fire_event_name}/{job_id}",
            "/fire-recovery/upload/geojson",
            "/fire-recovery/upload/shapefile",
            "/fire-recovery/healthz",
        ]

        missing_endpoints = [ep for ep in critical_endpoints if ep not in paths]

        assert not missing_endpoints, (
            f"Critical endpoints missing from schema: {missing_endpoints}"
        )

    def test_all_response_models_documented(
        self, openapi_schema: Dict[str, Any]
    ) -> None:
        """Test that all response models are in components.schemas."""
        schemas = openapi_schema.get("components", {}).get("schemas", {})

        expected_response_models = [
            "TaskPendingResponse",
            "ProcessingStartedResponse",
            "RefinedBoundaryResponse",
            "FireSeverityResponse",
            "VegMapMatrixResponse",
            "UploadedGeoJSONResponse",
            "UploadedShapefileZipResponse",
            "HealthCheckResponse",
        ]

        missing_models = [
            model for model in expected_response_models if model not in schemas
        ]

        assert not missing_models, (
            f"Response models missing from schema: {missing_models}"
        )

    def test_all_request_models_documented(
        self, openapi_schema: Dict[str, Any]
    ) -> None:
        """Test that all request models are in components.schemas."""
        schemas = openapi_schema.get("components", {}).get("schemas", {})

        expected_request_models = [
            "ProcessingRequest",
            "RefineRequest",
            "VegMapResolveRequest",
            "GeoJSONUploadRequest",
        ]

        missing_models = [
            model for model in expected_request_models if model not in schemas
        ]

        assert not missing_models, (
            f"Request models missing from schema: {missing_models}"
        )

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
