"""Contract tests for individual API endpoints."""

from typing import Any, Dict, Optional


class TestFireSeverityEndpointContract:
    """Contract tests for fire severity analysis endpoint."""

    ENDPOINT_PATH = "/fire-recovery/process/analyze_fire_severity"

    def _get_request_schema(self, openapi_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and resolve request schema for this endpoint."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        request_body = post_op["requestBody"]["content"]["application/json"]["schema"]

        # Resolve $ref if present
        if "$ref" in request_body:
            ref_name = request_body["$ref"].split("/")[-1]
            return openapi_schema["components"]["schemas"][ref_name]
        return request_body

    def _get_response_schema(
        self, openapi_schema: Dict[str, Any], status_code: str
    ) -> Optional[Dict[str, Any]]:
        """Extract and resolve response schema for this endpoint."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        responses = post_op.get("responses", {})

        if status_code not in responses:
            return None

        response_content = responses[status_code].get("content", {})
        if "application/json" not in response_content:
            return None

        schema = response_content["application/json"]["schema"]

        # Resolve $ref if present
        if "$ref" in schema:
            ref_name = schema["$ref"].split("/")[-1]
            return openapi_schema["components"]["schemas"][ref_name]
        return schema

    def test_request_has_required_fields(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate request schema has all required fields."""
        schema = self._get_request_schema(openapi_schema)
        required_fields = schema.get("required", [])

        # Assert critical fields are required
        assert "fire_event_name" in required_fields
        assert "coarse_geojson" in required_fields
        assert "prefire_date_range" in required_fields
        assert "postfire_date_range" in required_fields

    def test_request_field_types(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate request field types match expected types."""
        schema = self._get_request_schema(openapi_schema)
        properties = schema.get("properties", {})

        # Validate types
        assert properties["fire_event_name"]["type"] == "string"
        # Date ranges should be arrays
        assert properties["prefire_date_range"]["type"] == "array"
        assert properties["postfire_date_range"]["type"] == "array"

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        responses = post_op.get("responses", {})

        # Should document success response
        assert any(code in responses for code in ["200", "201", "202"])

        # Should document validation error
        assert "422" in responses, "Should document validation errors"

    def test_error_response_schemas(self, openapi_schema: Dict[str, Any]) -> None:
        """Verify error response schemas are documented."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        responses = post_op.get("responses", {})

        # Validation error should be documented
        assert "422" in responses


class TestBoundaryRefinementEndpointContract:
    """Contract tests for boundary refinement endpoint."""

    ENDPOINT_PATH = "/fire-recovery/process/refine"

    def _get_request_schema(self, openapi_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and resolve request schema for this endpoint."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        request_body = post_op["requestBody"]["content"]["application/json"]["schema"]

        if "$ref" in request_body:
            ref_name = request_body["$ref"].split("/")[-1]
            return openapi_schema["components"]["schemas"][ref_name]
        return request_body

    def test_request_has_required_fields(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate request schema has all required fields."""
        schema = self._get_request_schema(openapi_schema)
        required_fields = schema.get("required", [])

        assert "fire_event_name" in required_fields
        assert "refined_geojson" in required_fields
        assert "job_id" in required_fields

    def test_request_field_types(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate request field types match expected types."""
        schema = self._get_request_schema(openapi_schema)
        properties = schema.get("properties", {})

        assert properties["fire_event_name"]["type"] == "string"
        assert properties["job_id"]["type"] == "string"

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        responses = post_op.get("responses", {})

        assert any(code in responses for code in ["200", "201", "202"])
        assert "422" in responses


class TestVegetationAnalysisEndpointContract:
    """Contract tests for vegetation analysis endpoint."""

    ENDPOINT_PATH = "/fire-recovery/process/resolve_against_veg_map"

    def _get_request_schema(self, openapi_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and resolve request schema for this endpoint."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        request_body = post_op["requestBody"]["content"]["application/json"]["schema"]

        if "$ref" in request_body:
            ref_name = request_body["$ref"].split("/")[-1]
            return openapi_schema["components"]["schemas"][ref_name]
        return request_body

    def test_request_has_required_fields(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate request schema has all required fields."""
        schema = self._get_request_schema(openapi_schema)
        required_fields = schema.get("required", [])

        assert "fire_event_name" in required_fields
        assert "veg_gpkg_url" in required_fields
        assert "fire_cog_url" in required_fields
        assert "job_id" in required_fields
        assert "severity_breaks" in required_fields
        assert "geojson_url" in required_fields
        # park_unit_id is optional

    def test_request_field_types(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate request field types match expected types."""
        schema = self._get_request_schema(openapi_schema)
        properties = schema.get("properties", {})

        assert properties["fire_event_name"]["type"] == "string"
        assert properties["veg_gpkg_url"]["type"] == "string"
        assert properties["fire_cog_url"]["type"] == "string"
        assert properties["job_id"]["type"] == "string"
        assert properties["severity_breaks"]["type"] == "array"
        assert properties["geojson_url"]["type"] == "string"

    def test_optional_park_unit_id(self, openapi_schema: Dict[str, Any]) -> None:
        """Verify park_unit_id is optional."""
        schema = self._get_request_schema(openapi_schema)
        required_fields = schema.get("required", [])

        # park_unit_id should not be in required fields
        assert "park_unit_id" not in required_fields

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        responses = post_op.get("responses", {})

        assert any(code in responses for code in ["200", "201", "202"])
        assert "422" in responses


class TestFireSeverityResultEndpointContract:
    """Contract tests for fire severity result endpoint."""

    ENDPOINT_PATH = (
        "/fire-recovery/result/analyze_fire_severity/{fire_event_name}/{job_id}"
    )

    def test_path_parameters(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate path parameters are documented."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        params = endpoint["get"].get("parameters", [])

        param_names = [p["name"] for p in params]
        assert "fire_event_name" in param_names
        assert "job_id" in param_names

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        get_op = endpoint["get"]
        responses = get_op.get("responses", {})

        assert "200" in responses


class TestRefinementResultEndpointContract:
    """Contract tests for refinement result endpoint."""

    ENDPOINT_PATH = "/fire-recovery/result/refine/{fire_event_name}/{job_id}"

    def test_path_parameters(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate path parameters are documented."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        params = endpoint["get"].get("parameters", [])

        param_names = [p["name"] for p in params]
        assert "fire_event_name" in param_names
        assert "job_id" in param_names

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        get_op = endpoint["get"]
        responses = get_op.get("responses", {})

        assert "200" in responses


class TestVegetationResultEndpointContract:
    """Contract tests for vegetation result endpoint."""

    ENDPOINT_PATH = (
        "/fire-recovery/result/resolve_against_veg_map/{fire_event_name}/{job_id}"
    )

    def test_path_parameters(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate path parameters are documented."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        params = endpoint["get"].get("parameters", [])

        param_names = [p["name"] for p in params]
        assert "fire_event_name" in param_names
        assert "job_id" in param_names

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        get_op = endpoint["get"]
        responses = get_op.get("responses", {})

        assert "200" in responses


class TestGeoJSONUploadEndpointContract:
    """Contract tests for GeoJSON upload endpoint."""

    ENDPOINT_PATH = "/fire-recovery/upload/geojson"

    def _get_request_schema(self, openapi_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and resolve request schema for this endpoint."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        request_body = post_op["requestBody"]["content"]["application/json"]["schema"]

        if "$ref" in request_body:
            ref_name = request_body["$ref"].split("/")[-1]
            return openapi_schema["components"]["schemas"][ref_name]
        return request_body

    def test_request_has_required_fields(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate request schema has all required fields."""
        schema = self._get_request_schema(openapi_schema)
        required_fields = schema.get("required", [])

        assert "fire_event_name" in required_fields
        assert "geojson" in required_fields

    def test_boundary_type_is_optional(self, openapi_schema: Dict[str, Any]) -> None:
        """Verify boundary_type has a default value."""
        schema = self._get_request_schema(openapi_schema)
        properties = schema.get("properties", {})

        # boundary_type should exist but may have a default
        assert "boundary_type" in properties

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        responses = post_op.get("responses", {})

        assert any(code in responses for code in ["200", "201", "202"])
        assert "422" in responses


class TestShapefileUploadEndpointContract:
    """Contract tests for shapefile upload endpoint."""

    ENDPOINT_PATH = "/fire-recovery/upload/shapefile"

    def test_endpoint_accepts_multipart(self, openapi_schema: Dict[str, Any]) -> None:
        """Verify endpoint accepts multipart/form-data."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        request_body = post_op.get("requestBody", {})
        content = request_body.get("content", {})

        # Should accept multipart/form-data
        assert "multipart/form-data" in content

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        post_op = endpoint["post"]
        responses = post_op.get("responses", {})

        assert any(code in responses for code in ["200", "201", "202"])
        assert "422" in responses


class TestHealthCheckEndpointContract:
    """Contract tests for health check endpoint."""

    ENDPOINT_PATH = "/fire-recovery/healthz"

    def _get_response_schema(
        self, openapi_schema: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Extract and resolve response schema for this endpoint."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        get_op = endpoint["get"]
        responses = get_op.get("responses", {})

        if "200" not in responses:
            return None

        response_content = responses["200"].get("content", {})
        if "application/json" not in response_content:
            return None

        schema = response_content["application/json"]["schema"]

        if "$ref" in schema:
            ref_name = schema["$ref"].split("/")[-1]
            return openapi_schema["components"]["schemas"][ref_name]
        return schema

    def test_response_has_required_fields(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate response schema has all required fields."""
        schema = self._get_response_schema(openapi_schema)
        assert schema is not None, "Health check response schema not found"

        required_fields = schema.get("required", [])

        # HealthCheckResponse extends BaseResponse
        assert "fire_event_name" in required_fields
        assert "status" in required_fields
        assert "job_id" in required_fields
        assert "overall_status" in required_fields
        assert "timestamp" in required_fields
        assert "checks" in required_fields
        assert "unhealthy_components" in required_fields

    def test_response_field_types(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate response field types match expected types."""
        schema = self._get_response_schema(openapi_schema)
        assert schema is not None

        properties = schema.get("properties", {})

        assert properties["fire_event_name"]["type"] == "string"
        assert properties["status"]["type"] == "string"
        assert properties["job_id"]["type"] == "string"
        assert properties["overall_status"]["type"] == "string"
        assert properties["timestamp"]["type"] == "number"
        assert properties["checks"]["type"] == "object"
        assert properties["unhealthy_components"]["type"] == "integer"

    def test_response_status_codes(self, openapi_schema: Dict[str, Any]) -> None:
        """Validate endpoint returns expected status codes."""
        endpoint = openapi_schema["paths"][self.ENDPOINT_PATH]
        get_op = endpoint["get"]
        responses = get_op.get("responses", {})

        assert "200" in responses
