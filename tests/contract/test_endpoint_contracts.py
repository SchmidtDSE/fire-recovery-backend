"""Contract tests for individual API endpoints."""

from typing import Any, Dict, Optional


def resolve_response_schema(
    openapi_schema: Dict[str, Any], path: str, method: str, status_code: str
) -> Optional[Dict[str, Any]]:
    """The JSON schema of one response, with a top-level $ref resolved."""
    responses = openapi_schema["paths"][path][method].get("responses", {})
    if status_code not in responses:
        return None
    content = responses[status_code].get("content", {})
    if "application/json" not in content:
        return None
    schema = content["application/json"]["schema"]
    if "$ref" in schema:
        ref_name = schema["$ref"].split("/")[-1]
        return openapi_schema["components"]["schemas"][ref_name]
    return schema


class TestHealthContract:
    """GET /health: liveness, no I/O."""

    PATH = "/health"

    def test_response_shape(self, openapi_schema: Dict[str, Any]) -> None:
        schema = resolve_response_schema(openapi_schema, self.PATH, "get", "200")
        assert schema is not None, "/health 200 response schema not found"
        assert schema.get("required") == ["status"]
        assert schema["properties"]["status"]["type"] == "string"


class TestVersionContract:
    """GET /version: what the deployment runs (D3, D18)."""

    PATH = "/version"

    def test_response_shape(self, openapi_schema: Dict[str, Any]) -> None:
        schema = resolve_response_schema(openapi_schema, self.PATH, "get", "200")
        assert schema is not None, "/version 200 response schema not found"
        assert "pipeline_version" in schema.get("required", [])
        assert schema["properties"]["pipeline_version"]["type"] == "integer"
        assert {"git_sha", "image"} <= set(schema["properties"])


def component(openapi_schema: Dict[str, Any], name: str) -> Dict[str, Any]:
    schema: Dict[str, Any] = openapi_schema["components"]["schemas"][name]
    return schema


class TestSubmitSeverityContract:
    """POST /v2/severity."""

    PATH = "/v2/severity"

    def test_request_fields(self, openapi_schema: Dict[str, Any]) -> None:
        body = openapi_schema["paths"][self.PATH]["post"]["requestBody"]
        ref = body["content"]["application/json"]["schema"]["$ref"]
        assert ref.endswith("/SeverityRequest")
        request = component(openapi_schema, "SeverityRequest")
        assert set(request["required"]) == {
            "fire_event_name",
            "geometry",
            "prefire_window",
            "postfire_window",
        }
        assert {"sensor", "email"} <= set(request["properties"])

    def test_fire_event_name_constraints_reach_the_schema(
        self, openapi_schema: Dict[str, Any]
    ) -> None:
        # Clients compile this pattern (D12, D20); it must be in the schema.
        name = component(openapi_schema, "SeverityRequest")["properties"][
            "fire_event_name"
        ]
        assert name["pattern"] == "^[A-Za-z0-9_-]+$"
        assert name["maxLength"] == 128

    def test_sensor_default(self, openapi_schema: Dict[str, Any]) -> None:
        sensor = component(openapi_schema, "SeverityRequest")["properties"]["sensor"]
        assert sensor["default"] == "sentinel-2"
        assert set(sensor["enum"]) == {"sentinel-2", "landsat"}

    def test_responses(self, openapi_schema: Dict[str, Any]) -> None:
        responses = openapi_schema["paths"][self.PATH]["post"]["responses"]
        assert {"200", "202", "422", "429", "503"} <= set(responses)
        accepted = resolve_response_schema(openapi_schema, self.PATH, "post", "202")
        assert accepted is not None
        assert {"job_id", "state", "grid", "status_url"} <= set(accepted["required"])
        result = resolve_response_schema(openapi_schema, self.PATH, "post", "200")
        assert result is not None
        assert {"job_id", "catalog_url", "assets"} <= set(result["required"])


class TestSeverityStatusContract:
    """GET /v2/severity/{job_id}."""

    PATH = "/v2/severity/{job_id}"

    def test_job_id_is_a_uuid(self, openapi_schema: Dict[str, Any]) -> None:
        (param,) = openapi_schema["paths"][self.PATH]["get"]["parameters"]
        assert param["name"] == "job_id"
        assert param["schema"]["pattern"].startswith("^[0-9a-f]{8}-")

    def test_response(self, openapi_schema: Dict[str, Any]) -> None:
        schema = resolve_response_schema(openapi_schema, self.PATH, "get", "200")
        assert schema is not None
        assert set(schema["properties"]["state"]["enum"]) == {
            "pending",
            "running",
            "succeeded",
            "failed",
        }
        assert "404" in openapi_schema["paths"][self.PATH]["get"]["responses"]
