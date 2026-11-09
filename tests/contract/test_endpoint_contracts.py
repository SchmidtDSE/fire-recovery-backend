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


class TestHealthzContract:
    """GET /healthz: liveness, no I/O."""

    PATH = "/healthz"

    def test_response_shape(self, openapi_schema: Dict[str, Any]) -> None:
        schema = resolve_response_schema(openapi_schema, self.PATH, "get", "200")
        assert schema is not None, "/healthz 200 response schema not found"
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
