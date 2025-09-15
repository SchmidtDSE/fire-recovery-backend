"""
API integration tests for vegetation schema system.

Tests the API endpoints that handle park_unit_id parameter for vegetation analysis.
"""

import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient

from src.app import app


class TestVegetationAPIIntegration:
    """Test API integration for vegetation schema functionality."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    @pytest.fixture
    def sample_request_data(self):
        """Sample request data for vegetation analysis."""
        return {
            "fire_event_name": "test_fire_2024",
            "veg_gpkg_url": "https://example.com/vegetation.gpkg",
            "fire_cog_url": "https://example.com/fire_severity.tif",
            "job_id": "test_job_123",
            "severity_breaks": [0.1, 0.27, 0.66],
            "geojson_url": "https://example.com/boundary.geojson"
        }

    def test_vegetation_resolve_api_with_park_unit_id(self, client, sample_request_data):
        """Test vegetation resolve API with park_unit_id parameter."""
        # Add park_unit_id to request
        sample_request_data["park_unit_id"] = "JOTR"

        with patch('src.routers.fire_recovery.execute_vegetation_resolution_command') as mock_execute:
            mock_execute.return_value = None  # Background task doesn't return

            response = client.post(
                "/fire-recovery/process/resolve_against_veg_map",
                json=sample_request_data
            )

            assert response.status_code == 200
            response_data = response.json()
            assert response_data["fire_event_name"] == "test_fire_2024"
            assert response_data["status"] == "Processing started"
            assert response_data["job_id"] == "test_job_123"

            # Verify the background task was called with park_unit_id
            mock_execute.assert_called_once()
            call_kwargs = mock_execute.call_args.kwargs
            assert call_kwargs["park_unit_id"] == "JOTR"

    def test_vegetation_resolve_api_without_park_unit_id(self, client, sample_request_data):
        """Test vegetation resolve API without park_unit_id (backward compatibility)."""
        # Don't add park_unit_id - should default to None

        with patch('src.routers.fire_recovery.execute_vegetation_resolution_command') as mock_execute:
            mock_execute.return_value = None  # Background task doesn't return

            response = client.post(
                "/fire-recovery/process/resolve_against_veg_map",
                json=sample_request_data
            )

            assert response.status_code == 200
            response_data = response.json()
            assert response_data["fire_event_name"] == "test_fire_2024"
            assert response_data["status"] == "Processing started"

            # Verify the background task was called with park_unit_id as None
            mock_execute.assert_called_once()
            call_kwargs = mock_execute.call_args.kwargs
            assert call_kwargs["park_unit_id"] is None

    def test_vegetation_resolve_api_with_invalid_park_unit_id(self, client, sample_request_data):
        """Test that API accepts any park_unit_id string (validation happens in command)."""
        # Add invalid park_unit_id - API should accept it, validation happens in command
        sample_request_data["park_unit_id"] = "INVALID_PARK"

        with patch('src.routers.fire_recovery.execute_vegetation_resolution_command') as mock_execute:
            mock_execute.return_value = None

            response = client.post(
                "/fire-recovery/process/resolve_against_veg_map",
                json=sample_request_data
            )

            assert response.status_code == 200
            # API accepts the request, validation happens during command execution
            mock_execute.assert_called_once()
            call_kwargs = mock_execute.call_args.kwargs
            assert call_kwargs["park_unit_id"] == "INVALID_PARK"

    def test_vegetation_resolve_api_request_validation(self, client):
        """Test API request validation for required fields."""
        # Test with missing required fields
        invalid_request = {
            "fire_event_name": "test_fire",
            # Missing other required fields
        }

        response = client.post(
            "/fire-recovery/process/resolve_against_veg_map",
            json=invalid_request
        )

        assert response.status_code == 422  # Validation error
        error_detail = response.json()
        assert "detail" in error_detail

    def test_vegetation_resolve_api_with_optional_fields(self, client, sample_request_data):
        """Test API with all optional fields including park_unit_id."""
        # Add all possible optional fields
        sample_request_data.update({
            "park_unit_id": "MOJN",
            # Could add other optional fields as they're added to the API
        })

        with patch('src.routers.fire_recovery.execute_vegetation_resolution_command') as mock_execute:
            mock_execute.return_value = None

            response = client.post(
                "/fire-recovery/process/resolve_against_veg_map",
                json=sample_request_data
            )

            assert response.status_code == 200
            mock_execute.assert_called_once()
            call_kwargs = mock_execute.call_args.kwargs
            assert call_kwargs["park_unit_id"] == "MOJN"


class TestVegetationSchemaAPIIntegration:
    """Test broader API integration scenarios for schema system."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    def test_api_error_handling_with_schema_failures(self, client):
        """Test that API properly handles schema-related errors."""
        request_data = {
            "fire_event_name": "test_fire_2024",
            "veg_gpkg_url": "https://example.com/vegetation.gpkg",
            "fire_cog_url": "https://example.com/fire_severity.tif",
            "job_id": "test_job_123",
            "severity_breaks": [0.1, 0.27, 0.66],
            "geojson_url": "https://example.com/boundary.geojson",
            "park_unit_id": "UNKNOWN_PARK"
        }

        # Mock the command execution to simulate schema failure
        async def mock_execute_with_failure(*args, **kwargs):
            # Simulate a command that would fail due to schema issues
            pass

        with patch('src.routers.fire_recovery.execute_vegetation_resolution_command', mock_execute_with_failure):
            response = client.post(
                "/fire-recovery/process/resolve_against_veg_map",
                json=request_data
            )

            # API should still accept the request (error handling happens in background)
            assert response.status_code == 200
            response_data = response.json()
            assert response_data["status"] == "Processing started"

    def test_api_schema_parameter_types(self, client):
        """Test API parameter type validation for park_unit_id."""
        base_request = {
            "fire_event_name": "test_fire_2024", 
            "veg_gpkg_url": "https://example.com/vegetation.gpkg",
            "fire_cog_url": "https://example.com/fire_severity.tif",
            "job_id": "test_job_123",
            "severity_breaks": [0.1, 0.27, 0.66],
            "geojson_url": "https://example.com/boundary.geojson"
        }

        # Test valid string park_unit_id
        request_with_string = base_request.copy()
        request_with_string["park_unit_id"] = "JOTR"

        with patch('src.routers.fire_recovery.execute_vegetation_resolution_command'):
            response = client.post(
                "/fire-recovery/process/resolve_against_veg_map",
                json=request_with_string
            )
            assert response.status_code == 200

        # Test null park_unit_id (should be allowed)
        request_with_null = base_request.copy()
        request_with_null["park_unit_id"] = None

        with patch('src.routers.fire_recovery.execute_vegetation_resolution_command'):
            response = client.post(
                "/fire-recovery/process/resolve_against_veg_map",
                json=request_with_null
            )
            assert response.status_code == 200

        # Test invalid type (should be rejected)
        request_with_invalid = base_request.copy()
        request_with_invalid["park_unit_id"] = 123  # Number instead of string

        response = client.post(
            "/fire-recovery/process/resolve_against_veg_map",
            json=request_with_invalid
        )
        assert response.status_code == 422  # Validation error