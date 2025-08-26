import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from fastapi.testclient import TestClient
import uuid


class TestHealthEndpoint:
    """Integration tests for the /healthz endpoint using command pattern"""

    @patch("src.config.storage.get_storage")
    def test_health_check_success(self, mock_get_storage: MagicMock) -> None:
        """Test successful health check endpoint"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry with get_available_indices method
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = [
            "nbr",
            "dnbr",
            "rdnbr",
            "rbr",
        ]

        # Patch the router dependencies
        with (
            patch("src.routers.fire_recovery.storage_factory") as mock_storage_factory,
            patch("src.routers.fire_recovery.stac_manager", mock_stac_manager),
            patch("src.routers.fire_recovery.index_registry", mock_index_registry),
        ):
            mock_storage_factory.get_temp_storage.return_value = mock_storage

            # Import app after patching to avoid initialization issues
            from src.app import app

            client = TestClient(app)

            # Make the request
            response = client.get("/fire-recovery/healthz")

            # Verify response structure
            assert response.status_code == 200
            data = response.json()

            # Check response contract (HealthCheckResponse)
            required_fields = [
                "fire_event_name",
                "status",
                "job_id",
                "overall_status",
                "timestamp",
                "checks",
                "unhealthy_components",
            ]
            for field in required_fields:
                assert field in data, f"Missing required field: {field}"

            # Verify values
            assert data["fire_event_name"] == "health-check"
            assert data["status"] in ["healthy", "unhealthy"]
            assert isinstance(data["job_id"], str)
            assert len(data["job_id"]) > 0  # Should be a UUID
            assert data["overall_status"] in ["healthy", "unhealthy"]
            assert isinstance(data["timestamp"], (int, float))
            assert isinstance(data["checks"], dict)
            assert isinstance(data["unhealthy_components"], int)

            # Verify component checks
            expected_components = ["storage", "stac_manager", "index_registry"]
            for component in expected_components:
                assert component in data["checks"]
                assert "status" in data["checks"][component]

            # Since we mocked everything, should be healthy
            assert data["overall_status"] == "healthy"
            assert data["unhealthy_components"] == 0

            # Verify index registry was called
            mock_index_registry.get_available_indices.assert_called_once()

    @patch("src.config.storage.get_storage")
    def test_health_check_with_unhealthy_component(
        self, mock_get_storage: MagicMock
    ) -> None:
        """Test health check when a component is unhealthy"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry that throws an error
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.side_effect = Exception(
            "Calculator registry failed"
        )

        # Patch the router dependencies
        with (
            patch("src.routers.fire_recovery.storage_factory") as mock_storage_factory,
            patch("src.routers.fire_recovery.stac_manager", mock_stac_manager),
            patch("src.routers.fire_recovery.index_registry", mock_index_registry),
        ):
            mock_storage_factory.get_temp_storage.return_value = mock_storage

            # Import app after patching
            from src.app import app

            client = TestClient(app)

            # Make the request
            response = client.get("/fire-recovery/healthz")

            # Should still return 200 but with unhealthy status
            assert response.status_code == 200
            data = response.json()

            # Should indicate unhealthy system
            assert data["overall_status"] == "unhealthy"
            assert data["status"] == "unhealthy"
            assert data["unhealthy_components"] > 0

            # Check that index_registry is marked as unhealthy
            assert data["checks"]["index_registry"]["status"] == "unhealthy"
            assert "error" in data["checks"]["index_registry"]

    @patch("src.config.storage.get_storage")
    def test_health_check_command_failure(self, mock_get_storage: MagicMock) -> None:
        """Test health check when command execution fails"""

        # Setup mock storage that fails
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Mock storage factory to return failing storage
        mock_storage_factory = MagicMock()
        mock_storage_factory.get_temp_storage.side_effect = Exception(
            "Storage initialization failed"
        )

        # Patch the router dependencies
        with (
            patch("src.routers.fire_recovery.storage_factory", mock_storage_factory),
            patch("src.routers.fire_recovery.stac_manager") as __mock_stac_manager,
            patch("src.routers.fire_recovery.index_registry") as __mock_index_registry,
        ):
            # Import app after patching
            from src.app import app

            client = TestClient(app)

            # Make the request
            response = client.get("/fire-recovery/healthz")

            # Should return 500 error due to command context creation failure
            assert response.status_code == 500
            data = response.json()
            assert "detail" in data
            assert "Health check error" in data["detail"]

    def test_health_check_response_model_validation(self) -> None:
        """Test that response model validation works correctly"""
        from src.models.responses import HealthCheckResponse

        # Test valid response model
        job_id = str(uuid.uuid4())

        # Should not raise validation error
        response = HealthCheckResponse(
            fire_event_name="health-check",
            status="healthy",
            job_id=job_id,
            overall_status="healthy",
            timestamp=1234567890.0,
            checks={
                "storage": {"status": "healthy"},
                "stac_manager": {"status": "healthy"},
            },
            unhealthy_components=0,
        )
        assert response.overall_status == "healthy"
        assert response.unhealthy_components == 0

        # Test invalid response model (missing required field)
        with pytest.raises(Exception):  # Pydantic validation error
            HealthCheckResponse(
                fire_event_name="health-check",
                # Missing status field to trigger validation error
                job_id=job_id,
                overall_status="healthy",
                timestamp=1234567890.0,
                checks={},
                unhealthy_components=0,
            )


if __name__ == "__main__":
    # Run a quick manual test
    test_instance = TestHealthEndpoint()
    print("Running health endpoint integration tests...")

    try:
        test_instance.test_health_check_success()
        print("✅ Health check success test passed!")

        test_instance.test_health_check_with_unhealthy_component()
        print("✅ Health check unhealthy component test passed!")

        test_instance.test_health_check_response_model_validation()
        print("✅ Response model validation test passed!")

        print("\n🎉 All health endpoint integration tests passed!")
        print("\n✅ Command pattern working correctly")
        print("✅ Response contracts validated")
        print("✅ Error handling tested")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        raise
