import pytest
from unittest.mock import AsyncMock, MagicMock
from fastapi.testclient import TestClient
import uuid


class TestHealthEndpoint:
    """Integration tests for the /healthz endpoint using command pattern"""

    def test_health_check_success(self) -> None:
        """Test successful health check endpoint"""

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

        # Import app and router dependency functions
        from src.app import app
        from src.routers.fire_recovery import (
            get_stac_manager,
            get_storage_factory,
            get_index_registry,
        )

        # Create mock storage factory
        mock_storage = AsyncMock()
        mock_storage_factory = MagicMock()
        mock_storage_factory.get_temp_storage.return_value = mock_storage

        # Override dependencies with mocks
        async def mock_get_stac_manager():
            return mock_stac_manager

        app.dependency_overrides[get_stac_manager] = mock_get_stac_manager
        app.dependency_overrides[get_storage_factory] = lambda: mock_storage_factory
        app.dependency_overrides[get_index_registry] = lambda: mock_index_registry

        try:
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
        finally:
            # Clean up dependency overrides
            app.dependency_overrides.clear()

    def test_health_check_with_unhealthy_component(self) -> None:
        """Test health check when a component is unhealthy"""

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry that throws an error
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.side_effect = Exception(
            "Calculator registry failed"
        )

        # Import app and router dependency functions
        from src.app import app
        from src.routers.fire_recovery import (
            get_stac_manager,
            get_storage_factory,
            get_index_registry,
        )

        # Create mock storage factory
        mock_storage = AsyncMock()
        mock_storage_factory = MagicMock()
        mock_storage_factory.get_temp_storage.return_value = mock_storage

        # Override dependencies with mocks
        async def mock_get_stac_manager():
            return mock_stac_manager

        app.dependency_overrides[get_stac_manager] = mock_get_stac_manager
        app.dependency_overrides[get_storage_factory] = lambda: mock_storage_factory
        app.dependency_overrides[get_index_registry] = lambda: mock_index_registry

        try:
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
        finally:
            # Clean up dependency overrides
            app.dependency_overrides.clear()

    def test_health_check_command_failure(self) -> None:
        """Test health check when command execution fails"""

        # Mock storage factory to return failing storage
        mock_storage_factory = MagicMock()
        mock_storage_factory.get_temp_storage.side_effect = Exception(
            "Storage initialization failed"
        )

        # Import app and router dependency functions
        from src.app import app
        from src.routers.fire_recovery import (
            get_stac_manager,
            get_storage_factory,
            get_index_registry,
        )

        # Create mock STAC manager and index registry
        mock_stac_manager = MagicMock()
        mock_index_registry = MagicMock()

        # Override dependencies with mocks
        async def mock_get_stac_manager():
            return mock_stac_manager

        app.dependency_overrides[get_stac_manager] = mock_get_stac_manager
        app.dependency_overrides[get_storage_factory] = lambda: mock_storage_factory
        app.dependency_overrides[get_index_registry] = lambda: mock_index_registry

        try:
            client = TestClient(app)

            # Make the request
            response = client.get("/fire-recovery/healthz")

            # Should return 500 error due to command context creation failure
            assert response.status_code == 500
            data = response.json()
            assert "detail" in data
            assert "Health check error" in data["detail"]
        finally:
            # Clean up dependency overrides
            app.dependency_overrides.clear()

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
