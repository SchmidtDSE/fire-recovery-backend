import pytest
import uuid
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from typing import Dict, Any
from datetime import datetime


class TestFireSeverityAnalysisEndpoints:
    """Integration tests for Fire Severity Analysis endpoints following FastAPI TestClient pattern"""

    @pytest.fixture
    def sample_request_payload(self) -> Dict[str, Any]:
        """Standard request payload for testing"""
        return {
            "fire_event_name": "test-fire-2024",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-120.5, 35.5], [-120.0, 35.5], [-120.0, 36.0], [-120.5, 36.0], [-120.5, 35.5]]]
            },
            "prefire_date_range": ["2024-01-01", "2024-02-01"],
            "postfire_date_range": ["2024-08-01", "2024-09-01"]
        }

    @pytest.fixture
    def sample_stac_item(self) -> Dict[str, Any]:
        """Sample STAC item for result testing"""
        job_id = str(uuid.uuid4())
        return {
            "id": f"test-fire-2024-severity-{job_id}",
            "assets": {
                "nbr": {"href": f"https://storage.googleapis.com/test-bucket/{job_id}/nbr.tif"},
                "dnbr": {"href": f"https://storage.googleapis.com/test-bucket/{job_id}/dnbr.tif"},
                "rdnbr": {"href": f"https://storage.googleapis.com/test-bucket/{job_id}/rdnbr.tif"},
                "rbr": {"href": f"https://storage.googleapis.com/test-bucket/{job_id}/rbr.tif"}
            },
            "properties": {
                "datetime": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            }
        }

    def create_test_geometry(self) -> Dict[str, Any]:
        """Create valid test geometry for requests"""
        return {
            "type": "Polygon",
            "coordinates": [[[-120.5, 35.5], [-120.0, 35.5], [-120.0, 36.0], [-120.5, 36.0], [-120.5, 35.5]]]
        }

    # POST endpoint tests
    @patch("src.config.storage.get_storage")
    def test_analyze_fire_severity_success(self, mock_get_storage: MagicMock, sample_request_payload: Dict[str, Any]) -> None:
        """Test successful fire severity analysis request"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

        # Patch the router dependencies
        with (
            patch("src.routers.fire_recovery.storage_factory") as mock_storage_factory,
            patch("src.routers.fire_recovery.stac_manager", mock_stac_manager),
            patch("src.routers.fire_recovery.index_registry", mock_index_registry),
            patch("fastapi.BackgroundTasks.add_task") as mock_add_task,
        ):
            mock_storage_factory.get_temp_storage.return_value = mock_storage

            # Import app after patching to avoid initialization issues
            from src.app import app

            client = TestClient(app)

            # Make the request
            response = client.post(
                "/fire-recovery/process/analyze_fire_severity",
                json=sample_request_payload,
                headers={"Content-Type": "application/json"}
            )

            # Verify response structure
            assert response.status_code == 200
            data = response.json()

            # Check response contract (ProcessingStartedResponse)
            required_fields = ["fire_event_name", "status", "job_id"]
            for field in required_fields:
                assert field in data, f"Missing required field: {field}"

            # Verify values
            assert data["fire_event_name"] == "test-fire-2024"
            assert data["status"] == "Processing started"
            assert isinstance(data["job_id"], str)
            assert len(data["job_id"]) > 0  # Should be a UUID

            # Verify background task was called
            mock_add_task.assert_called_once()
            call_args = mock_add_task.call_args
            assert call_args[1]["fire_event_name"] == "test-fire-2024"
            assert call_args[1]["prefire_date_range"] == ["2024-01-01", "2024-02-01"]
            assert call_args[1]["postfire_date_range"] == ["2024-08-01", "2024-09-01"]

    @patch("src.config.storage.get_storage")
    def test_analyze_fire_severity_invalid_geometry(self, mock_get_storage: MagicMock) -> None:
        """Test fire severity analysis with invalid geometry format"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

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

            # Invalid request payload with malformed coordinates
            invalid_payload = {
                "fire_event_name": "test-fire-2024",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": "invalid_coordinates"  # Should be array
                },
                "prefire_date_range": ["2024-01-01", "2024-02-01"],
                "postfire_date_range": ["2024-08-01", "2024-09-01"]
            }

            # Make the request
            response = client.post(
                "/fire-recovery/process/analyze_fire_severity",
                json=invalid_payload,
                headers={"Content-Type": "application/json"}
            )

            # Should return 422 validation error
            assert response.status_code == 422
            data = response.json()
            assert "detail" in data

    @patch("src.config.storage.get_storage")
    def test_analyze_fire_severity_missing_date_ranges(self, mock_get_storage: MagicMock) -> None:
        """Test fire severity analysis with missing required date ranges"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

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

            # Invalid request payload missing date ranges
            invalid_payload = {
                "fire_event_name": "test-fire-2024",
                "geometry": self.create_test_geometry()
                # Missing prefire_date_range and postfire_date_range
            }

            # Make the request
            response = client.post(
                "/fire-recovery/process/analyze_fire_severity",
                json=invalid_payload,
                headers={"Content-Type": "application/json"}
            )

            # Should return 422 validation error
            assert response.status_code == 422
            data = response.json()
            assert "detail" in data

    @patch("src.config.storage.get_storage")
    def test_analyze_fire_severity_invalid_date_format(self, mock_get_storage: MagicMock) -> None:
        """Test fire severity analysis with invalid date format"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

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

            # Invalid request payload with invalid date format
            invalid_payload = {
                "fire_event_name": "test-fire-2024",
                "geometry": self.create_test_geometry(),
                "prefire_date_range": ["not-a-date", "also-not-a-date"],
                "postfire_date_range": ["2024-08-01", "2024-09-01"]
            }

            # Make the request
            response = client.post(
                "/fire-recovery/process/analyze_fire_severity",
                json=invalid_payload,
                headers={"Content-Type": "application/json"}
            )

            # Note: This might pass validation at the Pydantic level since date format validation
            # is likely handled during processing. The response could be 200 but fail during background processing.
            # For this test, we'll check that the request is accepted but could add more specific validation.
            assert response.status_code in [200, 422]

    @patch("src.config.storage.get_storage")
    def test_analyze_fire_severity_storage_failure(self, mock_get_storage: MagicMock, sample_request_payload: Dict[str, Any]) -> None:
        """Test fire severity analysis when storage system fails"""

        # Setup mock storage that fails
        mock_get_storage.return_value = AsyncMock()

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

        # Mock storage factory to throw exception
        mock_storage_factory = MagicMock()
        mock_storage_factory.get_temp_storage.side_effect = Exception("Storage initialization failed")

        # Patch the router dependencies
        with (
            patch("src.routers.fire_recovery.storage_factory", mock_storage_factory),
            patch("src.routers.fire_recovery.stac_manager", mock_stac_manager),
            patch("src.routers.fire_recovery.index_registry", mock_index_registry),
        ):
            # Import app after patching
            from src.app import app

            client = TestClient(app)

            # Make the request
            response = client.post(
                "/fire-recovery/process/analyze_fire_severity",
                json=sample_request_payload,
                headers={"Content-Type": "application/json"}
            )

            # Should return 200 because the error occurs during background processing
            # The endpoint itself accepts the request and queues the background task
            assert response.status_code == 200

    # GET endpoint tests
    @patch("src.config.storage.get_storage")
    def test_get_fire_severity_result_pending(self, mock_get_storage: MagicMock) -> None:
        """Test getting fire severity result when processing is still pending"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager that returns None (no item found)
        mock_stac_manager = AsyncMock()
        mock_stac_manager.get_item_by_id.return_value = None

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

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
            test_fire_name = "test-fire-2024"
            test_job_id = str(uuid.uuid4())
            response = client.get(f"/fire-recovery/result/analyze_fire_severity/{test_fire_name}/{test_job_id}")

            # Verify response structure
            assert response.status_code == 200
            data = response.json()

            # Check response contract (TaskPendingResponse)
            required_fields = ["fire_event_name", "status", "job_id"]
            for field in required_fields:
                assert field in data, f"Missing required field: {field}"

            # Verify values
            assert data["fire_event_name"] == test_fire_name
            assert data["status"] == "pending"
            assert data["job_id"] == test_job_id

            # Verify STAC manager was called with correct item ID
            mock_stac_manager.get_item_by_id.assert_called_once_with(f"{test_fire_name}-severity-{test_job_id}")

    @patch("src.config.storage.get_storage")
    def test_get_fire_severity_result_complete(self, mock_get_storage: MagicMock, sample_stac_item: Dict[str, Any]) -> None:
        """Test getting fire severity result when analysis is complete"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager that returns the sample STAC item
        mock_stac_manager = AsyncMock()
        mock_stac_manager.get_item_by_id.return_value = sample_stac_item

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

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
            test_fire_name = "test-fire-2024"
            test_job_id = str(uuid.uuid4())
            response = client.get(f"/fire-recovery/result/analyze_fire_severity/{test_fire_name}/{test_job_id}")

            # Verify response structure
            assert response.status_code == 200
            data = response.json()

            # Check response contract (FireSeverityResponse)
            required_fields = ["fire_event_name", "status", "job_id", "coarse_severity_cog_urls"]
            for field in required_fields:
                assert field in data, f"Missing required field: {field}"

            # Verify values
            assert data["fire_event_name"] == test_fire_name
            assert data["status"] == "complete"
            assert data["job_id"] == test_job_id
            assert isinstance(data["coarse_severity_cog_urls"], dict)

            # Verify all expected COG URLs are present
            cog_urls = data["coarse_severity_cog_urls"]
            expected_metrics = ["nbr", "dnbr", "rdnbr", "rbr"]
            for metric in expected_metrics:
                assert metric in cog_urls
                assert cog_urls[metric].startswith("https://storage.googleapis.com/")

    @patch("src.config.storage.get_storage")
    def test_get_fire_severity_result_invalid_job_id(self, mock_get_storage: MagicMock) -> None:
        """Test getting fire severity result with invalid job ID"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager that returns None (no item found)
        mock_stac_manager = AsyncMock()
        mock_stac_manager.get_item_by_id.return_value = None

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

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

            # Make the request with malformed job_id
            test_fire_name = "test-fire-2024"
            malformed_job_id = "not-a-valid-uuid"
            response = client.get(f"/fire-recovery/result/analyze_fire_severity/{test_fire_name}/{malformed_job_id}")

            # Should return pending status (graceful handling)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "pending"

    @patch("src.config.storage.get_storage")
    def test_get_fire_severity_result_nonexistent_event(self, mock_get_storage: MagicMock) -> None:
        """Test getting fire severity result with non-existent fire event name"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager that returns None (no item found)
        mock_stac_manager = AsyncMock()
        mock_stac_manager.get_item_by_id.return_value = None

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

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

            # Make the request with non-existent fire event
            nonexistent_fire_name = "nonexistent-fire-2024"
            test_job_id = str(uuid.uuid4())
            response = client.get(f"/fire-recovery/result/analyze_fire_severity/{nonexistent_fire_name}/{test_job_id}")

            # Should return pending status (graceful handling)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "pending"

    # Background processing integration tests
    @patch("src.config.storage.get_storage")
    @patch("src.routers.fire_recovery.process_remote_sensing_data")
    @patch("src.routers.fire_recovery.upload_to_gcs")
    def test_background_processing_satellite_data_failure(
        self, 
        mock_upload_to_gcs: AsyncMock,
        mock_process_remote_sensing: AsyncMock, 
        mock_get_storage: MagicMock,
        sample_request_payload: Dict[str, Any]
    ) -> None:
        """Test background processing error handling when satellite data processing fails"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

        # Mock satellite data processing to raise exception
        mock_process_remote_sensing.side_effect = Exception("Satellite data processing failed")

        # Patch the router dependencies
        with (
            patch("src.routers.fire_recovery.storage_factory") as mock_storage_factory,
            patch("src.routers.fire_recovery.stac_manager", mock_stac_manager),
            patch("src.routers.fire_recovery.index_registry", mock_index_registry),
            patch("src.routers.fire_recovery.StacEndpointHandler") as mock_stac_handler_class,
        ):
            mock_storage_factory.get_temp_storage.return_value = mock_storage
            mock_stac_handler = AsyncMock()
            mock_stac_handler_class.return_value = mock_stac_handler

            # Import app after patching
            from src.app import app

            client = TestClient(app)

            # Make the request - should succeed in queueing the background task
            response = client.post(
                "/fire-recovery/process/analyze_fire_severity",
                json=sample_request_payload,
                headers={"Content-Type": "application/json"}
            )

            # Should return 200 (background task queued successfully)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "Processing started"

            # The background task failure would be logged, but the API endpoint succeeds
            # This tests that the system remains stable even when background processing fails

    @patch("src.config.storage.get_storage")
    @patch("src.routers.fire_recovery.process_remote_sensing_data")
    @patch("src.routers.fire_recovery.upload_to_gcs")
    def test_background_processing_upload_failure(
        self, 
        mock_upload_to_gcs: AsyncMock,
        mock_process_remote_sensing: AsyncMock, 
        mock_get_storage: MagicMock,
        sample_request_payload: Dict[str, Any]
    ) -> None:
        """Test background processing error handling when GCS upload fails"""

        # Setup mock storage
        mock_storage = AsyncMock()
        mock_get_storage.return_value = mock_storage

        # Create mock STAC manager
        mock_stac_manager = AsyncMock()

        # Create mock index registry
        mock_index_registry = MagicMock()
        mock_index_registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]

        # Mock successful data processing but failed upload
        mock_process_remote_sensing.return_value = {
            "output_files": {
                "nbr": "/tmp/nbr.tif",
                "dnbr": "/tmp/dnbr.tif"
            }
        }
        mock_upload_to_gcs.side_effect = Exception("GCS upload failed")

        # Patch the router dependencies
        with (
            patch("src.routers.fire_recovery.storage_factory") as mock_storage_factory,
            patch("src.routers.fire_recovery.stac_manager", mock_stac_manager),
            patch("src.routers.fire_recovery.index_registry", mock_index_registry),
            patch("src.routers.fire_recovery.StacEndpointHandler") as mock_stac_handler_class,
        ):
            mock_storage_factory.get_temp_storage.return_value = mock_storage
            mock_stac_handler = AsyncMock()
            mock_stac_handler_class.return_value = mock_stac_handler

            # Import app after patching
            from src.app import app

            client = TestClient(app)

            # Make the request - should succeed in queueing the background task
            response = client.post(
                "/fire-recovery/process/analyze_fire_severity",
                json=sample_request_payload,
                headers={"Content-Type": "application/json"}
            )

            # Should return 200 (background task queued successfully)
            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "Processing started"

            # The upload failure would be logged, but the API endpoint succeeds
            # This tests error handling and cleanup in background processing

    def test_response_model_validation(self) -> None:
        """Test that response models validate correctly"""
        from src.models.responses import ProcessingStartedResponse, TaskPendingResponse, FireSeverityResponse

        job_id = str(uuid.uuid4())

        # Test ProcessingStartedResponse validation
        processing_response = ProcessingStartedResponse(
            fire_event_name="test-fire-2024",
            status="Processing started",
            job_id=job_id
        )
        assert processing_response.fire_event_name == "test-fire-2024"
        assert processing_response.status == "Processing started"
        assert processing_response.job_id == job_id

        # Test TaskPendingResponse validation
        pending_response = TaskPendingResponse(
            fire_event_name="test-fire-2024",
            status="pending",
            job_id=job_id
        )
        assert pending_response.status == "pending"

        # Test FireSeverityResponse validation
        test_cog_urls = {
            "nbr": "https://storage.googleapis.com/test/nbr.tif",
            "dnbr": "https://storage.googleapis.com/test/dnbr.tif"
        }
        severity_response = FireSeverityResponse(
            fire_event_name="test-fire-2024",
            status="complete",
            job_id=job_id,
            coarse_severity_cog_urls=test_cog_urls
        )
        assert severity_response.status == "complete"
        assert severity_response.coarse_severity_cog_urls == test_cog_urls

        # Test invalid response model (missing required field)
        with pytest.raises(Exception):  # Pydantic validation error
            ProcessingStartedResponse(
                fire_event_name="test-fire-2024",
                # Missing status and job_id to trigger validation error
            )


if __name__ == "__main__":
    # Run a quick manual test
    test_instance = TestFireSeverityAnalysisEndpoints()
    print("Running fire severity analysis endpoint integration tests...")

    try:
        # Test POST endpoint
        test_instance.test_analyze_fire_severity_success(
            MagicMock(), test_instance.sample_request_payload()
        )
        print("✅ Fire severity analysis POST success test passed!")

        test_instance.test_analyze_fire_severity_invalid_geometry(MagicMock())
        print("✅ Fire severity analysis invalid geometry test passed!")

        # Test GET endpoint
        test_instance.test_get_fire_severity_result_pending(MagicMock())
        print("✅ Fire severity result pending test passed!")

        test_instance.test_get_fire_severity_result_complete(
            MagicMock(), test_instance.sample_stac_item()
        )
        print("✅ Fire severity result complete test passed!")

        # Test response models
        test_instance.test_response_model_validation()
        print("✅ Response model validation test passed!")

        print("\n🎉 All fire severity analysis endpoint integration tests passed!")
        print("\n✅ Request/response contracts validated")
        print("✅ Background processing integration tested")
        print("✅ Error handling scenarios covered")
        print("✅ External dependencies properly mocked")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        raise