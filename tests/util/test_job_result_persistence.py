"""Tests for job result persistence utilities."""

import pytest
from datetime import datetime
from unittest.mock import Mock
from typing import Dict, Any

from src.commands.interfaces.command_result import CommandResult, CommandStatus
from src.util.job_result_persistence import (
    get_job_result_path,
    persist_job_result,
    get_job_result,
)
from src.core.storage.storage_factory import StorageFactory
from src.core.storage.interface import StorageInterface


class MockStorage(StorageInterface):
    """Mock storage implementation for testing."""

    def __init__(self) -> None:
        self._json_data: Dict[str, Dict[str, Any]] = {}
        self._bytes_data: Dict[str, bytes] = {}

    def get_obstore(self) -> Mock:
        return Mock()

    async def save_bytes(self, data: bytes, path: str, temporary: bool = False) -> str:
        self._bytes_data[path] = data
        return f"mock://{path}"

    async def get_bytes(self, path: str) -> bytes:
        if path not in self._bytes_data:
            raise FileNotFoundError(f"Path not found: {path}")
        return self._bytes_data[path]

    async def save_json(
        self, data: Dict[str, Any], path: str, temporary: bool = False
    ) -> str:
        self._json_data[path] = data
        return f"mock://{path}"

    async def get_json(self, path: str) -> Dict[str, Any]:
        if path not in self._json_data:
            raise FileNotFoundError(f"Path not found: {path}")
        return self._json_data[path]

    async def list_files(self, prefix: str) -> list[str]:
        all_paths = list(self._bytes_data.keys()) + list(self._json_data.keys())
        return [p for p in all_paths if p.startswith(prefix)]

    def get_url(self, path: str) -> str:
        return f"mock://{path}"

    async def process_stream(
        self,
        source_path: str,
        processor: Any,
        target_path: str,
        temporary: bool = False,
    ) -> str:
        return f"mock://{target_path}"

    async def copy_from_url(
        self, url: str, target_path: str, temporary: bool = False
    ) -> str:
        return f"mock://{target_path}"

    async def cleanup(self, max_age_seconds: float | None = None) -> int:
        count = len(self._bytes_data) + len(self._json_data)
        self._bytes_data.clear()
        self._json_data.clear()
        return count


@pytest.fixture
def mock_storage() -> MockStorage:
    """Create mock storage for testing."""
    return MockStorage()


@pytest.fixture
def mock_storage_factory(mock_storage: MockStorage) -> Mock:
    """Create mock storage factory."""
    factory = Mock(spec=StorageFactory)
    factory.get_final_storage.return_value = mock_storage
    factory.get_temp_storage.return_value = mock_storage
    return factory


class TestCommandResultSerialization:
    """Test CommandResult to_dict/from_dict round-trip serialization."""

    def test_success_result_serialization_roundtrip(self) -> None:
        """Test serialization and deserialization of success result."""
        original = CommandResult.success(
            job_id="test-job-123",
            fire_event_name="test-fire",
            command_name="fire_severity_analysis",
            execution_time_ms=12345.67,
            data={"some_key": "some_value"},
            asset_urls={"cog": "gs://bucket/cog.tif"},
            metadata={"custom": "metadata"},
        )

        # Serialize
        serialized = original.to_dict()

        # Verify serialized format
        assert serialized["status"] == "success"
        assert serialized["job_id"] == "test-job-123"
        assert serialized["fire_event_name"] == "test-fire"
        assert serialized["command_name"] == "fire_severity_analysis"
        assert serialized["execution_time_ms"] == 12345.67
        assert serialized["data"] == {"some_key": "some_value"}
        assert serialized["asset_urls"] == {"cog": "gs://bucket/cog.tif"}
        assert serialized["metadata"] == {"custom": "metadata"}
        assert serialized["error_message"] is None
        assert serialized["error_details"] is None
        assert "timestamp" in serialized

        # Deserialize
        restored = CommandResult.from_dict(serialized)

        # Verify restoration
        assert restored.status == CommandStatus.SUCCESS
        assert restored.job_id == original.job_id
        assert restored.fire_event_name == original.fire_event_name
        assert restored.command_name == original.command_name
        assert restored.execution_time_ms == original.execution_time_ms
        assert restored.data == original.data
        assert restored.asset_urls == original.asset_urls
        assert restored.metadata == original.metadata
        assert restored.is_success()

    def test_failure_result_serialization_roundtrip(self) -> None:
        """Test serialization and deserialization of failure result."""
        original = CommandResult.failure(
            job_id="test-job-456",
            fire_event_name="test-fire-2",
            command_name="boundary_refinement",
            execution_time_ms=5000.0,
            error_message="No satellite imagery found",
            error_details={
                "stage": "data_retrieval",
                "exception_type": "NoDataError",
                "inputs": {"date_range": ["2023-01-01", "2023-01-05"]},
            },
            metadata={"retry_count": 3},
        )

        # Serialize
        serialized = original.to_dict()

        # Verify serialized format
        assert serialized["status"] == "failed"
        assert serialized["error_message"] == "No satellite imagery found"
        assert serialized["error_details"]["stage"] == "data_retrieval"
        assert serialized["error_details"]["exception_type"] == "NoDataError"

        # Deserialize
        restored = CommandResult.from_dict(serialized)

        # Verify restoration
        assert restored.status == CommandStatus.FAILED
        assert restored.is_failure()
        assert restored.error_message == original.error_message
        assert restored.error_details == original.error_details

    def test_partial_success_result_serialization_roundtrip(self) -> None:
        """Test serialization and deserialization of partial success result."""
        original = CommandResult.partial_success(
            job_id="test-job-789",
            fire_event_name="test-fire-3",
            command_name="vegetation_resolution",
            execution_time_ms=8000.0,
            data={"processed_layers": 3},
            asset_urls={"partial_output": "gs://bucket/partial.json"},
            error_message="Some vegetation types skipped",
            error_details={"skipped_count": 2},
        )

        # Serialize and deserialize
        serialized = original.to_dict()
        restored = CommandResult.from_dict(serialized)

        # Verify
        assert restored.status == CommandStatus.PARTIAL_SUCCESS
        assert restored.is_partial_success()
        assert restored.data == original.data
        assert restored.error_message == original.error_message

    def test_result_with_none_optional_fields(self) -> None:
        """Test serialization of result with None optional fields."""
        original = CommandResult.success(
            job_id="test-job",
            fire_event_name="test-fire",
            command_name="test",
            execution_time_ms=100.0,
            # All optional fields left as None
        )

        serialized = original.to_dict()
        restored = CommandResult.from_dict(serialized)

        assert restored.data is None
        assert restored.asset_urls is None
        assert restored.error_message is None
        assert restored.error_details is None
        assert restored.metadata is None


class TestJobResultPath:
    """Test job result path generation."""

    def test_get_job_result_path(self) -> None:
        """Test job result path formatting."""
        path = get_job_result_path("abc-123-def")
        assert path == "assets/abc-123-def/job_result.json"

    def test_get_job_result_path_uuid(self) -> None:
        """Test job result path with UUID-like job ID."""
        job_id = "550e8400-e29b-41d4-a716-446655440000"
        path = get_job_result_path(job_id)
        assert path == f"assets/{job_id}/job_result.json"


class TestPersistJobResult:
    """Test persist_job_result function."""

    @pytest.mark.asyncio
    async def test_persist_success_result(
        self, mock_storage_factory: Mock, mock_storage: MockStorage
    ) -> None:
        """Test persisting a success result."""
        result = CommandResult.success(
            job_id="persist-test-123",
            fire_event_name="test-fire",
            command_name="fire_severity_analysis",
            execution_time_ms=5000.0,
            asset_urls={"cog": "gs://bucket/cog.tif"},
        )

        url = await persist_job_result(result, mock_storage_factory)

        # Verify URL is returned
        assert "persist-test-123" in url
        assert "job_result.json" in url

        # Verify data was stored
        expected_path = "assets/persist-test-123/job_result.json"
        assert expected_path in mock_storage._json_data

        stored_data = mock_storage._json_data[expected_path]
        assert stored_data["status"] == "success"
        assert stored_data["job_id"] == "persist-test-123"

    @pytest.mark.asyncio
    async def test_persist_failure_result(
        self, mock_storage_factory: Mock, mock_storage: MockStorage
    ) -> None:
        """Test persisting a failure result."""
        result = CommandResult.failure(
            job_id="persist-fail-456",
            fire_event_name="test-fire",
            command_name="boundary_refinement",
            execution_time_ms=1000.0,
            error_message="Test error message",
            error_details={"stage": "test"},
        )

        await persist_job_result(result, mock_storage_factory)

        # Verify data was stored
        expected_path = "assets/persist-fail-456/job_result.json"
        stored_data = mock_storage._json_data[expected_path]
        assert stored_data["status"] == "failed"
        assert stored_data["error_message"] == "Test error message"
        assert stored_data["error_details"]["stage"] == "test"


class TestGetJobResult:
    """Test get_job_result function."""

    @pytest.mark.asyncio
    async def test_get_existing_job_result(
        self, mock_storage_factory: Mock, mock_storage: MockStorage
    ) -> None:
        """Test retrieving an existing job result."""
        # Pre-populate storage with a job result
        job_id = "existing-job-123"
        result_data = {
            "status": "success",
            "job_id": job_id,
            "fire_event_name": "test-fire",
            "command_name": "fire_severity_analysis",
            "execution_time_ms": 5000.0,
            "timestamp": datetime.utcnow().isoformat(),
            "data": None,
            "asset_urls": {"cog": "gs://bucket/cog.tif"},
            "error_message": None,
            "error_details": None,
            "metadata": None,
        }
        mock_storage._json_data[f"assets/{job_id}/job_result.json"] = result_data

        # Retrieve it
        result = await get_job_result(job_id, mock_storage_factory)

        assert result is not None
        assert result.job_id == job_id
        assert result.is_success()
        assert result.command_name == "fire_severity_analysis"

    @pytest.mark.asyncio
    async def test_get_nonexistent_job_result(
        self, mock_storage_factory: Mock, mock_storage: MockStorage
    ) -> None:
        """Test retrieving a non-existent job result returns None."""
        result = await get_job_result("nonexistent-job-999", mock_storage_factory)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_failed_job_result(
        self, mock_storage_factory: Mock, mock_storage: MockStorage
    ) -> None:
        """Test retrieving a failed job result."""
        job_id = "failed-job-456"
        result_data = {
            "status": "failed",
            "job_id": job_id,
            "fire_event_name": "test-fire",
            "command_name": "vegetation_resolution",
            "execution_time_ms": 2000.0,
            "timestamp": datetime.utcnow().isoformat(),
            "data": None,
            "asset_urls": None,
            "error_message": "Something went wrong",
            "error_details": {"stage": "processing", "exception_type": "ValueError"},
            "metadata": None,
        }
        mock_storage._json_data[f"assets/{job_id}/job_result.json"] = result_data

        result = await get_job_result(job_id, mock_storage_factory)

        assert result is not None
        assert result.is_failure()
        assert result.error_message == "Something went wrong"
        assert result.error_details is not None
        assert result.error_details["stage"] == "processing"


class TestPersistAndRetrieveIntegration:
    """Integration tests for persist and retrieve workflow."""

    @pytest.mark.asyncio
    async def test_persist_and_retrieve_success_result(
        self, mock_storage_factory: Mock, mock_storage: MockStorage
    ) -> None:
        """Test full persist and retrieve cycle for success result."""
        original = CommandResult.success(
            job_id="integration-test-001",
            fire_event_name="integration-fire",
            command_name="fire_severity_analysis",
            execution_time_ms=10000.0,
            data={"vegetation_types": 15},
            asset_urls={
                "rbr": "gs://bucket/rbr.tif",
                "dnbr": "gs://bucket/dnbr.tif",
            },
            metadata={"source": "sentinel-2"},
        )

        # Persist
        await persist_job_result(original, mock_storage_factory)

        # Retrieve
        retrieved = await get_job_result(original.job_id, mock_storage_factory)

        # Verify full round-trip
        assert retrieved is not None
        assert retrieved.status == original.status
        assert retrieved.job_id == original.job_id
        assert retrieved.fire_event_name == original.fire_event_name
        assert retrieved.command_name == original.command_name
        assert retrieved.execution_time_ms == original.execution_time_ms
        assert retrieved.data == original.data
        assert retrieved.asset_urls == original.asset_urls
        assert retrieved.metadata == original.metadata

    @pytest.mark.asyncio
    async def test_persist_and_retrieve_failure_result(
        self, mock_storage_factory: Mock, mock_storage: MockStorage
    ) -> None:
        """Test full persist and retrieve cycle for failure result."""
        original = CommandResult.failure(
            job_id="integration-test-002",
            fire_event_name="integration-fire",
            command_name="boundary_refinement",
            execution_time_ms=500.0,
            error_message="No matching COGs found",
            error_details={
                "stage": "cog_retrieval",
                "exception_type": "FileNotFoundError",
                "searched_paths": ["assets/job-xyz/fire_severity/"],
            },
        )

        # Persist
        await persist_job_result(original, mock_storage_factory)

        # Retrieve
        retrieved = await get_job_result(original.job_id, mock_storage_factory)

        # Verify full round-trip
        assert retrieved is not None
        assert retrieved.is_failure()
        assert retrieved.error_message == original.error_message
        assert retrieved.error_details == original.error_details
