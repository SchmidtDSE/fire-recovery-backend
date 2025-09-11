import pytest
from unittest.mock import Mock, AsyncMock
from typing import Dict, Any
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult
from src.commands.registry.command_registry import CommandRegistry
from src.commands.executor.command_executor import CommandExecutor

# TestCommand imported locally in each test to avoid naming conflicts
from src.core.storage.interface import StorageInterface
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry
from src.core.storage.storage_factory import StorageFactory


class MockStorage(StorageInterface):
    """Mock storage implementation for testing"""

    def __init__(self) -> None:
        self._data: Dict[str, bytes] = {}
        self._json_data: Dict[str, Dict[str, Any]] = {}

    def get_obstore(self) -> Mock:
        """Mock obstore - not needed for testing"""
        return Mock()

    async def save_bytes(self, data: bytes, path: str, temporary: bool = False) -> str:
        self._data[path] = data
        return f"mock://{path}"

    async def get_bytes(self, path: str) -> bytes:
        if path not in self._data:
            raise FileNotFoundError(f"Path not found: {path}")
        return self._data[path]

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
        all_paths = list(self._data.keys()) + list(self._json_data.keys())
        return [path for path in all_paths if path.startswith(prefix)]

    def get_url(self, path: str) -> str:
        return f"mock://{path}"

    async def process_stream(
        self,
        source_path: str,
        processor: Any,
        target_path: str,
        temporary: bool = False,
    ) -> str:
        # Mock implementation - just copy data
        __source_data = await self.get_bytes(source_path)
        processed_data = processor(Mock())  # Mock the stream processing
        return await self.save_bytes(processed_data, target_path, temporary)

    async def copy_from_url(
        self, url: str, target_path: str, temporary: bool = False
    ) -> str:
        # Mock implementation - save fake data
        fake_data = f"Downloaded from {url}".encode()
        return await self.save_bytes(fake_data, target_path, temporary)

    async def cleanup(self, max_age_seconds: float | None = None) -> int:
        count = len(self._data) + len(self._json_data)
        self._data.clear()
        self._json_data.clear()
        return count


@pytest.fixture
def mock_storage() -> MockStorage:
    """Create mock storage for testing"""
    return MockStorage()


@pytest.fixture
def mock_storage_factory(mock_storage: MockStorage) -> Mock:
    """Create mock storage factory"""
    factory = Mock(spec=StorageFactory)
    factory.get_temp_storage.return_value = mock_storage
    factory.get_final_storage.return_value = mock_storage
    return factory


@pytest.fixture
def mock_stac_manager() -> Mock:
    """Create mock STAC manager"""
    manager = Mock(spec=STACJSONManager)
    manager.create_fire_severity_item = AsyncMock()
    manager.create_boundary_item = AsyncMock()
    manager.get_item_by_id = AsyncMock()
    return manager


@pytest.fixture
def mock_index_registry() -> Mock:
    """Create mock index registry"""
    registry = Mock(spec=IndexRegistry)
    registry.get_available_indices.return_value = ["nbr", "dnbr", "rdnbr", "rbr"]
    return registry


@pytest.fixture
def mock_storage_factory(mock_storage: MockStorage) -> Mock:
    """Create mock storage factory"""
    factory = Mock(spec=StorageFactory)
    factory.get_temp_storage.return_value = mock_storage
    factory.get_final_storage.return_value = mock_storage
    return factory


@pytest.fixture
def command_context(
    mock_storage: MockStorage, mock_storage_factory: Mock, mock_stac_manager: Mock, mock_index_registry: Mock
) -> CommandContext:
    """Create test command context"""
    return CommandContext(
        job_id="test-job-123",
        fire_event_name="test-fire",
        geometry={
            "type": "Polygon",
            "coordinates": [
                [[-120, 35], [-119, 35], [-119, 36], [-120, 36], [-120, 35]]
            ],
        },
        storage=mock_storage,
        storage_factory=mock_storage_factory,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        computation_config={"test_config": True},
        metadata={"test_metadata": "value"},
    )


class TestCommandContext:
    """Test CommandContext functionality"""

    def test_context_creation(self, command_context: CommandContext) -> None:
        """Test that context is created with all required fields"""
        assert command_context.job_id == "test-job-123"
        assert command_context.fire_event_name == "test-fire"
        assert command_context.geometry is not None
        assert command_context.storage is not None
        assert command_context.stac_manager is not None
        assert command_context.index_registry is not None

    def test_context_validation_missing_fields(self) -> None:
        """Test context validation with missing required fields"""
        with pytest.raises(ValueError, match="job_id is required"):
            CommandContext(
                job_id="",
                fire_event_name="test",
                geometry={},
                storage=Mock(),
                storage_factory=Mock(),
                stac_manager=Mock(),
                index_registry=Mock(),
            )

    def test_context_metadata_operations(self, command_context: CommandContext) -> None:
        """Test metadata getter/setter operations"""
        # Test existing metadata
        assert command_context.get_metadata("test_metadata") == "value"

        # Test default value
        assert command_context.get_metadata("nonexistent", "default") == "default"

        # Test adding new metadata
        command_context.add_metadata("new_key", "new_value")
        assert command_context.get_metadata("new_key") == "new_value"

    def test_context_computation_config_operations(
        self, command_context: CommandContext
    ) -> None:
        """Test computation config getter operations"""
        # Test existing config
        assert command_context.get_computation_config("test_config") is True

        # Test default value
        assert command_context.get_computation_config("nonexistent", False) is False


class TestCommandResult:
    """Test CommandResult functionality"""

    def test_success_result_creation(self) -> None:
        """Test creating successful command result"""
        result = CommandResult.success(
            job_id="test-job",
            fire_event_name="test-fire",
            command_name="test_command",
            execution_time_ms=100.0,
            data={"result": "success"},
            asset_urls={"output": "gs://bucket/output.tif"},
        )

        assert result.is_success()
        assert not result.is_failure()
        assert not result.is_partial_success()
        assert result.has_assets()
        assert result.get_asset_url("output") == "gs://bucket/output.tif"

    def test_failure_result_creation(self) -> None:
        """Test creating failed command result"""
        result = CommandResult.failure(
            job_id="test-job",
            fire_event_name="test-fire",
            command_name="test_command",
            execution_time_ms=50.0,
            error_message="Test error",
            error_details={"error_type": "TestError"},
        )

        assert result.is_failure()
        assert not result.is_success()
        assert not result.is_partial_success()
        assert not result.has_assets()
        assert result.error_message == "Test error"

    def test_result_asset_operations(self) -> None:
        """Test asset URL operations on result"""
        result = CommandResult.success(
            job_id="test-job",
            fire_event_name="test-fire",
            command_name="test_command",
            execution_time_ms=100.0,
        )

        # Initially no assets
        assert not result.has_assets()

        # Add asset
        result.add_asset_url("test_asset", "gs://bucket/test.tif")
        assert result.has_assets()
        assert result.get_asset_url("test_asset") == "gs://bucket/test.tif"

        # Non-existent asset
        assert result.get_asset_url("nonexistent") is None


class TestCommandImplementation:
    """Test Command interface and TestCommand implementation"""

    def test_test_command_properties(self) -> None:
        """Test TestCommand basic properties"""
        from src.commands.impl.test_command import TestCommand

        command = TestCommand()

        assert command.get_command_name() == "test_command"
        assert command.get_estimated_duration_seconds() == 1.0
        assert command.supports_retry() is True
        assert command.get_dependencies() == []
        assert command.get_required_permissions() == []

    def test_test_command_context_validation(
        self, command_context: CommandContext
    ) -> None:
        """Test TestCommand context validation"""
        from src.commands.impl.test_command import TestCommand

        command = TestCommand()

        # Valid context should pass
        assert command.validate_context(command_context) is True

        # Invalid context should fail - we can't create invalid context due to __post_init__ validation
        # So we'll test by setting attributes to None after creation
        invalid_context = CommandContext(
            job_id="test",
            fire_event_name="test",
            geometry={"type": "Point", "coordinates": [0, 0]},
            storage=Mock(),
            storage_factory=Mock(),
            stac_manager=Mock(),
            index_registry=Mock(),
        )
        # Make storage None to trigger validation failure
        invalid_context.storage = None
        assert command.validate_context(invalid_context) is False

    @pytest.mark.asyncio
    async def test_test_command_execution_success(
        self, command_context: CommandContext
    ) -> None:
        """Test successful TestCommand execution"""
        from src.commands.impl.test_command import TestCommand

        command = TestCommand()

        result = await command.execute(command_context)

        assert result.is_success()
        assert result.command_name == "test_command"
        assert result.job_id == "test-job-123"
        assert result.execution_time_ms > 0
        assert result.data["test_passed"] is True
        assert result.has_assets()
        assert "test_output" in result.asset_urls


class TestCommandRegistry:
    """Test CommandRegistry functionality"""

    def test_registry_creation(
        self,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
    ) -> None:
        """Test creating command registry with dependencies"""
        # Registry should create successfully and find implemented commands
        registry = CommandRegistry(
            mock_storage_factory, mock_stac_manager, mock_index_registry
        )

        # Should have found our implemented commands
        available_commands = registry.get_available_commands()
        assert (
            len(available_commands) >= 2
        )  # At least fire_severity_analysis and upload_aoi
        assert "fire_severity_analysis" in available_commands
        assert "upload_aoi" in available_commands

        # Should still have the dependencies set
        assert registry._storage_factory == mock_storage_factory
        assert registry._stac_manager == mock_stac_manager
        assert registry._index_registry == mock_index_registry

    def test_registry_with_test_command_only(
        self,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
    ) -> None:
        """Test registry functionality with test command"""
        # Create registry without auto-setup
        registry = CommandRegistry.__new__(CommandRegistry)
        registry._storage_factory = mock_storage_factory
        registry._stac_manager = mock_stac_manager
        registry._index_registry = mock_index_registry
        registry._command_classes = {}
        registry._command_instances = {}

        # Manually register test command
        from src.commands.impl.test_command import TestCommand

        registry._register_command_class(TestCommand)

        # Test registry functionality
        assert "test_command" in registry.get_available_commands()

        # Test command info
        info = registry.get_command_info("test_command")
        assert info["name"] == "test_command"
        assert info["estimated_duration_seconds"] == 1.0
        assert info["supports_retry"] is True


class TestCommandExecutor:
    """Test CommandExecutor functionality"""

    @pytest.mark.asyncio
    async def test_executor_command_execution(
        self,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
        command_context: CommandContext,
    ) -> None:
        """Test command execution through executor"""
        # Create minimal registry with test command
        registry = CommandRegistry.__new__(CommandRegistry)
        registry._storage_factory = mock_storage_factory
        registry._stac_manager = mock_stac_manager
        registry._index_registry = mock_index_registry
        from src.commands.impl.test_command import TestCommand

        registry._command_classes = {"test_command": TestCommand}
        registry._command_instances = {}

        # Create executor
        executor = CommandExecutor(registry, default_timeout_seconds=30.0)

        # Execute command
        result = await executor.execute_command("test_command", command_context)

        assert result.is_success()
        assert result.command_name == "test_command"

        # Check metrics
        metrics = executor.get_execution_metrics()
        assert metrics["total_executions"] == 1
        assert metrics["successful_executions"] == 1
        assert metrics["failed_executions"] == 0

    @pytest.mark.asyncio
    async def test_executor_command_not_found(
        self,
        mock_storage_factory: Mock,
        mock_stac_manager: Mock,
        mock_index_registry: Mock,
        command_context: CommandContext,
    ) -> None:
        """Test executor with non-existent command"""
        # Create empty registry
        registry = CommandRegistry.__new__(CommandRegistry)
        registry._storage_factory = mock_storage_factory
        registry._stac_manager = mock_stac_manager
        registry._index_registry = mock_index_registry
        registry._command_classes = {}
        registry._command_instances = {}

        executor = CommandExecutor(registry)

        # This should fail because command doesn't exist
        with pytest.raises(ValueError, match="Command 'nonexistent' not found"):
            await executor.execute_command("nonexistent", command_context)


# Run a simple integration test if this file is executed directly
if __name__ == "__main__":
    import asyncio

    async def main() -> bool:
        print("Running command infrastructure integration test...")

        # Create mocks
        storage = MockStorage()
        stac_manager = Mock(spec=STACJSONManager)
        index_registry = Mock(spec=IndexRegistry)

        # Create context
        storage_factory = Mock()
        context = CommandContext(
            job_id="integration-test",
            fire_event_name="test-fire",
            geometry={"type": "Point", "coordinates": [-120, 35]},
            storage=storage,
            storage_factory=storage_factory,
            stac_manager=stac_manager,
            index_registry=index_registry,
        )

        # Create and execute test command
        from src.commands.impl.test_command import TestCommand

        command = TestCommand()
        result = await command.execute(context)

        if result.is_success():
            print("✅ Command infrastructure test passed!")
            print(f"   Execution time: {result.execution_time_ms:.2f}ms")
            print(f"   Test data: {result.data}")
        else:
            print("❌ Command infrastructure test failed!")
            print(f"   Error: {result.error_message}")

        return result.is_success()

    success = asyncio.run(main())
    exit(0 if success else 1)
