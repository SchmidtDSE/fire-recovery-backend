import pytest
from unittest.mock import AsyncMock, MagicMock
from src.commands.impl.health_check_command import HealthCheckCommand
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandStatus


@pytest.fixture
def mock_storage() -> AsyncMock:
    """Mock storage interface"""
    return AsyncMock()


@pytest.fixture
def mock_stac_manager() -> AsyncMock:
    """Mock STAC manager"""
    return AsyncMock()


@pytest.fixture
def mock_index_registry() -> MagicMock:
    """Mock index registry"""
    mock_registry = MagicMock()
    mock_registry.get_available_calculators.return_value = [
        "nbr",
        "dnbr",
        "rdnbr",
        "rbr",
    ]
    return mock_registry


@pytest.fixture
def health_command_context(
    mock_storage: AsyncMock,
    mock_stac_manager: AsyncMock,
    mock_index_registry: MagicMock,
) -> CommandContext:
    """Command context for health check testing"""
    return CommandContext(
        job_id="health-check-123",
        fire_event_name="health-check",
        geometry={
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {},
        },
        storage=mock_storage,
        stac_manager=mock_stac_manager,
        index_registry=mock_index_registry,
        metadata={"check_type": "health"},
    )


class TestHealthCheckCommand:
    """Test suite for HealthCheckCommand"""

    def test_command_properties(self) -> None:
        """Test basic command properties"""
        command = HealthCheckCommand()

        assert command.get_command_name() == "health_check"
        assert command.get_estimated_duration_seconds() == 0.1
        assert command.supports_retry() is True
        assert command.get_dependencies() == []
        assert command.get_required_permissions() == []

    def test_validate_context_success(
        self, health_command_context: CommandContext
    ) -> None:
        """Test successful context validation"""
        command = HealthCheckCommand()
        assert command.validate_context(health_command_context) is True

    def test_validate_context_missing_job_id(
        self, health_command_context: CommandContext
    ) -> None:
        """Test context validation with missing job_id"""
        health_command_context.job_id = ""
        command = HealthCheckCommand()
        assert command.validate_context(health_command_context) is False

    @pytest.mark.asyncio
    async def test_execute_all_healthy(
        self, health_command_context: CommandContext, mock_index_registry: MagicMock
    ) -> None:
        """Test execution when all components are healthy"""
        command = HealthCheckCommand()
        result = await command.execute(health_command_context)

        # Verify result
        assert result.status == CommandStatus.SUCCESS
        assert result.job_id == "health-check-123"
        assert result.fire_event_name == "health-check"
        assert result.command_name == "health_check"
        assert result.is_success() is True

        # Verify result data
        assert result.data is not None
        assert result.data["overall_status"] == "healthy"
        assert "timestamp" in result.data
        assert "checks" in result.data
        assert result.data["unhealthy_components"] == 0

        # Verify component checks
        checks = result.data["checks"]
        assert "storage" in checks
        assert "stac_manager" in checks
        assert "index_registry" in checks

        # All should be healthy
        assert checks["storage"]["status"] == "healthy"
        assert checks["stac_manager"]["status"] == "healthy"
        assert checks["index_registry"]["status"] == "healthy"

        # Verify index registry was queried
        mock_index_registry.get_available_calculators.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_with_unhealthy_component(
        self, health_command_context: CommandContext, mock_index_registry: MagicMock
    ) -> None:
        """Test execution when index registry fails"""
        # Make index registry fail
        mock_index_registry.get_available_calculators.side_effect = Exception(
            "Registry failed"
        )

        command = HealthCheckCommand()
        result = await command.execute(health_command_context)

        # Should still succeed but report unhealthy status
        assert result.status == CommandStatus.SUCCESS
        assert result.data is not None
        assert result.data["overall_status"] == "unhealthy"
        assert result.data["unhealthy_components"] == 1

        # Verify index registry is marked unhealthy
        checks = result.data["checks"]
        assert checks["index_registry"]["status"] == "unhealthy"
        assert "error" in checks["index_registry"]
        assert "Registry failed" in checks["index_registry"]["error"]

        # Other components should still be healthy
        assert checks["storage"]["status"] == "healthy"
        assert checks["stac_manager"]["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_execute_with_exception_in_health_check(
        self, health_command_context: CommandContext
    ) -> None:
        """Test execution when health check itself fails"""
        # Make storage throw an exception when we try to check it
        health_command_context.storage = None  # type: ignore  # This should trigger not_configured, not an exception

        command = HealthCheckCommand()
        result = await command.execute(health_command_context)

        # Should succeed but show storage as not configured
        assert result.status == CommandStatus.SUCCESS
        assert result.data is not None

        # Verify storage is marked as not configured
        checks = result.data["checks"]
        assert checks["storage"]["status"] == "not_configured"

    def test_str_and_repr(self) -> None:
        """Test string representations"""
        command = HealthCheckCommand()

        str_repr = str(command)
        assert "HealthCheckCommand" in str_repr
        assert "health_check" in str_repr

        repr_str = repr(command)
        assert "HealthCheckCommand" in repr_str
        assert "name='health_check'" in repr_str
        assert "estimated_duration=0.1s" in repr_str
        assert "supports_retry=True" in repr_str
