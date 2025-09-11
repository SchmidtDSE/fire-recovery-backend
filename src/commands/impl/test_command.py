import asyncio
from typing import List, Tuple
from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult


class TestCommand(Command):
    """
    Simple test command to validate command infrastructure.

    This command performs basic validation and creates a test result
    to ensure the command pattern implementation works correctly.
    """

    def get_command_name(self) -> str:
        return "test_command"

    def validate_context(self, context: CommandContext) -> Tuple[bool, str]:
        """Validate that context has required fields"""
        try:
            # Check required fields
            required_fields = [
                "job_id",
                "fire_event_name",
                "geometry",
                "storage",
                "stac_manager",
                "index_registry",
            ]
            for field in required_fields:
                if not hasattr(context, field) or getattr(context, field) is None:
                    self.logger.error(f"Context missing required field: {field}")
                    return False

            return True, ""
        except Exception as e:
            self.logger.error(f"Context validation failed: {e}")
            return False, e

    async def execute(self, context: CommandContext) -> CommandResult:
        """Execute test command with mock operations"""
        start_time = asyncio.get_event_loop().time()

        try:
            # Simulate some processing time
            await asyncio.sleep(0.1)

            # Test storage interface
            test_data = b"test command output"
            test_path = f"{context.job_id}/test_output.txt"
            await context.storage.save_bytes(test_data, test_path, temporary=True)

            # Test that we can retrieve it
            retrieved_data = await context.storage.get_bytes(test_path)
            if retrieved_data != test_data:
                raise ValueError("Storage round-trip test failed")

            # Calculate execution time
            execution_time_ms = (asyncio.get_event_loop().time() - start_time) * 1000

            # Create successful result
            result = CommandResult.success(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time_ms,
                data={
                    "test_passed": True,
                    "storage_test": "success",
                    "processed_bytes": len(test_data),
                },
                asset_urls={
                    "test_output": f"temp://{test_path}",
                },
                metadata={
                    "test_command_version": "1.0.0",
                    "validation_checks": ["context", "storage", "timing"],
                },
            )

            self.logger.info(
                f"Test command completed successfully for job {context.job_id}"
            )
            return result

        except Exception as e:
            execution_time_ms = (asyncio.get_event_loop().time() - start_time) * 1000

            self.logger.error(f"Test command failed: {e}")
            return CommandResult.failure(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time_ms,
                error_message=str(e),
                error_details={
                    "exception_type": type(e).__name__,
                    "test_phase": "execution",
                },
            )

    def get_estimated_duration_seconds(self) -> float:
        return 1.0  # Very fast test command

    def supports_retry(self) -> bool:
        return True  # Test command is safe to retry

    def get_dependencies(self) -> List[str]:
        return []  # No dependencies

    def get_required_permissions(self) -> List[str]:
        return []  # No special permissions needed
