import asyncio
import time
from typing import Any, Dict, List, Optional
import logging
from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult
from src.commands.registry.command_registry import CommandRegistry


logger = logging.getLogger(__name__)


class CommandExecutionError(Exception):
    """Exception raised when command execution fails"""

    def __init__(self, command_name: str, original_error: Exception):
        self.command_name = command_name
        self.original_error = original_error
        super().__init__(
            f"Command '{command_name}' execution failed: {str(original_error)}"
        )


class CommandExecutor:
    """
    Orchestrates command execution with error handling, logging, and metrics.

    The CommandExecutor provides a standardized way to execute commands
    with proper error handling, timeout management, retry logic, and
    comprehensive logging for debugging and monitoring.

    Features:
    - Timeout management with configurable limits
    - Retry logic for transient failures
    - Comprehensive logging and metrics collection
    - Resource cleanup and error recovery
    - Progress tracking for long-running operations
    """

    def __init__(
        self,
        command_registry: CommandRegistry,
        default_timeout_seconds: float = 600.0,  # 10 minutes
        max_retries: int = 3,
        retry_delay_seconds: float = 1.0,
    ):
        """
        Initialize command executor with configuration.

        Args:
            command_registry: Registry for creating command instances
            default_timeout_seconds: Default timeout for command execution
            max_retries: Maximum number of retry attempts for failed commands
            retry_delay_seconds: Delay between retry attempts
        """
        logger.info("Initializing CommandExecutor")

        self._command_registry = command_registry
        self._default_timeout_seconds = default_timeout_seconds
        self._max_retries = max_retries
        self._retry_delay_seconds = retry_delay_seconds

        # Track execution metrics
        self._execution_count = 0
        self._success_count = 0
        self._failure_count = 0
        self._total_execution_time = 0.0

    async def execute_command(
        self,
        command_name: str,
        context: CommandContext,
        timeout_seconds: Optional[float] = None,
        max_retries: Optional[int] = None,
    ) -> CommandResult:
        """
        Execute a single command with error handling and retry logic.

        Args:
            command_name: Name of the command to execute
            context: Execution context with all required data
            timeout_seconds: Custom timeout (overrides default)
            max_retries: Custom retry count (overrides default)

        Returns:
            CommandResult with execution status and data

        Raises:
            CommandExecutionError: When command execution fails after all retries
            ValueError: When command_name is not found or context is invalid
        """
        start_time = time.time()
        self._execution_count += 1

        # Use custom values or fall back to defaults
        timeout = timeout_seconds or self._default_timeout_seconds
        retries = max_retries if max_retries is not None else self._max_retries

        logger.info(
            f"Starting execution of command '{command_name}' for job {context.job_id} "
            f"(timeout: {timeout}s, max_retries: {retries})"
        )

        last_error = None

        # Create command instance outside retry loop - let ValueError propagate
        try:
            command = self._command_registry.create_command(command_name, context)
        except (ValueError, TypeError) as e:
            # These are configuration/setup errors that shouldn't be retried
            logger.error(f"Failed to create command '{command_name}': {e}")
            raise

        for attempt in range(retries + 1):  # +1 for initial attempt
            try:
                # Execute with timeout
                result = await self._execute_with_timeout(
                    command, context, timeout, attempt
                )

                # Success - update metrics and return
                execution_time = (time.time() - start_time) * 1000  # Convert to ms
                self._success_count += 1
                self._total_execution_time += execution_time

                logger.info(
                    f"Command '{command_name}' completed successfully "
                    f"for job {context.job_id} in {execution_time:.2f}ms "
                    f"(attempt {attempt + 1}/{retries + 1})"
                )

                return result

            except Exception as e:
                last_error = e
                execution_time = (time.time() - start_time) * 1000

                logger.warning(
                    f"Command '{command_name}' failed for job {context.job_id} "
                    f"(attempt {attempt + 1}/{retries + 1}): {str(e)}"
                )

                # If this was the last attempt, don't retry
                if attempt >= retries:
                    break

                # Check if command supports retry
                if attempt > 0:  # Only check after first attempt
                    try:
                        command = self._command_registry.create_command(
                            command_name, context
                        )
                        if not command.supports_retry():
                            logger.info(
                                f"Command '{command_name}' does not support retry, stopping"
                            )
                            break
                    except Exception:
                        # If we can't create command to check retry support, don't retry
                        break

                # Wait before retry
                if attempt < retries:
                    await asyncio.sleep(self._retry_delay_seconds)

        # All attempts failed
        execution_time = (time.time() - start_time) * 1000
        self._failure_count += 1
        self._total_execution_time += execution_time

        logger.error(
            f"Command '{command_name}' failed for job {context.job_id} "
            f"after {retries + 1} attempts in {execution_time:.2f}ms"
        )

        # Create failure result
        error_message = str(last_error) if last_error else "Unknown error"
        result = CommandResult.failure(
            job_id=context.job_id,
            fire_event_name=context.fire_event_name,
            command_name=command_name,
            execution_time_ms=execution_time,
            error_message=error_message,
            error_details={"attempts": retries + 1, "last_error": str(last_error)},
        )

        return result

    async def _execute_with_timeout(
        self,
        command: Command,
        context: CommandContext,
        timeout_seconds: float,
        attempt: int,
    ) -> CommandResult:
        """Execute command with timeout handling"""
        try:
            # Pre-execution hook
            await command.pre_execute_hook(context)

            # Execute with timeout
            start_time = time.time()
            result = await asyncio.wait_for(
                command.execute(context), timeout=timeout_seconds
            )
            execution_time_ms = (time.time() - start_time) * 1000

            # Update result with actual execution time if not set
            if result.execution_time_ms == 0:
                result.execution_time_ms = execution_time_ms

            # Post-execution hook
            await command.post_execute_hook(context, result)

            return result

        except asyncio.TimeoutError:
            execution_time_ms = timeout_seconds * 1000
            error_msg = f"Command execution timed out after {timeout_seconds}s"

            logger.error(
                f"Command '{command.get_command_name()}' timed out "
                f"for job {context.job_id} (attempt {attempt + 1})"
            )

            return CommandResult.failure(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=command.get_command_name(),
                execution_time_ms=execution_time_ms,
                error_message=error_msg,
                error_details={
                    "timeout_seconds": timeout_seconds,
                    "attempt": attempt + 1,
                },
            )

        except Exception as e:
            execution_time_ms = (
                (time.time() - start_time) * 1000 if "start_time" in locals() else 0
            )

            logger.error(
                f"Command '{command.get_command_name()}' raised exception "
                f"for job {context.job_id} (attempt {attempt + 1}): {str(e)}",
                exc_info=True,
            )

            return CommandResult.failure(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=command.get_command_name(),
                execution_time_ms=execution_time_ms,
                error_message=str(e),
                error_details={
                    "exception_type": type(e).__name__,
                    "attempt": attempt + 1,
                },
            )

    async def execute_command_chain(
        self,
        command_names: List[str],
        context: CommandContext,
        stop_on_failure: bool = True,
    ) -> List[CommandResult]:
        """
        Execute a chain of commands in sequence.

        Args:
            command_names: List of command names in execution order
            context: Shared execution context
            stop_on_failure: Whether to stop execution if a command fails

        Returns:
            List of CommandResult objects for each command
        """
        logger.info(
            f"Starting command chain execution for job {context.job_id}: {command_names}"
        )

        # Validate command chain
        if not self._command_registry.validate_command_chain(command_names):
            raise ValueError("Invalid command chain - dependency order not satisfied")

        results = []

        for i, command_name in enumerate(command_names):
            logger.info(
                f"Executing command {i + 1}/{len(command_names)}: {command_name}"
            )

            result = await self.execute_command(command_name, context)
            results.append(result)

            # Update context with results from previous commands
            if result.is_success() and result.has_assets() and result.asset_urls:
                for asset_name, asset_url in result.asset_urls.items():
                    context.add_metadata(f"{command_name}_{asset_name}_url", asset_url)

            # Stop on failure if requested
            if stop_on_failure and result.is_failure():
                logger.error(
                    f"Command chain stopped at command '{command_name}' due to failure"
                )
                break

        successful_commands = sum(1 for r in results if r.is_success())
        logger.info(
            f"Command chain completed for job {context.job_id}: "
            f"{successful_commands}/{len(results)} commands successful"
        )

        return results

    def get_execution_metrics(self) -> Dict[str, Any]:
        """
        Get execution metrics for monitoring and debugging.

        Returns:
            Dictionary containing execution statistics
        """
        avg_execution_time = (
            self._total_execution_time / self._execution_count
            if self._execution_count > 0
            else 0
        )

        success_rate = (
            (self._success_count / self._execution_count) * 100
            if self._execution_count > 0
            else 0
        )

        return {
            "total_executions": self._execution_count,
            "successful_executions": self._success_count,
            "failed_executions": self._failure_count,
            "success_rate_percent": round(success_rate, 2),
            "average_execution_time_ms": round(avg_execution_time, 2),
            "total_execution_time_ms": round(self._total_execution_time, 2),
            "default_timeout_seconds": self._default_timeout_seconds,
            "max_retries": self._max_retries,
        }

    def reset_metrics(self) -> None:
        """Reset execution metrics (useful for testing)"""
        self._execution_count = 0
        self._success_count = 0
        self._failure_count = 0
        self._total_execution_time = 0.0
        logger.info("Execution metrics reset")
