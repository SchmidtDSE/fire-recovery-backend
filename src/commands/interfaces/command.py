from abc import ABC, abstractmethod
from typing import List
import logging
from .command_context import CommandContext
from .command_result import CommandResult


class Command(ABC):
    """
    Base interface for all commands in the fire recovery system.
    
    Commands encapsulate business logic operations and can be executed
    independently of the API layer. This design enables better testability,
    separation of concerns, and serverless compatibility.
    
    All commands must implement:
    - execute(): The main business logic
    - get_command_name(): Unique identifier for the command
    - validate_context(): Context validation before execution
    - get_required_permissions(): Security requirements
    """
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    @abstractmethod
    async def execute(self, context: CommandContext) -> CommandResult:
        """
        Execute the command with the given context.
        
        This method contains the core business logic for the command.
        It should be idempotent when possible and handle errors gracefully.
        
        Args:
            context: CommandContext containing all necessary data and dependencies
            
        Returns:
            CommandResult with execution status, data, and metadata
            
        Raises:
            CommandExecutionError: When command execution fails
            ValidationError: When context validation fails
        """
        pass
    
    @abstractmethod
    def get_command_name(self) -> str:
        """
        Return unique identifier for this command.
        
        This name is used for logging, metrics, and command registry.
        Should be lowercase with underscores (e.g., 'fire_severity_analysis').
        
        Returns:
            Unique command name string
        """
        pass
    
    @abstractmethod
    def validate_context(self, context: CommandContext) -> bool:
        """
        Validate that the context contains all required data for execution.
        
        This method should check that all required fields are present
        and contain valid data. It should not modify the context.
        
        Args:
            context: CommandContext to validate
            
        Returns:
            True if context is valid, False otherwise
        """
        pass
    
    def get_required_permissions(self) -> List[str]:
        """
        Return list of permissions required to execute this command.
        
        Override this method if the command requires specific permissions.
        Default implementation requires no special permissions.
        
        Returns:
            List of required permission strings
        """
        return []
    
    def get_estimated_duration_seconds(self) -> float:
        """
        Return estimated execution duration in seconds.
        
        This is used for timeout configuration and user expectations.
        Override for commands with known execution patterns.
        
        Returns:
            Estimated duration in seconds (default: 300 = 5 minutes)
        """
        return 300.0
    
    def supports_retry(self) -> bool:
        """
        Return whether this command supports retry on failure.
        
        Commands that are idempotent and don't have side effects
        should return True. Commands that modify external state
        or are not idempotent should return False.
        
        Returns:
            True if command supports retry, False otherwise
        """
        return True
    
    def get_dependencies(self) -> List[str]:
        """
        Return list of other commands that must complete before this one.
        
        This is used for command orchestration and dependency management.
        Override for commands that depend on outputs from other commands.
        
        Returns:
            List of command names that are dependencies
        """
        return []
    
    async def pre_execute_hook(self, context: CommandContext) -> None:
        """
        Hook called before command execution.
        
        Override this method to perform setup operations like
        logging, metrics collection, or resource allocation.
        
        Args:
            context: CommandContext for execution
        """
        self.logger.info(
            f"Executing command '{self.get_command_name()}' for job {context.job_id}"
        )
    
    async def post_execute_hook(
        self, context: CommandContext, result: CommandResult
    ) -> None:
        """
        Hook called after command execution.
        
        Override this method to perform cleanup operations like
        resource deallocation, metrics collection, or notifications.
        
        Args:
            context: CommandContext that was used for execution
            result: CommandResult from execution
        """
        status_msg = "successfully" if result.is_success() else "with errors"
        self.logger.info(
            f"Command '{self.get_command_name()}' completed {status_msg} "
            f"for job {context.job_id} in {result.execution_time_ms:.2f}ms"
        )
    
    def __str__(self) -> str:
        """String representation of the command"""
        return f"{self.__class__.__name__}(name='{self.get_command_name()}')"
    
    def __repr__(self) -> str:
        """Detailed string representation of the command"""
        return (
            f"{self.__class__.__name__}("
            f"name='{self.get_command_name()}', "
            f"estimated_duration={self.get_estimated_duration_seconds()}s, "
            f"supports_retry={self.supports_retry()}, "
            f"dependencies={self.get_dependencies()}"
            f")"
        )