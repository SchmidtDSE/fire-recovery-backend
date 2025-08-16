from typing import Any, Dict, List, Optional, Type
import logging
from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.core.storage.storage_factory import StorageFactory
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry


logger = logging.getLogger(__name__)


class CommandRegistry:
    """
    Registry for managing command instances with proper dependency injection.
    
    The CommandRegistry acts as a factory for creating command instances
    with all required dependencies properly injected. It follows the
    Dependency Inversion Principle by depending on abstractions rather
    than concrete implementations.
    
    Usage:
        registry = CommandRegistry(storage_factory, stac_manager, index_registry)
        context = CommandContext(...)
        command = registry.create_command("fire_severity_analysis", context)
        result = await command.execute(context)
    """
    
    def __init__(
        self,
        storage_factory: StorageFactory,
        stac_manager: STACJSONManager,
        index_registry: IndexRegistry,
    ):
        """
        Initialize command registry with required dependencies.
        
        Args:
            storage_factory: Factory for creating storage interfaces
            stac_manager: STAC catalog manager for metadata operations
            index_registry: Registry of spectral index calculators
        """
        logger.info("Initializing CommandRegistry")
        
        self._storage_factory = storage_factory
        self._stac_manager = stac_manager
        self._index_registry = index_registry
        
        # Registry of command classes keyed by command name
        self._command_classes: Dict[str, Type[Command]] = {}
        
        # Cache of command instances for reuse (stateless commands only)
        self._command_instances: Dict[str, Command] = {}
        
        self._setup_commands()
        self._validate_registry()
    
    def _setup_commands(self) -> None:
        """Initialize and register all available command classes"""
        logger.info("Setting up command classes")
        
        # Import command implementations
        # Note: Importing here to avoid circular dependencies
        try:
            from src.commands.impl.fire_severity_command import FireSeverityAnalysisCommand
            from src.commands.impl.boundary_refinement_command import BoundaryRefinementCommand
            from src.commands.impl.vegetation_analysis_command import VegetationAnalysisCommand
            
            # Register command classes
            self._register_command_class(FireSeverityAnalysisCommand)
            self._register_command_class(BoundaryRefinementCommand)
            self._register_command_class(VegetationAnalysisCommand)
            
        except ImportError as e:
            logger.warning(f"Some command implementations not available: {e}")
            # In development, it's ok if not all commands are implemented yet
        
        logger.info(
            f"Registered {len(self._command_classes)} command classes: "
            f"{list(self._command_classes.keys())}"
        )
    
    def _register_command_class(self, command_class: Type[Command]) -> None:
        """Register a command class in the registry"""
        # Create temporary instance to get command name
        temp_instance = command_class()
        command_name = temp_instance.get_command_name()
        
        if command_name in self._command_classes:
            logger.warning(f"Command '{command_name}' already registered, overriding")
        
        self._command_classes[command_name] = command_class
        logger.debug(f"Registered command class: {command_name}")
    
    def _validate_registry(self) -> None:
        """Validate that all registered commands have satisfied dependencies"""
        logger.info("Validating command registry dependencies")
        
        for command_name, command_class in self._command_classes.items():
            try:
                # Create temporary instance to check dependencies
                temp_instance = command_class()
                dependencies = temp_instance.get_dependencies()
                
                # Check that all dependencies are available
                for dep in dependencies:
                    if dep not in self._command_classes:
                        logger.error(
                            f"Command '{command_name}' depends on '{dep}' which is not registered"
                        )
                        raise ValueError(
                            f"Unresolved dependency: {command_name} -> {dep}"
                        )
                
                logger.debug(f"Command '{command_name}' dependencies satisfied: {dependencies}")
                
            except Exception as e:
                logger.error(f"Failed to validate command '{command_name}': {e}")
                raise
        
        logger.info("All command dependencies validated successfully")
    
    def create_command(self, command_name: str, context: CommandContext) -> Command:
        """
        Create a command instance with proper dependency injection.
        
        Args:
            command_name: Name of the command to create
            context: Context that will be used for execution (for validation)
            
        Returns:
            Command instance with dependencies injected
            
        Raises:
            ValueError: If command_name is not registered
            TypeError: If context is invalid
        """
        if command_name not in self._command_classes:
            available_commands = list(self._command_classes.keys())
            raise ValueError(
                f"Command '{command_name}' not found. Available commands: {available_commands}"
            )
        
        # Validate context before creating command
        if not isinstance(context, CommandContext):
            raise TypeError("context must be a CommandContext instance")
        
        command_class = self._command_classes[command_name]
        
        # Create command instance (assuming stateless for now)
        command = command_class()
        
        # Inject dependencies into the context if not already present
        if context.storage is None:
            context.storage = self._storage_factory.get_temp_storage()
        
        if context.stac_manager is None:
            context.stac_manager = self._stac_manager
        
        if context.index_registry is None:
            context.index_registry = self._index_registry
        
        # Validate that the command can work with this context
        if not command.validate_context(context):
            raise ValueError(
                f"Context validation failed for command '{command_name}'"
            )
        
        logger.debug(f"Created command instance: {command_name}")
        return command
    
    def get_available_commands(self) -> List[str]:
        """
        Get list of all registered command names.
        
        Returns:
            List of available command names
        """
        return list(self._command_classes.keys())
    
    def get_command_info(self, command_name: str) -> Dict[str, Any]:
        """
        Get information about a specific command.
        
        Args:
            command_name: Name of the command
            
        Returns:
            Dictionary containing command information
            
        Raises:
            ValueError: If command_name is not registered
        """
        if command_name not in self._command_classes:
            raise ValueError(f"Command '{command_name}' not found")
        
        command_class = self._command_classes[command_name]
        temp_instance = command_class()
        
        return {
            "name": command_name,
            "class": command_class.__name__,
            "estimated_duration_seconds": temp_instance.get_estimated_duration_seconds(),
            "supports_retry": temp_instance.supports_retry(),
            "dependencies": temp_instance.get_dependencies(),
            "required_permissions": temp_instance.get_required_permissions(),
        }
    
    def get_commands_by_dependency(self, dependency_name: str) -> List[str]:
        """
        Get list of commands that depend on a specific command.
        
        Args:
            dependency_name: Name of the dependency command
            
        Returns:
            List of command names that depend on the given command
        """
        dependent_commands = []
        
        for command_name, command_class in self._command_classes.items():
            temp_instance = command_class()
            if dependency_name in temp_instance.get_dependencies():
                dependent_commands.append(command_name)
        
        return dependent_commands
    
    def validate_command_chain(self, command_names: List[str]) -> bool:
        """
        Validate that a chain of commands can be executed in order.
        
        Args:
            command_names: List of command names in execution order
            
        Returns:
            True if the chain is valid, False otherwise
        """
        executed_commands = set()
        
        for command_name in command_names:
            if command_name not in self._command_classes:
                logger.error(f"Command '{command_name}' not found in chain validation")
                return False
            
            command_class = self._command_classes[command_name]
            temp_instance = command_class()
            dependencies = temp_instance.get_dependencies()
            
            # Check that all dependencies have been executed
            for dep in dependencies:
                if dep not in executed_commands:
                    logger.error(
                        f"Command '{command_name}' depends on '{dep}' which has not been executed yet"
                    )
                    return False
            
            executed_commands.add(command_name)
        
        logger.debug(f"Command chain validation successful: {command_names}")
        return True
    
    def add_command_class(self, command_class: Type[Command]) -> None:
        """
        Add a new command class to the registry at runtime.
        
        Args:
            command_class: Command class to add
            
        Raises:
            ValueError: If command with same name already exists
        """
        temp_instance = command_class()
        command_name = temp_instance.get_command_name()
        
        if command_name in self._command_classes:
            raise ValueError(f"Command '{command_name}' already exists")
        
        self._register_command_class(command_class)
        self._validate_registry()
        
        logger.info(f"Added new command class: {command_name}")
    
    def remove_command_class(self, command_name: str) -> bool:
        """
        Remove a command class from the registry.
        
        Args:
            command_name: Name of command to remove
            
        Returns:
            True if command was removed, False if not found
        """
        if command_name not in self._command_classes:
            return False
        
        # Check if other commands depend on this one
        dependent_commands = self.get_commands_by_dependency(command_name)
        if dependent_commands:
            raise ValueError(
                f"Cannot remove command '{command_name}' - it has dependents: {dependent_commands}"
            )
        
        del self._command_classes[command_name]
        if command_name in self._command_instances:
            del self._command_instances[command_name]
        
        logger.info(f"Removed command class: {command_name}")
        return True