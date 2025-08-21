import time
import logging
from typing import List

from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult, CommandStatus

logger = logging.getLogger(__name__)


class HealthCheckCommand(Command):
    """
    Simple health check command that validates system components.
    
    This demonstrates the command pattern for a minimal endpoint
    without external dependencies for testing purposes.
    """
    
    def get_command_name(self) -> str:
        return "health_check"
    
    def get_estimated_duration_seconds(self) -> float:
        return 0.1  # Very fast operation
    
    def supports_retry(self) -> bool:
        return True
    
    def get_dependencies(self) -> List[str]:
        return []  # No command dependencies
    
    def get_required_permissions(self) -> List[str]:
        return []  # Public endpoint
    
    def validate_context(self, context: CommandContext) -> bool:
        """Validate context - healthz needs minimal validation"""
        if not context.job_id:
            logger.error("job_id is required for health check")
            return False
        return True
    
    async def execute(self, context: CommandContext) -> CommandResult:
        """Execute health check workflow"""
        start_time = time.time()
        
        logger.info(f"Starting health check for job {context.job_id}")
        
        try:
            # Perform basic health checks
            health_status = await self._check_system_health(context)
            
            execution_time = (time.time() - start_time) * 1000
            
            logger.info(f"Health check completed for job {context.job_id} in {execution_time:.2f}ms")
            
            return CommandResult.success(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time,
                data=health_status
            )
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Health check failed for job {context.job_id}: {str(e)}", exc_info=True)
            
            return CommandResult.failure(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time,
                error_message=str(e),
                error_details={
                    "error_type": type(e).__name__,
                    "check_type": "system_health"
                }
            )
    
    async def _check_system_health(self, context: CommandContext) -> dict:
        """Perform basic system health checks"""
        checks = {}
        
        # Check storage availability (if available)
        if context.storage:
            try:
                # Simple check - storage interface exists
                checks["storage"] = {
                    "status": "healthy",
                    "type": type(context.storage).__name__
                }
            except Exception as e:
                checks["storage"] = {
                    "status": "unhealthy", 
                    "error": str(e)
                }
        else:
            checks["storage"] = {"status": "not_configured"}
        
        # Check STAC manager availability (if available)
        if context.stac_manager:
            try:
                # Simple check - stac manager exists
                checks["stac_manager"] = {
                    "status": "healthy",
                    "type": type(context.stac_manager).__name__
                }
            except Exception as e:
                checks["stac_manager"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
        else:
            checks["stac_manager"] = {"status": "not_configured"}
        
        # Check index registry availability (if available)
        if context.index_registry:
            try:
                # Simple check - get available calculators
                available_calculators = context.index_registry.get_available_calculators()
                checks["index_registry"] = {
                    "status": "healthy",
                    "available_calculators": len(available_calculators),
                    "calculators": list(available_calculators)
                }
            except Exception as e:
                checks["index_registry"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
        else:
            checks["index_registry"] = {"status": "not_configured"}
        
        # Overall status
        unhealthy_count = sum(1 for check in checks.values() 
                             if check.get("status") == "unhealthy")
        
        overall_status = "healthy" if unhealthy_count == 0 else "unhealthy"
        
        return {
            "overall_status": overall_status,
            "timestamp": time.time(),
            "checks": checks,
            "unhealthy_components": unhealthy_count
        }