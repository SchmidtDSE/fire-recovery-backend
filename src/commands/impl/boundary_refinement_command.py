import logging
import time
from datetime import datetime, timezone
from typing import List, Tuple

from geojson_pydantic import Polygon, Feature

from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult
from src.util.boundary_utils import (
    process_and_upload_geojson,
    process_cog_with_boundary,
)

logger = logging.getLogger(__name__)


class BoundaryRefinementCommand(Command):
    """
    Command for refining fire boundaries based on user-drawn polygons.

    Supports iterative refinement where users can refine boundaries multiple
    times, with each iteration overwriting previous refined outputs while
    preserving original coarse assets.

    This command:
    - Processes and uploads refined boundary GeoJSON
    - Retrieves original coarse fire severity COGs from STAC
    - Crops COGs to refined boundary using existing utilities
    - Creates new STAC items with refined assets
    - Supports multiple refinement iterations with overwriting pattern
    """

    def get_command_name(self) -> str:
        return "boundary_refinement"

    def get_estimated_duration_seconds(self) -> float:
        return 180.0  # 3 minutes for COG processing and boundary refinement

    def supports_retry(self) -> bool:
        return True

    def get_dependencies(self) -> List[str]:
        return []  # No command dependencies

    def get_required_permissions(self) -> List[str]:
        return ["stac:read", "stac:write", "storage:write", "computation:execute"]

    def validate_context(self, context: CommandContext) -> Tuple[bool, str]:
        """
        Validate that context contains all required data for boundary refinement.

        Args:
            context: CommandContext to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Standard validation
        if not all([context.job_id, context.fire_event_name, context.geometry]):
            return False, "job_id, fire_event_name, and geometry are required"

        if not all([context.storage, context.storage_factory, context.stac_manager]):
            return False, "storage, storage_factory, and stac_manager are required"

        # Boundary refinement specific validation
        if not context.geometry:
            return (
                False,
                "refined boundary geometry is required for boundary refinement",
            )

        # Ensure geometry is valid format (Polygon or Feature)
        if not isinstance(context.geometry, (Polygon, Feature)):
            return False, "geometry must be a Polygon or Feature object"

        return True, "Context validation passed"

    async def execute(self, context: CommandContext) -> CommandResult:
        """
        Execute boundary refinement with iterative refinement support.

        This method:
        1. Validates the context
        2. Processes and uploads refined boundary GeoJSON
        3. Retrieves original STAC item with coarse COGs
        4. Processes each COG with refined boundary (overwrite pattern)
        5. Creates/updates STAC metadata for refined assets
        6. Returns success result with asset URLs

        Args:
            context: CommandContext containing all necessary data

        Returns:
            CommandResult with execution status and asset URLs
        """
        start_time = time.time()

        try:
            # Validate context
            is_valid, error_msg = self.validate_context(context)
            if not is_valid:
                execution_time_ms = (time.time() - start_time) * 1000
                return CommandResult.failure(
                    job_id=context.job_id,
                    fire_event_name=context.fire_event_name,
                    command_name=self.get_command_name(),
                    execution_time_ms=execution_time_ms,
                    error_message=f"Context validation failed: {error_msg}",
                )

            logger.info(f"Starting boundary refinement for job {context.job_id}")

            # Step 1: Process and upload the refined boundary GeoJSON
            geojson_url, valid_geojson, bbox = await process_and_upload_geojson(
                geometry=context.geometry,
                fire_event_name=context.fire_event_name,
                job_id=context.job_id,
                filename="refined_boundary",
                storage_factory=context.storage_factory,
            )

            logger.info(f"Refined boundary uploaded: {geojson_url}")

            # Step 2: Get the original/coarse fire severity COG URLs from STAC
            stac_id = f"{context.fire_event_name}-severity-{context.job_id}"
            original_cog_item = await context.stac_manager.get_item_by_id(stac_id)
            if not original_cog_item:
                execution_time_ms = (time.time() - start_time) * 1000
                return CommandResult.failure(
                    job_id=context.job_id,
                    fire_event_name=context.fire_event_name,
                    command_name=self.get_command_name(),
                    execution_time_ms=execution_time_ms,
                    error_message=f"Original COG not found for job ID {context.job_id}",
                    error_details={"stac_id": stac_id},
                )

            logger.info(
                f"Processing {len(original_cog_item['assets'])} fire severity metrics"
            )

            # Step 3: Process each COG with the refined boundary (overwrite pattern)
            refined_cog_urls = {}
            failed_metrics = []

            for metric, cog_data in original_cog_item["assets"].items():
                try:
                    coarse_cog_url = cog_data["href"]

                    # Process COG with refined boundary - use "refined_" prefix for overwriting
                    refined_cog_url = await process_cog_with_boundary(
                        original_cog_url=coarse_cog_url,
                        valid_geojson=valid_geojson,
                        fire_event_name=context.fire_event_name,
                        job_id=context.job_id,
                        output_filename=f"refined_{metric}",  # Overwrite pattern
                        storage_factory=context.storage_factory,
                    )

                    refined_cog_urls[metric] = refined_cog_url
                    logger.info(
                        f"Processed refined COG for metric {metric}: {refined_cog_url}"
                    )

                except Exception as e:
                    logger.error(
                        f"COG processing failed for metric {metric}: {str(e)}",
                        exc_info=True,
                    )
                    failed_metrics.append(metric)

            # Check if we have any successful COG processing
            if not refined_cog_urls:
                execution_time_ms = (time.time() - start_time) * 1000
                return CommandResult.failure(
                    job_id=context.job_id,
                    fire_event_name=context.fire_event_name,
                    command_name=self.get_command_name(),
                    execution_time_ms=execution_time_ms,
                    error_message="All COG processing operations failed",
                    error_details={"failed_metrics": failed_metrics},
                )

            # Step 4: Create STAC metadata for refined assets
            try:
                # Create the STAC item for refined COGs
                # Use the original pydantic geometry object which has __geo_interface__
                await context.stac_manager.create_fire_severity_item(
                    fire_event_name=context.fire_event_name,
                    job_id=context.job_id,
                    cog_urls=refined_cog_urls,
                    geometry=context.geometry,
                    datetime_str=original_cog_item["properties"]["datetime"],
                    boundary_type="refined",
                )

                # Create the STAC item for the refined boundary
                datetime_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                await context.stac_manager.create_boundary_item(
                    fire_event_name=context.fire_event_name,
                    job_id=context.job_id,
                    boundary_geojson_url=geojson_url,
                    bbox=bbox,
                    datetime_str=datetime_str,
                    boundary_type="refined",
                )

                logger.info("STAC metadata created successfully")

            except Exception as e:
                logger.error(f"STAC metadata creation failed: {str(e)}", exc_info=True)
                # This is a partial failure - we have assets but no metadata
                execution_time_ms = (time.time() - start_time) * 1000
                return CommandResult.partial_success(
                    job_id=context.job_id,
                    fire_event_name=context.fire_event_name,
                    command_name=self.get_command_name(),
                    execution_time_ms=execution_time_ms,
                    asset_urls={"boundary": geojson_url, **refined_cog_urls},
                    error_message="Assets created but STAC metadata creation failed",
                    error_details={"stac_error": str(e)},
                )

            # Success!
            execution_time_ms = (time.time() - start_time) * 1000

            # Prepare result data
            result_data = {
                "processed_metrics": list(refined_cog_urls.keys()),
                "failed_metrics": failed_metrics,
                "boundary_type": "refined",
                "iteration_count": context.get_metadata("refinement_iteration", 1),
            }

            # Prepare asset URLs
            asset_urls = {
                "boundary": geojson_url,
                **refined_cog_urls,
            }

            logger.info(
                f"Boundary refinement completed successfully in {execution_time_ms:.2f}ms"
            )

            # Return partial success if some metrics failed, otherwise full success
            if failed_metrics:
                return CommandResult.partial_success(
                    job_id=context.job_id,
                    fire_event_name=context.fire_event_name,
                    command_name=self.get_command_name(),
                    execution_time_ms=execution_time_ms,
                    data=result_data,
                    asset_urls=asset_urls,
                    error_message=f"Some metrics failed: {failed_metrics}",
                    metadata={"partial_success_reason": "some_cog_processing_failed"},
                )
            else:
                return CommandResult.success(
                    job_id=context.job_id,
                    fire_event_name=context.fire_event_name,
                    command_name=self.get_command_name(),
                    execution_time_ms=execution_time_ms,
                    data=result_data,
                    asset_urls=asset_urls,
                )

        except Exception as e:
            logger.error(
                f"Boundary refinement execution failed: {str(e)}", exc_info=True
            )
            execution_time_ms = (time.time() - start_time) * 1000
            return CommandResult.failure(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time_ms,
                error_message=f"Command execution failed: {str(e)}",
                error_details={"exception_type": type(e).__name__},
            )
