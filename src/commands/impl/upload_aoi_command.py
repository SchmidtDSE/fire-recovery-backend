import json
import logging
import time
from typing import Any, Dict, List, Tuple

from fastapi import UploadFile
from geojson_pydantic import Feature, FeatureCollection
from shapely.geometry import shape

from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult
# GeoJSONGeometry type removed - using Dict[str, Any] for dictionary-based geometry data
from src.util.polygon_ops import polygon_to_valid_geojson
from src.util.upload_blob import upload_to_gcs

logger = logging.getLogger(__name__)


class UploadAOICommand(Command):
    """
    Command for uploading and processing Area of Interest (AOI) data.

    Handles both GeoJSON and Shapefile uploads, validates geometry,
    normalizes format, and creates STAC boundary items. This command
    encapsulates the first step of the fire recovery workflow.

    Supports:
    - GeoJSON Feature and FeatureCollection uploads
    - Zipped Shapefile uploads
    - Geometry validation and normalization
    - Storage via abstraction layer
    - STAC metadata creation
    """

    def get_command_name(self) -> str:
        return "upload_aoi"

    def get_estimated_duration_seconds(self) -> float:
        return 30.0  # 30 seconds for file upload and validation

    def supports_retry(self) -> bool:
        return True

    def get_dependencies(self) -> List[str]:
        return []  # No command dependencies

    def get_required_permissions(self) -> List[str]:
        return ["storage:write", "stac:write"]

    def validate_context(self, context: CommandContext) -> bool:
        """Validate that context has required data for AOI upload"""
        if not context.job_id or not context.fire_event_name:
            logger.error("job_id and fire_event_name are required")
            return False

        if not context.storage:
            logger.error("storage interface is required")
            return False

        if not context.stac_manager:
            logger.error("stac_manager is required")
            return False

        # Check that either geometry or upload_data is provided
        upload_data = context.get_metadata("upload_data")
        upload_type = context.get_metadata("upload_type", "geojson")

        if upload_type == "geojson" and not context.geometry:
            logger.error("geometry is required for GeoJSON uploads")
            return False

        if upload_type == "shapefile" and not upload_data:
            logger.error("upload_data is required for shapefile uploads")
            return False

        return True

    async def execute(self, context: CommandContext) -> CommandResult:
        """Execute AOI upload and processing workflow"""
        start_time = time.time()

        logger.info(
            f"Starting AOI upload for job {context.job_id}, "
            f"fire event: {context.fire_event_name}"
        )

        try:
            # Determine upload type and process accordingly
            upload_data = context.get_metadata("upload_data")
            upload_type = context.get_metadata("upload_type", "geojson")

            if upload_type == "geojson":
                result_data = await self._process_geojson_upload(context)
            elif upload_type == "shapefile":
                result_data = await self._process_shapefile_upload(context, upload_data)
            else:
                raise ValueError(f"Unsupported upload type: {upload_type}")

            execution_time = (time.time() - start_time) * 1000

            logger.info(
                f"AOI upload completed for job {context.job_id} "
                f"in {execution_time:.2f}ms"
            )

            return CommandResult.success(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time,
                data=result_data,
                asset_urls=result_data.get("asset_urls", {}),
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(
                f"AOI upload failed for job {context.job_id}: {str(e)}", exc_info=True
            )

            return CommandResult.failure(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time,
                error_message=str(e),
                error_details={
                    "error_type": type(e).__name__,
                    "upload_type": context.get_metadata("upload_type", "unknown"),
                },
            )

    async def _process_geojson_upload(self, context: CommandContext) -> Dict[str, Any]:
        """Process GeoJSON upload (Feature or FeatureCollection)"""
        logger.info("Processing GeoJSON upload")

        try:
            # Get geometry from context and convert to dict format for processing
            geometry_obj = context.geometry
            if hasattr(geometry_obj, 'model_dump'):
                # Handle pydantic models - convert to dict
                geojson_dict: Dict[str, Any] = geometry_obj.model_dump()
            else:
                # Handle dict format (backwards compatibility)
                geojson_dict: Dict[str, Any] = geometry_obj  # type: ignore

            # Validate GeoJSON structure using geojson_pydantic
            if geojson_dict.get("type") == "FeatureCollection":
                FeatureCollection.model_validate(geojson_dict)
            elif geojson_dict.get("type") == "Feature":
                Feature.model_validate(geojson_dict)
            else:
                raise ValueError(
                    f"Unsupported GeoJSON type: {geojson_dict.get('type')}"
                )

            # Process and upload the GeoJSON
            geojson_url, valid_geojson, bbox = await self._process_and_upload_geojson(
                context=context, geometry=geojson_dict, filename="uploaded_aoi"
            )

            # Create STAC boundary item
            stac_item_url = await self._create_boundary_stac_item(
                context=context,
                boundary_url=geojson_url,
                bbox=bbox,
                boundary_type="uploaded",
            )

            return {
                "upload_type": "geojson",
                "status": "complete",
                "boundary_geojson_url": geojson_url,
                "stac_item_url": stac_item_url,
                "bbox": bbox,
                "geometry_validated": True,
                "asset_urls": {
                    "boundary_geojson": geojson_url,
                    "stac_item": stac_item_url,
                },
            }

        except Exception as e:
            # Add upload stage information for debugging
            setattr(e, "_upload_stage", "geojson_processing")
            raise

    async def _process_shapefile_upload(
        self, context: CommandContext, upload_file: UploadFile
    ) -> Dict[str, Any]:
        """Process Shapefile zip upload"""
        logger.info("Processing Shapefile upload")

        try:
            # Validate file extension
            if not upload_file.filename or not upload_file.filename.lower().endswith(
                ".zip"
            ):
                raise ValueError("Only zipped shapefiles (.zip) are supported")

            # Read file content
            content = await upload_file.read()

            # Upload zip file to storage
            zip_path = f"assets/{context.job_id}/uploads/original_shapefile.zip"
            shapefile_url = await context.storage.save_bytes(
                data=content, path=zip_path, temporary=False
            )

            # Create GCS fallback upload for backward compatibility
            zip_blob_name = (
                f"{context.fire_event_name}/{context.job_id}/original_shapefile.zip"
            )
            gcs_url = await upload_to_gcs(content, zip_blob_name)

            return {
                "upload_type": "shapefile",
                "status": "complete",
                "shapefile_url": shapefile_url,
                "gcs_shapefile_url": gcs_url,
                "filename": upload_file.filename,
                "file_size_bytes": len(content),
                "asset_urls": {"shapefile": shapefile_url, "gcs_shapefile": gcs_url},
            }

        except Exception as e:
            # Add upload stage information for debugging
            setattr(e, "_upload_stage", "shapefile_processing")
            raise

    async def _process_and_upload_geojson(
        self, context: CommandContext, geometry: Dict[str, Any], filename: str
    ) -> Tuple[str, Dict[str, Any], List[float]]:
        """
        Validate, normalize and upload a GeoJSON boundary

        Args:
            context: Command execution context
            geometry: The geometry or GeoJSON to process
            filename: Base filename for the GeoJSON (without extension)

        Returns:
            Tuple containing:
            - URL to the uploaded GeoJSON
            - Validated GeoJSON object
            - Bounding box coordinates [minx, miny, maxx, maxy]
        """
        logger.info(f"Processing and uploading GeoJSON: {filename}")

        # Convert to valid GeoJSON using existing utility
        valid_geojson = polygon_to_valid_geojson(geometry)

        # Save to storage via abstraction
        geojson_path = f"assets/{context.job_id}/boundaries/{filename}.geojson"

        geojson_bytes = json.dumps(valid_geojson.model_dump()).encode("utf-8")

        # Upload via storage interface
        geojson_url = await context.storage.save_bytes(
            data=geojson_bytes, path=geojson_path, temporary=False
        )

        # Also upload to GCS for backward compatibility
        gcs_blob_name = f"{context.fire_event_name}/{context.job_id}/{filename}.geojson"
        gcs_url = await upload_to_gcs(geojson_bytes, gcs_blob_name)

        # Extract bbox from geometry for STAC
        valid_geojson_dict = valid_geojson.model_dump()
        geom_shape = shape(valid_geojson_dict["features"][0]["geometry"])
        bbox = list(geom_shape.bounds)  # (minx, miny, maxx, maxy)

        logger.info(f"GeoJSON uploaded to storage: {geojson_url} and GCS: {gcs_url}")

        return geojson_url, valid_geojson_dict, bbox

    async def _create_boundary_stac_item(
        self,
        context: CommandContext,
        boundary_url: str,
        bbox: List[float],
        boundary_type: str,
    ) -> str:
        """Create STAC item for the uploaded boundary"""
        logger.info(f"Creating STAC boundary item for {boundary_type} boundary")

        try:
            from datetime import datetime

            datetime_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

            # Create STAC item via STAC manager
            await context.stac_manager.create_boundary_item(
                fire_event_name=context.fire_event_name,
                job_id=context.job_id,
                boundary_geojson_url=boundary_url,
                bbox=bbox,
                datetime_str=datetime_str,
                boundary_type=boundary_type,
            )

            # Return the boundary URL as the item identifier
            stac_item_url = (
                f"stac://{context.fire_event_name}-boundary-{context.job_id}"
            )
            logger.info(f"Created STAC boundary item: {stac_item_url}")
            return stac_item_url

        except Exception as e:
            # Add upload stage information for debugging
            setattr(e, "_upload_stage", "stac_creation")
            raise
