import io
import json
import logging
import time
from typing import Any, Dict, List, Tuple

import geopandas as gpd
from fastapi import UploadFile
from geojson_pydantic import Feature, FeatureCollection
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry import MultiPolygon as ShapelyMultiPolygon
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

    def validate_context(self, context: CommandContext) -> Tuple[bool, str]:
        """Validate that context has required data for AOI upload"""
        if not context.job_id or not context.fire_event_name:
            error_msg = "job_id and fire_event_name are required"
            logger.error(error_msg)
            return False, error_msg

        if not context.storage:
            error_msg = "storage interface is required"
            logger.error(error_msg)
            return False, error_msg

        if not context.stac_manager:
            error_msg = "stac_manager is required"
            logger.error(error_msg)
            return False, error_msg

        # Check that either geometry or upload_data is provided
        upload_data = context.get_metadata("upload_data")
        upload_type = context.get_metadata("upload_type", "geojson")

        if upload_type == "geojson" and not context.geometry:
            error_msg = "geometry is required for GeoJSON uploads"
            logger.error(error_msg)
            return False, error_msg

        if upload_type == "shapefile" and not upload_data:
            error_msg = "upload_data is required for shapefile uploads"
            logger.error(error_msg)
            return False, error_msg

        return True, "Context validation passed"

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
            # Get boundary_type from metadata (defaults to "coarse" for backward compatibility)
            boundary_type = context.get_metadata("boundary_type", "coarse")

            # Validate boundary_type
            if boundary_type not in ["coarse", "refined"]:
                raise ValueError(
                    f"Invalid boundary_type '{boundary_type}'. Must be 'coarse' or 'refined'"
                )

            # Assert geometry is not None (validated in validate_context for geojson uploads)
            assert context.geometry is not None, "geometry required for GeoJSON upload"

            # Get geometry from context and convert to dict format for processing
            geometry_obj = context.geometry
            if hasattr(geometry_obj, "model_dump"):
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

            # Process and upload the GeoJSON with dynamic filename
            filename = f"{boundary_type}_boundary"
            geojson_url, valid_geojson, bbox = await self._process_and_upload_geojson(
                context=context, geometry=geojson_dict, filename=filename
            )

            # Create STAC boundary item with correct boundary_type
            stac_item_url = await self._create_boundary_stac_item(
                context=context,
                boundary_url=geojson_url,
                bbox=bbox,
                boundary_type=boundary_type,
            )

            return {
                "upload_type": "geojson",
                "status": "complete",
                "boundary_geojson_url": geojson_url,
                "stac_item_url": stac_item_url,
                "bbox": bbox,
                "boundary_type": boundary_type,
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
        """
        Process Shapefile zip upload - extract geometry and save both zip and GeoJSON.

        This method:
        1. Validates the zip file
        2. Saves the original .zip to storage
        3. Extracts geometry from shapefile in-memory
        4. Converts to GeoJSON and uploads
        5. Creates STAC boundary item

        Args:
            context: Command execution context
            upload_file: The uploaded .zip file

        Returns:
            Dict with shapefile_url, boundary_geojson_url, stac_item_url, etc.
        """
        logger.info("Processing Shapefile upload")

        try:
            # Get boundary_type from metadata (defaults to "refined")
            boundary_type = context.get_metadata("boundary_type", "refined")

            # Validate boundary_type
            if boundary_type not in ["coarse", "refined"]:
                raise ValueError(
                    f"Invalid boundary_type '{boundary_type}'. Must be 'coarse' or 'refined'"
                )

            # Validate file extension
            if not upload_file.filename or not upload_file.filename.lower().endswith(
                ".zip"
            ):
                raise ValueError("Only zipped shapefiles (.zip) are supported")

            # Read file content
            content = await upload_file.read()
            logger.info(f"Read {len(content)} bytes from shapefile upload")

            # Step 1: Upload zip file to storage (for archival purposes)
            zip_path = f"assets/{context.job_id}/uploads/original_shapefile.zip"
            shapefile_url = await context.storage.save_bytes(
                data=content, path=zip_path, temporary=False
            )
            logger.info(f"Shapefile zip uploaded to: {shapefile_url}")

            # Step 2: Extract geometry from shapefile in-memory
            try:
                geometry_dict = self._extract_geometry_from_shapefile_zip(content)
                logger.info("Successfully extracted geometry from shapefile")
            except Exception as e:
                logger.error(f"Failed to extract geometry from shapefile: {str(e)}")
                raise ValueError(f"Invalid shapefile: {str(e)}")

            # Step 3: Process and upload the extracted GeoJSON
            filename = f"{boundary_type}_boundary"
            geojson_url, valid_geojson, bbox = await self._process_and_upload_geojson(
                context=context, geometry=geometry_dict, filename=filename
            )
            logger.info(f"Boundary GeoJSON uploaded to: {geojson_url}")

            # Step 4: Create STAC boundary item
            stac_item_url = await self._create_boundary_stac_item(
                context=context,
                boundary_url=geojson_url,
                bbox=bbox,
                boundary_type=boundary_type,
            )
            logger.info(f"STAC item created: {stac_item_url}")

            # Step 5: Create GCS fallback upload for backward compatibility
            zip_blob_name = (
                f"{context.fire_event_name}/{context.job_id}/original_shapefile.zip"
            )
            gcs_url = await upload_to_gcs(
                content, zip_blob_name, context.storage_factory
            )

            return {
                "upload_type": "shapefile",
                "status": "complete",
                "shapefile_url": shapefile_url,
                "boundary_geojson_url": geojson_url,
                "stac_item_url": stac_item_url,
                "gcs_shapefile_url": gcs_url,
                "boundary_type": boundary_type,
                "filename": upload_file.filename,
                "file_size_bytes": len(content),
                "bbox": bbox,
                "asset_urls": {
                    "shapefile": shapefile_url,
                    "gcs_shapefile": gcs_url,
                    "boundary_geojson": geojson_url,
                    "stac_item": stac_item_url,
                },
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
        gcs_url = await upload_to_gcs(
            geojson_bytes, gcs_blob_name, context.storage_factory
        )

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

    def _extract_geometry_from_shapefile_zip(self, zip_bytes: bytes) -> Dict[str, Any]:
        """
        Extract GeoJSON geometry from a zipped shapefile using in-memory processing.

        This method uses GeoPandas with pyogrio backend to read the shapefile
        directly from memory without requiring filesystem access. It's fully
        serverless-compatible.

        Args:
            zip_bytes: Bytes of the zipped shapefile

        Returns:
            GeoJSON dict with geometry (either Polygon or MultiPolygon)

        Raises:
            ValueError: If shapefile is invalid, empty, or contains unsupported geometry
        """
        logger.info("Extracting geometry from shapefile zip")

        try:
            # Wrap bytes in BytesIO for in-memory processing
            zip_buffer = io.BytesIO(zip_bytes)

            # Read shapefile using GeoPandas (uses pyogrio backend)
            # GeoPandas automatically handles /vsizip/ GDAL virtual filesystem
            gdf = gpd.read_file(zip_buffer)

            logger.info(f"Read {len(gdf)} features from shapefile")

            # Validate we have features
            if len(gdf) == 0:
                raise ValueError("Shapefile contains no features")

            # Validate geometry types
            geom_types = gdf.geometry.geom_type.unique()
            logger.info(f"Geometry types found: {list(geom_types)}")

            valid_types = {"Polygon", "MultiPolygon"}
            invalid_types = set(geom_types) - valid_types
            if invalid_types:
                raise ValueError(
                    f"Shapefile contains unsupported geometry types: {invalid_types}. "
                    f"Only Polygon and MultiPolygon are supported for fire boundaries."
                )

            # Dissolve all features into a single geometry
            # This is standard for fire boundary shapefiles which may have
            # multiple polygons representing different burned areas
            logger.info("Dissolving features into single geometry")
            dissolved = gdf.dissolve()

            if len(dissolved) != 1:
                raise ValueError(
                    f"Expected single geometry after dissolve, got {len(dissolved)}"
                )

            # Extract the geometry
            geometry = dissolved.iloc[0].geometry

            # Validate it's a Polygon or MultiPolygon
            if not isinstance(geometry, (ShapelyPolygon, ShapelyMultiPolygon)):
                raise ValueError(
                    f"Expected Polygon or MultiPolygon, got {type(geometry).__name__}"
                )

            # Convert to GeoJSON dict using shapely's __geo_interface__
            geojson_dict = geometry.__geo_interface__

            logger.info(
                f"Extracted {geojson_dict['type']} geometry with "
                f"{len(geojson_dict.get('coordinates', []))} coordinate rings"
            )

            return geojson_dict

        except Exception as e:
            # Enhance error messages for common issues
            error_msg = str(e)
            if "No such file or directory" in error_msg:
                raise ValueError(
                    "Invalid zip file or no .shp file found in zip. "
                    "Ensure the zip contains all shapefile components (.shp, .dbf, .shx, .prj)"
                )
            elif "Cannot open" in error_msg or "not recognized as being in a supported file format" in error_msg:
                raise ValueError(
                    "Invalid zip file or no .shp file found in zip. "
                    "Ensure the zip contains all shapefile components (.shp, .dbf, .shx, .prj)"
                )
            else:
                raise ValueError(f"Failed to extract geometry from shapefile: {error_msg}")
