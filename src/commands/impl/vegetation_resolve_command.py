import json
import logging
import time
import warnings
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Tuple, Optional
import geopandas as gpd
from geojson_pydantic import Polygon as GeoJSONPolygon
import pandas as pd
import numpy as np
import xarray as xr
import xvec  # noqa: F401 - Required for xarray.xvec accessor

from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult
from src.config.vegetation_schema_loader import VegetationSchemaLoader
from src.config.vegetation_schemas import VegetationSchema, detect_vegetation_schema

logger = logging.getLogger(__name__)

# UTM Zone 11N for consistent projection (matching original logic)
PROJECTED_CRS = "EPSG:32611"


class VegetationResolveCommand(Command):
    """
    Command for resolving vegetation impact analysis against fire severity data.

    Migrates the business logic from src.routers.fire_recovery.py:process_veg_map_resolution
    and src.process.resolve_veg.py into the command pattern architecture, completely
    eliminating filesystem dependencies using storage abstraction.

    This command:
    - Downloads vegetation GPKG, fire COG, and boundary GeoJSON via storage.copy_from_url()
    - Performs zonal statistics analysis using xvec for vegetation impact assessment
    - Generates CSV and JSON reports in memory using BytesIO buffers
    - Saves results directly to storage without temporary files
    - Creates STAC metadata for discoverability
    - Ensures complete serverless compatibility with zero filesystem usage
    """

    def __init__(self) -> None:
        """Initialize the vegetation resolve command with schema loader."""
        self._schema_loader = VegetationSchemaLoader.get_instance()

    def get_command_name(self) -> str:
        return "vegetation_resolve"

    def get_estimated_duration_seconds(self) -> float:
        return 240.0  # 4 minutes for vegetation processing and zonal statistics

    def supports_retry(self) -> bool:
        return True

    def get_dependencies(self) -> List[str]:
        return []  # No command dependencies

    def get_required_permissions(self) -> List[str]:
        return ["stac:read", "stac:write", "storage:write", "computation:execute"]

    def validate_context(self, context: CommandContext) -> Tuple[bool, str]:
        """
        Validate that context contains all required data for vegetation analysis.

        Required context fields:
        - veg_gpkg_url: URL to vegetation geopackage
        - fire_cog_url: URL to fire severity COG
        - severity_breaks: Classification breaks for severity levels
        - geojson_url: URL to boundary GeoJSON

        Args:
            context: CommandContext to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Standard validation
        if not all([context.job_id, context.fire_event_name]):
            return False, "job_id and fire_event_name are required"

        if not all([context.storage, context.storage_factory, context.stac_manager]):
            return False, "storage, storage_factory, and stac_manager are required"

        # Vegetation analysis specific validation
        veg_gpkg_url = context.get_metadata("veg_gpkg_url")
        if not veg_gpkg_url:
            return False, "veg_gpkg_url is required in metadata"

        fire_cog_url = context.get_metadata("fire_cog_url")
        if not fire_cog_url:
            return False, "fire_cog_url is required in metadata"

        geojson_url = context.get_metadata("geojson_url")
        if not geojson_url:
            return False, "geojson_url is required in metadata"

        # Validate park unit ID if provided
        park_unit_id = context.get_metadata("park_unit_id")
        if park_unit_id and not self._schema_loader.has_schema(park_unit_id):
            available_parks = self._schema_loader.list_available_parks()
            return False, f"Unknown park unit '{park_unit_id}'. Available: {', '.join(available_parks)}"

        severity_breaks = context.severity_breaks
        if not severity_breaks or len(severity_breaks) < 3:
            return False, "severity_breaks must contain at least 3 values"

        return True, "Context validation passed"

    async def execute(self, context: CommandContext) -> CommandResult:
        """
        Execute vegetation impact analysis workflow.

        This method:
        1. Validates the context
        2. Downloads required files via storage abstraction (no temp files)
        3. Performs vegetation impact analysis using rasterio/rasterstats zonal statistics
        4. Generates CSV and JSON reports in memory
        5. Saves results directly to storage
        6. Creates STAC metadata for discoverability
        7. Returns success result with asset URLs

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

            logger.info(f"Starting vegetation impact analysis for job {context.job_id}")

            # Extract required URLs and parameters
            veg_gpkg_url = context.get_metadata("veg_gpkg_url")
            fire_cog_url = context.get_metadata("fire_cog_url")
            geojson_url = context.get_metadata("geojson_url")
            park_unit_id = context.get_metadata("park_unit_id")  # Optional park unit hint
            severity_breaks = context.severity_breaks

            # Type narrowing: we've already validated that severity_breaks is not None
            if severity_breaks is None:
                # This should never happen due to validation above, but helps mypy
                raise ValueError(
                    "severity_breaks is required but was None after validation"
                )

            # Mypy now understands severity_breaks is not None

            logger.info(
                f"Processing vegetation analysis - "
                f"GPKG: {veg_gpkg_url}, "
                f"COG: {fire_cog_url}, "
                f"GeoJSON: {geojson_url}, "
                f"Breaks: {severity_breaks}"
            )

            # Step 1: Download files via storage abstraction (zero filesystem usage)
            file_data = await self._download_input_files(
                context, veg_gpkg_url, fire_cog_url, geojson_url
            )

            # Step 2: Perform vegetation impact analysis
            result_df, json_structure = await self._analyze_vegetation_impact(
                file_data, severity_breaks, park_unit_id
            )

            # Step 3: Generate and save reports (memory-only processing)
            asset_urls = await self._save_analysis_reports(
                context, result_df, json_structure
            )

            # Step 4: Create STAC metadata for discoverability
            stac_item_url = await self._create_vegetation_stac_metadata(
                context, asset_urls, severity_breaks
            )

            execution_time_ms = (time.time() - start_time) * 1000

            # Prepare result data
            result_data = {
                "vegetation_types_analyzed": len(result_df),
                "total_area_hectares": float(result_df["total_ha"].sum()),
                "stac_item_url": stac_item_url,
                "severity_breaks": severity_breaks,
                "analysis_complete": True,
            }

            logger.info(
                f"Vegetation impact analysis completed successfully in {execution_time_ms:.2f}ms. "
                f"Analyzed {len(result_df)} vegetation types covering "
                f"{result_data['total_area_hectares']:.1f} hectares."
            )

            return CommandResult.success(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time_ms,
                data=result_data,
                asset_urls=asset_urls,
            )

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            logger.error(
                f"Vegetation impact analysis failed for job {context.job_id}: {str(e)}",
                exc_info=True,
            )

            return CommandResult.failure(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time_ms,
                error_message=f"Command execution failed: {str(e)}",
                error_details={"exception_type": type(e).__name__},
            )

    async def _download_input_files(
        self,
        context: CommandContext,
        veg_gpkg_url: str,
        fire_cog_url: str,
        geojson_url: str,
    ) -> Dict[str, bytes]:
        """
        Download input files via storage abstraction with zero filesystem usage.

        Uses storage.copy_from_url() to download files directly to storage, then
        retrieves as bytes for in-memory processing.

        Args:
            context: CommandContext for storage access
            veg_gpkg_url: URL to vegetation geopackage
            fire_cog_url: URL to fire severity COG
            geojson_url: URL to boundary GeoJSON

        Returns:
            Dictionary with file content as bytes
        """
        logger.info("Downloading input files via storage abstraction")

        # Validate URLs before attempting download
        urls_to_validate = {
            "vegetation GPKG": veg_gpkg_url,
            "fire severity COG": fire_cog_url,
            "boundary GeoJSON": geojson_url
        }
        
        for file_type, url in urls_to_validate.items():
            if not url or not isinstance(url, str) or len(url.strip()) == 0:
                raise ValueError(f"Invalid {file_type} URL: {url}")
            if not url.startswith(('http://', 'https://', 'gs://', 's3://')):
                raise ValueError(f"Unsupported {file_type} URL scheme: {url}")

        try:
            # Create temporary storage paths for downloads
            temp_veg_path = f"temp/{context.job_id}/vegetation.gpkg"
            temp_fire_path = f"temp/{context.job_id}/fire_severity.tif"
            temp_geojson_path = f"temp/{context.job_id}/boundary.geojson"

            # Downloads will happen sequentially below
            
            # Download files directly to storage (no filesystem temp files)
            try:
                await context.storage.copy_from_url(
                    veg_gpkg_url, temp_veg_path, temporary=True
                )
                logger.debug(f"Successfully downloaded vegetation GPKG from {veg_gpkg_url}")
            except Exception as e:
                raise ValueError(f"Failed to download vegetation GPKG from {veg_gpkg_url}: {str(e)}")
                
            try:
                await context.storage.copy_from_url(
                    fire_cog_url, temp_fire_path, temporary=True
                )
                logger.debug(f"Successfully downloaded fire COG from {fire_cog_url}")
            except Exception as e:
                raise ValueError(f"Failed to download fire COG from {fire_cog_url}: {str(e)}")
                
            try:
                await context.storage.copy_from_url(
                    geojson_url, temp_geojson_path, temporary=True
                )
                logger.debug(f"Successfully downloaded boundary GeoJSON from {geojson_url}")
            except Exception as e:
                raise ValueError(f"Failed to download boundary GeoJSON from {geojson_url}: {str(e)}")

            # Retrieve file contents as bytes for in-memory processing
            try:
                veg_data = await context.storage.get_bytes(temp_veg_path)
                if not veg_data or len(veg_data) == 0:
                    raise ValueError("Vegetation GPKG file is empty")
            except Exception as e:
                raise ValueError(f"Failed to retrieve vegetation GPKG data: {str(e)}")
                
            try:
                fire_data = await context.storage.get_bytes(temp_fire_path)
                if not fire_data or len(fire_data) == 0:
                    raise ValueError("Fire severity COG file is empty")
            except Exception as e:
                raise ValueError(f"Failed to retrieve fire COG data: {str(e)}")
                
            try:
                geojson_data = await context.storage.get_bytes(temp_geojson_path)
                if not geojson_data or len(geojson_data) == 0:
                    raise ValueError("Boundary GeoJSON file is empty")
            except Exception as e:
                raise ValueError(f"Failed to retrieve boundary GeoJSON data: {str(e)}")

            logger.info(
                f"Downloaded input files successfully - "
                f"GPKG: {len(veg_data)} bytes, "
                f"COG: {len(fire_data)} bytes, "
                f"GeoJSON: {len(geojson_data)} bytes"
            )

            return {
                "vegetation": veg_data,
                "fire_severity": fire_data,
                "boundary": geojson_data,
            }

        except ValueError:
            # Re-raise validation errors as-is
            raise
        except Exception as e:
            logger.error(f"Failed to download input files: {str(e)}", exc_info=True)
            raise ValueError(f"File download failed: {str(e)}")

    async def _analyze_vegetation_impact(
        self,
        file_data: Dict[str, bytes],
        severity_breaks: List[float],
        park_unit_id: Optional[str] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Perform vegetation impact analysis using memory-only processing.

        Migrated from src.process.resolve_veg.py:create_veg_fire_matrix but adapted
        for complete memory-only processing with BytesIO buffers.

        Args:
            context: CommandContext for logging
            file_data: File content as bytes
            severity_breaks: Severity classification breaks

        Returns:
            Tuple of (result DataFrame, JSON structure for visualization)
        """
        logger.info("Performing vegetation impact analysis")

        try:
            # Load fire data from bytes
            fire_ds, metadata = await self._load_fire_data_from_bytes(
                file_data["fire_severity"]
            )

            # Load vegetation data from bytes
            veg_gdf = await self._load_vegetation_data_from_bytes(
                file_data["vegetation"], metadata["crs"], park_unit_id
            )

            # Load boundary data from bytes
            boundary_gdf = await self._load_boundary_data_from_bytes(
                file_data["boundary"]
            )

            # Clip vegetation data to fire boundary
            fire_boundary_geom = boundary_gdf.geometry.union_all()
            buffer_distance = 100  # meters
            fire_boundary_buffered = fire_boundary_geom.buffer(buffer_distance)
            veg_gdf = gpd.clip(veg_gdf, fire_boundary_buffered)

            logger.info(f"Processing {len(veg_gdf)} vegetation polygons")

            # Create severity masks for analysis
            masks = await self._create_severity_masks(
                fire_ds[metadata["data_var"]], severity_breaks, boundary_gdf
            )

            # Initialize results DataFrame
            veg_types = veg_gdf["veg_type"].unique()
            
            # Validate that we have vegetation types to process
            if len(veg_types) == 0:
                logger.warning("No vegetation types found in the data")
                # Return empty results with proper structure
                empty_df = pd.DataFrame(columns=[
                    "unburned_ha", "low_ha", "moderate_ha", "high_ha", "total_ha",
                    "unburned_percent", "low_percent", "moderate_percent", "high_percent", "total_percent"
                ])
                empty_json: Dict[str, Any] = {"vegetation_communities": []}
                return empty_df, empty_json
                
            logger.info(f"Processing {len(veg_types)} unique vegetation types: {list(veg_types)}")
            
            severity_columns = [
                "unburned_ha",
                "low_ha", 
                "moderate_ha",
                "high_ha",
                "total_ha",
            ]
            result_df = pd.DataFrame(
                0.0, index=veg_types, columns=severity_columns, dtype=float
            )

            # Process each vegetation type
            for veg_type in veg_types:
                veg_subset = veg_gdf[veg_gdf["veg_type"] == veg_type]

                # Calculate zonal statistics
                stats = await self._calculate_zonal_statistics(
                    masks, veg_subset, metadata
                )

                # Update results
                total_pixels = stats.get("total_pixel_count", 0)
                result_df.loc[veg_type, "total_ha"] = (
                    total_pixels * metadata["pixel_area_ha"]
                )

                # Update severity-specific columns
                for key, value in stats.items():
                    if key in result_df.columns:
                        result_df.loc[veg_type, key] = float(value)

                # Add statistical measures
                for severity in ["unburned", "low", "moderate", "high"]:
                    result_df.loc[veg_type, f"{severity}_mean"] = stats.get(
                        f"{severity}_mean", 0
                    )
                    result_df.loc[veg_type, f"{severity}_std"] = stats.get(
                        f"{severity}_std", 0
                    )

                result_df.loc[veg_type, "mean_severity"] = stats.get("mean_severity", 0)
                result_df.loc[veg_type, "std_dev"] = stats.get("std_dev", 0)

            # Add percentage columns
            result_df = self._add_percentage_columns(result_df)

            # Create JSON structure for visualization
            json_structure = self._create_json_structure(result_df)

            logger.info(
                f"Analysis completed - {len(result_df)} vegetation types, "
                f"{result_df['total_ha'].sum():.1f} hectares total"
            )

            return result_df, json_structure

        except Exception as e:
            logger.error(f"Vegetation impact analysis failed: {str(e)}", exc_info=True)
            raise

    async def _load_fire_data_from_bytes(
        self, fire_data: bytes
    ) -> Tuple[xr.Dataset, Dict]:
        """Load fire severity data from bytes using BytesIO buffer with validation"""
        if not fire_data or len(fire_data) == 0:
            raise ValueError("Fire severity data is empty")
            
        fire_buffer = BytesIO(fire_data)
        fire_buffer.name = "fire_severity.tif"

        try:
            # Load as xarray dataset
            fire_ds = xr.open_dataset(fire_buffer, engine="rasterio")
            
            # Validate dataset
            if len(fire_ds.data_vars) == 0:
                raise ValueError("Fire severity dataset contains no data variables")
                
            # Extract main data variable
            data_var = list(fire_ds.data_vars)[0]
            data_array = fire_ds[data_var]
            
            # Validate data array
            if data_array.size == 0:
                raise ValueError("Fire severity data array is empty")
                
            # Check if data is completely NaN
            if np.isnan(data_array.values).all():
                logger.warning("Fire severity data contains only NaN values")

            # Validate CRS
            if fire_ds.rio.crs is None:
                logger.warning("Fire severity data has no CRS information, assuming WGS84")
                fire_ds = fire_ds.rio.write_crs("EPSG:4326")

            # Project to UTM if needed
            if fire_ds.rio.crs != PROJECTED_CRS:
                logger.info(f"Reprojecting fire data from {fire_ds.rio.crs} to {PROJECTED_CRS}")
                fire_ds = fire_ds.rio.reproject(PROJECTED_CRS)

            # Calculate pixel area from projected dataset
            projected_transform = fire_ds.rio.transform()
            pixel_width = abs(projected_transform[0])
            pixel_height = abs(projected_transform[4])
            
            if pixel_width <= 0 or pixel_height <= 0:
                raise ValueError(f"Invalid pixel dimensions: width={pixel_width}, height={pixel_height}")
                
            pixel_area_ha = (pixel_width * pixel_height) / 10000  # m² to ha

            # Determine coordinate names
            x_coord = "x" if "x" in fire_ds.coords else ("longitude" if "longitude" in fire_ds.coords else None)
            y_coord = "y" if "y" in fire_ds.coords else ("latitude" if "latitude" in fire_ds.coords else None)
            
            if not x_coord or not y_coord:
                raise ValueError(f"Cannot identify coordinate dimensions in fire data. Available coords: {list(fire_ds.coords.keys())}")

            metadata = {
                "crs": PROJECTED_CRS,
                "transform": fire_ds.rio.transform(),
                "pixel_area_ha": pixel_area_ha,
                "data_var": data_var,
                "x_coord": x_coord,
                "y_coord": y_coord,
                "width": data_array.sizes[x_coord],
                "height": data_array.sizes[y_coord],
            }
            
            logger.info(f"Successfully loaded fire data: {metadata['width']}x{metadata['height']} pixels, {pixel_area_ha:.6f} ha/pixel")
            return fire_ds, metadata
            
        except Exception as e:
            logger.error(f"Failed to load fire severity data: {str(e)}", exc_info=True)
            raise ValueError(f"Invalid fire severity data format: {str(e)}")

    async def _load_vegetation_data_from_bytes(
        self, veg_data: bytes, crs: str, park_unit_id: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        Load vegetation data from bytes using BytesIO buffer with configurable schema support.
        
        Uses the VegetationSchemaLoader to determine the appropriate schema based on the
        park_unit_id or auto-detection from the data structure.
        
        Args:
            veg_data: Vegetation geopackage data as bytes
            crs: Target CRS for reprojection
            park_unit_id: Optional park unit ID hint for schema selection
            
        Returns:
            GeoDataFrame with standardized vegetation type column
        """
        if not veg_data or len(veg_data) == 0:
            raise ValueError("Vegetation data is empty")
            
        veg_buffer = BytesIO(veg_data)
        veg_buffer.name = "vegetation.gpkg"

        # Suppress GPKG format warnings from GeoPandas/pyogrio
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=RuntimeWarning, message=".*GPKG.*")
            warnings.filterwarnings("ignore", category=UserWarning, message=".*GPKG.*")
            warnings.filterwarnings("ignore", message=".*non conformant file extension.*")
            
            gdf = None
            
            # Strategy 1: Try using specific park unit schema if provided
            if park_unit_id:
                try:
                    schema = self._schema_loader.get_schema(park_unit_id)
                    logger.info(f"Using configured schema for park unit: {park_unit_id}")
                    gdf = self._load_with_schema(veg_buffer, schema)
                except Exception as e:
                    logger.warning(f"Failed to load with park unit schema '{park_unit_id}': {str(e)}")
                    veg_buffer.seek(0)  # Reset buffer for next attempt
            
            # Strategy 2: Try auto-detection with all available schemas if park-specific loading failed
            if gdf is None:
                logger.info("Attempting auto-detection of vegetation schema")
                available_schemas = self._schema_loader.list_available_parks()
                for schema_id in available_schemas:
                    try:
                        schema = self._schema_loader.get_schema(schema_id)
                        logger.debug(f"Trying schema: {schema_id}")
                        gdf = self._load_with_schema(veg_buffer, schema)
                        logger.info(f"Successfully loaded vegetation data using {schema_id} schema with {len(gdf)} features")
                        break
                    except Exception as e:
                        logger.debug(f"Schema {schema_id} failed: {str(e)}")
                        veg_buffer.seek(0)  # Reset buffer for next attempt
            
            # Strategy 3: Fallback to auto-detection using existing logic
            if gdf is None:
                logger.info("Schema-based loading failed, attempting auto-detection")
                try:
                    # Load data first to analyze structure
                    temp_gdf = gpd.read_file(veg_buffer)
                    
                    # Use existing auto-detection logic
                    detected_schema = detect_vegetation_schema(temp_gdf, park_unit_id)
                    logger.info(f"Auto-detected schema: vegetation_type_field='{detected_schema.vegetation_type_field}'")
                    
                    # Apply the detected schema
                    gdf = temp_gdf.copy()
                    if detected_schema.vegetation_type_field in gdf.columns:
                        gdf["veg_type"] = gdf[detected_schema.vegetation_type_field]
                    else:
                        raise ValueError(f"Auto-detected field '{detected_schema.vegetation_type_field}' not found in data")
                    
                    logger.info(f"Successfully loaded vegetation data using auto-detection with {len(gdf)} features")
                    
                except Exception as e:
                    logger.error(f"All vegetation loading strategies failed: {str(e)}")
                    raise ValueError(f"Unable to load vegetation data: {str(e)}")

        # Validate that we have the required veg_type column
        if "veg_type" not in gdf.columns:
            raise ValueError("Vegetation data must contain vegetation type information after schema application")

        # Remove any rows with null vegetation types
        initial_count = len(gdf)
        gdf = gdf.dropna(subset=["veg_type"])
        if len(gdf) != initial_count:
            logger.warning(f"Removed {initial_count - len(gdf)} vegetation features with null veg_type")

        # Reproject to match fire data CRS
        if gdf.crs != crs:
            logger.info(f"Reprojecting vegetation data from {gdf.crs} to {crs}")
            gdf = gdf.to_crs(crs)

        return gdf

    def _load_with_schema(self, buffer: BytesIO, schema: VegetationSchema) -> gpd.GeoDataFrame:
        """
        Load vegetation data using a specific schema configuration.
        
        Args:
            buffer: BytesIO buffer containing vegetation data
            schema: VegetationSchema configuration to use
            
        Returns:
            GeoDataFrame with standardized vegetation type column
            
        Raises:
            Exception: If loading fails with the given schema
        """
        # Load the data with optional layer specification
        if schema.layer_name:
            gdf = gpd.read_file(buffer, layer=schema.layer_name)
            logger.debug(f"Loaded data from layer: {schema.layer_name}")
        else:
            gdf = gpd.read_file(buffer)
            logger.debug("Loaded data from default layer")
        
        # Validate that the required vegetation type field exists
        if schema.vegetation_type_field not in gdf.columns:
            available_cols = list(gdf.columns)
            raise ValueError(
                f"Required vegetation type field '{schema.vegetation_type_field}' not found. "
                f"Available columns: {available_cols}"
            )
        
        # Apply schema mapping to create standardized veg_type column
        gdf["veg_type"] = gdf[schema.vegetation_type_field]
        
        # Preserve additional fields if specified
        if schema.preserve_fields:
            for field in schema.preserve_fields:
                if field not in gdf.columns:
                    logger.warning(f"Preserve field '{field}' not found in data, skipping")
        
        logger.debug(f"Successfully applied schema with vegetation type field: {schema.vegetation_type_field}")
        return gdf

    async def _load_boundary_data_from_bytes(
        self, boundary_data: bytes
    ) -> gpd.GeoDataFrame:
        """Load boundary data from bytes using BytesIO buffer"""
        boundary_buffer = BytesIO(boundary_data)
        boundary_buffer.name = "boundary.geojson"

        # Suppress any file format warnings
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=RuntimeWarning)
            warnings.filterwarnings("ignore", category=UserWarning)
            
            boundary_gdf = gpd.read_file(boundary_buffer).to_crs(PROJECTED_CRS)
            logger.info(f"Successfully loaded boundary data with {len(boundary_gdf)} features")
            
        return boundary_gdf

    async def _create_severity_masks(
        self,
        fire_data: xr.DataArray,
        severity_breaks: List[float],
        boundary: gpd.GeoDataFrame,
    ) -> Dict[str, xr.DataArray]:
        """Create severity class masks for zonal analysis"""
        # Clip fire data to boundary
        fire_data = fire_data.rio.clip(
            boundary.geometry.apply(lambda geom: geom.__geo_interface__),
            boundary.crs,
            drop=True,
        )

        # Create severity class masks
        unburned_upper = severity_breaks[0]
        low_upper = severity_breaks[1]
        moderate_upper = severity_breaks[2]

        masks = {
            "unburned": fire_data.where(
                (fire_data >= -1) & (fire_data < unburned_upper), np.nan
            ),
            "low": fire_data.where(
                (fire_data >= unburned_upper) & (fire_data < low_upper), np.nan
            ),
            "moderate": fire_data.where(
                (fire_data >= low_upper) & (fire_data < moderate_upper), np.nan
            ),
            "high": fire_data.where(fire_data >= moderate_upper, np.nan),
            "original": fire_data,
        }

        return masks

    async def _calculate_zonal_statistics(
        self,
        masks: Dict[str, xr.DataArray],
        veg_subset: gpd.GeoDataFrame,
        metadata: Dict,
    ) -> Dict[str, float]:
        """Calculate zonal statistics for vegetation subset using xvec"""
        results = {}
        
        # Calculate statistics for each severity class
        severity_pixel_counts = {}
        severity_classes = ["unburned", "low", "moderate", "high"]

        for severity in severity_classes:
            mask_data = masks[severity]
            try:
                # Use xvec for zonal statistics
                # xvec.zonal_stats expects the geometries to be passed directly
                stats = mask_data.xvec.zonal_stats(
                    veg_subset.geometry,
                    stats=["count", "mean", "std"],
                    all_touched=True
                )
                
                # Extract statistics from result
                # The result is a DataArray with statistics for each geometry
                pixel_count = float(stats["count"].sum()) if "count" in stats else 0.0
                mean_val = float(stats["mean"].mean()) if "mean" in stats and not np.isnan(stats["mean"].mean()) else 0.0
                std_val = float(stats["std"].mean()) if "std" in stats and not np.isnan(stats["std"].mean()) else 0.0

                severity_pixel_counts[severity] = pixel_count
                results[f"{severity}_ha"] = pixel_count * metadata["pixel_area_ha"]
                results[f"{severity}_mean"] = mean_val
                results[f"{severity}_std"] = std_val

            except Exception as e:
                logger.warning(f"Error calculating {severity} stats: {str(e)}")
                severity_pixel_counts[severity] = 0.0
                results[f"{severity}_ha"] = 0.0
                results[f"{severity}_mean"] = 0.0
                results[f"{severity}_std"] = 0.0

        # Calculate totals
        results["total_pixel_count"] = sum(severity_pixel_counts.values())

        # Calculate overall statistics from original data
        try:
            original_data = masks["original"]
            
            # Calculate overall statistics using xvec
            overall_stats = original_data.xvec.zonal_stats(
                veg_subset.geometry,
                stats=["mean", "std"],
                all_touched=True
            )
            
            results["mean_severity"] = float(overall_stats["mean"].mean()) if "mean" in overall_stats and not np.isnan(overall_stats["mean"].mean()) else 0.0
            results["std_dev"] = float(overall_stats["std"].mean()) if "std" in overall_stats and not np.isnan(overall_stats["std"].mean()) else 0.0
                
        except Exception as e:
            logger.warning(f"Error calculating overall stats: {str(e)}")
            results["mean_severity"] = 0.0
            results["std_dev"] = 0.0

        return results

    def _add_percentage_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add percentage columns to results DataFrame with safe division"""
        # Calculate severity percentages within each vegetation type
        for severity in ["unburned", "low", "moderate", "high"]:
            # Safe division: handle zero/NaN denominators
            severity_ha = df[f"{severity}_ha"].fillna(0)
            total_ha = df["total_ha"].fillna(0)
            
            # Use numpy.where for safe division
            df[f"{severity}_percent"] = np.where(
                total_ha > 0,
                (severity_ha / total_ha * 100).round(2),
                0.0
            )

        # Add percentage of total area for each vegetation type
        total_study_area = df["total_ha"].sum()
        
        # Safe division for total percentages
        if total_study_area > 0 and not np.isnan(total_study_area):
            df["total_percent"] = (df["total_ha"].fillna(0) / total_study_area * 100).round(2)
        else:
            logger.warning("Total study area is zero or NaN, setting total_percent to 0")
            df["total_percent"] = 0.0

        # Ensure all percentage columns are numeric and handle any remaining NaN values
        percentage_columns = [f"{s}_percent" for s in ["unburned", "low", "moderate", "high"]] + ["total_percent"]
        for col in percentage_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        return df

    def _create_json_structure(self, result_df: pd.DataFrame) -> Dict[str, Any]:
        """Create JSON structure for frontend visualization with safe division"""
        total_park_area = result_df["total_ha"].sum()
        vegetation_communities = []

        for veg_type in result_df.index:
            row = result_df.loc[veg_type]
            color = "#" + format(hash(str(veg_type)) % 0xFFFFFF, "06x")

            # Safe division for percent_of_park
            if total_park_area > 0 and not np.isnan(total_park_area):
                percent_of_park = round((row["total_ha"] / total_park_area) * 100, 2)
            else:
                percent_of_park = 0.0

            community_data = {
                "name": str(veg_type),
                "color": color,
                "total_hectares": round(row["total_ha"], 2) if not np.isnan(row["total_ha"]) else 0.0,
                "percent_of_park": percent_of_park,
                "severity_breakdown": {},
            }

            # Build severity breakdown
            for severity in ["unburned", "low", "moderate", "high"]:
                if row[f"{severity}_ha"] > 0 and not np.isnan(row[f"{severity}_mean"]):
                    community_data["severity_breakdown"][severity] = {
                        "hectares": round(row[f"{severity}_ha"], 2),
                        "percent": round(row[f"{severity}_percent"], 2),
                        "mean_severity": round(row.get(f"{severity}_mean", 0), 3),
                        "std_dev": round(row.get(f"{severity}_std", 0), 3),
                    }

            vegetation_communities.append(community_data)

        return {"vegetation_communities": vegetation_communities}

    async def _save_analysis_reports(
        self,
        context: CommandContext,
        result_df: pd.DataFrame,
        json_structure: Dict[str, Any],
    ) -> Dict[str, str]:
        """
        Save analysis reports directly to storage using BytesIO buffers.

        Complete memory-only processing - no temporary files created.

        Args:
            context: CommandContext for storage access
            result_df: Results DataFrame for CSV export
            json_structure: JSON structure for visualization

        Returns:
            Dictionary of asset URLs
        """
        logger.info("Saving analysis reports via storage abstraction")

        try:
            # Generate CSV in memory
            csv_buffer = BytesIO()
            result_df.to_csv(
                csv_buffer, index=True, index_label="vegetation_classification"
            )
            csv_data = csv_buffer.getvalue()

            # Generate JSON in memory
            json_data = json.dumps(json_structure, indent=2).encode("utf-8")

            # Save CSV to storage
            csv_path = f"assets/{context.job_id}/vegetation/veg_fire_matrix.csv"
            csv_url = await context.storage.save_bytes(
                data=csv_data, path=csv_path, temporary=False
            )

            # Save JSON to storage
            json_path = f"assets/{context.job_id}/vegetation/veg_fire_matrix.json"
            json_url = await context.storage.save_bytes(
                data=json_data, path=json_path, temporary=False
            )

            logger.info(f"Saved reports - CSV: {csv_url}, JSON: {json_url}")

            return {
                "vegetation_matrix_csv": csv_url,
                "vegetation_matrix_json": json_url,
            }

        except Exception as e:
            logger.error(f"Failed to save analysis reports: {str(e)}", exc_info=True)
            raise

    async def _create_vegetation_stac_metadata(
        self,
        context: CommandContext,
        asset_urls: Dict[str, str],
        severity_breaks: List[float],
    ) -> str:
        """Create STAC metadata for vegetation analysis results"""
        logger.info("Creating STAC metadata for vegetation analysis")

        try:
            # Try multiple strategies to find the fire severity STAC item
            fire_stac_item = await self._find_fire_stac_item(context)

            if not fire_stac_item:
                # Fallback: create minimal STAC item using provided geometry
                logger.warning(f"Fire severity STAC item not found for job ID {context.job_id}, using fallback geometry")
                geometry, bbox, datetime_str = self._create_fallback_metadata(context)
            else:
                geometry = fire_stac_item["geometry"]
                bbox = fire_stac_item["bbox"]
                datetime_str = fire_stac_item["properties"]["datetime"]

            # Convert geometry dict to proper type if needed
            if isinstance(geometry, dict):
                # Convert dict to GeoJSONPolygon
                geojson_geometry = GeoJSONPolygon(**geometry)
            else:
                geojson_geometry = geometry

            # Create vegetation matrix STAC item
            await context.stac_manager.create_veg_matrix_item(
                fire_event_name=context.fire_event_name,
                job_id=context.job_id,
                fire_veg_matrix_csv_url=asset_urls["vegetation_matrix_csv"],
                fire_veg_matrix_json_url=asset_urls["vegetation_matrix_json"],
                geometry=geojson_geometry,
                bbox=bbox,
                classification_breaks=severity_breaks,
                datetime_str=datetime_str,
            )

            stac_item_url = (
                f"stac://{context.fire_event_name}-veg-matrix-{context.job_id}"
            )
            logger.info(f"Created vegetation STAC item: {stac_item_url}")
            return stac_item_url

        except Exception as e:
            logger.error(f"STAC metadata creation failed: {str(e)}", exc_info=True)
            raise
            
    async def _find_fire_stac_item(self, context: CommandContext) -> Optional[Dict]:
        """Try multiple strategies to find the fire severity STAC item"""
        stac_id_patterns = [
            f"{context.fire_event_name}-severity-{context.job_id}",  # Standard pattern
            f"{context.fire_event_name}-{context.job_id}",  # Alternative pattern
            f"severity-{context.job_id}",  # Simple pattern
        ]
        
        coarseness_levels = ["refined", "coarse", None]
        
        for stac_id in stac_id_patterns:
            for coarseness in coarseness_levels:
                try:
                    if coarseness:
                        fire_stac_item = await context.stac_manager.get_items_by_id_and_coarseness(
                            stac_id, coarseness
                        )
                    else:
                        # Try without coarseness filter
                        fire_stac_item = await context.stac_manager.get_item_by_id(stac_id)
                    
                    if fire_stac_item:
                        logger.info(f"Found fire STAC item with ID: {stac_id}, coarseness: {coarseness}")
                        return fire_stac_item
                        
                except Exception as e:
                    logger.debug(f"Failed to find STAC item {stac_id} with coarseness {coarseness}: {str(e)}")
                    continue
        
        logger.warning(f"No fire severity STAC item found for job {context.job_id}")
        return None
        
    def _create_fallback_metadata(self, context: CommandContext) -> Tuple[Dict, List[float], str]:
        """Create fallback metadata when STAC item is not found"""
        # Use the geometry from context, or create a minimal bounding box
        if hasattr(context.geometry, 'coordinates'):
            # Extract bounding box from geometry
            coords = context.geometry.coordinates[0] if context.geometry.coordinates else []
            if coords:
                lons = [c[0] for c in coords]
                lats = [c[1] for c in coords]
                bbox = [min(lons), min(lats), max(lons), max(lats)]
            else:
                bbox = [-180, -90, 180, 90]  # Global fallback
            
            geometry = {
                "type": context.geometry.type,
                "coordinates": context.geometry.coordinates
            }
        else:
            # Minimal fallback geometry
            bbox = [-180, -90, 180, 90]
            geometry = {
                "type": "Polygon",
                "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]]
            }
        
        # Use current datetime as fallback
        datetime_str = datetime.now(timezone.utc).isoformat() + "Z"
        
        logger.info(f"Created fallback metadata with bbox: {bbox}")
        return geometry, bbox, datetime_str
