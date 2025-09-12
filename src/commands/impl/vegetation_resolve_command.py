import json
import logging
import time
from io import BytesIO
from typing import Any, Dict, List, Tuple
import geopandas as gpd
import pandas as pd
import numpy as np
import xarray as xr

from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult

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
        3. Performs vegetation impact analysis using xvec zonal statistics
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
                file_data, severity_breaks
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

        try:
            # Create temporary storage paths for downloads
            temp_veg_path = f"temp/{context.job_id}/vegetation.gpkg"
            temp_fire_path = f"temp/{context.job_id}/fire_severity.tif"
            temp_geojson_path = f"temp/{context.job_id}/boundary.geojson"

            # Download files directly to storage (no filesystem temp files)
            await context.storage.copy_from_url(
                veg_gpkg_url, temp_veg_path, temporary=True
            )
            await context.storage.copy_from_url(
                fire_cog_url, temp_fire_path, temporary=True
            )
            await context.storage.copy_from_url(
                geojson_url, temp_geojson_path, temporary=True
            )

            # Retrieve file contents as bytes for in-memory processing
            veg_data = await context.storage.get_bytes(temp_veg_path)
            fire_data = await context.storage.get_bytes(temp_fire_path)
            geojson_data = await context.storage.get_bytes(temp_geojson_path)

            logger.info(
                f"Downloaded input files - "
                f"GPKG: {len(veg_data)} bytes, "
                f"COG: {len(fire_data)} bytes, "
                f"GeoJSON: {len(geojson_data)} bytes"
            )

            return {
                "vegetation": veg_data,
                "fire_severity": fire_data,
                "boundary": geojson_data,
            }

        except Exception as e:
            logger.error(f"Failed to download input files: {str(e)}", exc_info=True)
            raise

    async def _analyze_vegetation_impact(
        self,
        file_data: Dict[str, bytes],
        severity_breaks: List[float],
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
                file_data["vegetation"], metadata["crs"]
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
        """Load fire severity data from bytes using BytesIO buffer"""
        fire_buffer = BytesIO(fire_data)
        fire_buffer.name = "fire_severity.tif"

        # Load as xarray dataset
        fire_ds = xr.open_dataset(fire_buffer, engine="rasterio")

        # Extract main data variable
        data_var = list(fire_ds.data_vars)[0]

        # Project to UTM if needed
        if fire_ds.rio.crs != PROJECTED_CRS:
            fire_ds = fire_ds.rio.reproject(PROJECTED_CRS)

        # Calculate pixel area from projected dataset
        projected_transform = fire_ds.rio.transform()
        pixel_width = abs(projected_transform[0])
        pixel_height = abs(projected_transform[4])
        pixel_area_ha = (pixel_width * pixel_height) / 10000  # m² to ha

        metadata = {
            "crs": PROJECTED_CRS,
            "transform": fire_ds.rio.transform(),
            "pixel_area_ha": pixel_area_ha,
            "data_var": data_var,
            "x_coord": "x" if "x" in fire_ds.coords else "longitude",
            "y_coord": "y" if "y" in fire_ds.coords else "latitude",
        }

        return fire_ds, metadata

    async def _load_vegetation_data_from_bytes(
        self, veg_data: bytes, crs: str
    ) -> gpd.GeoDataFrame:
        """Load vegetation data from bytes using BytesIO buffer"""
        veg_buffer = BytesIO(veg_data)
        veg_buffer.name = "vegetation.gpkg"

        # Detect data source and load appropriate layer
        # TODO: This hardcoding should be made configurable in the future
        try:
            # Try JOTR format first
            gdf = gpd.read_file(veg_buffer, layer="JOTR_VegPolys")
            gdf["veg_type"] = gdf["MapUnit_Name"]
        except Exception:
            try:
                # Try MOJN format
                gdf = gpd.read_file(veg_buffer)
                gdf["veg_type"] = gdf["MAP_DESC"]
            except Exception as e:
                logger.error(f"Failed to load vegetation data: {str(e)}")
                raise ValueError(f"Unsupported vegetation data format: {str(e)}")

        # Reproject to match fire data CRS
        if gdf.crs != crs:
            gdf = gdf.to_crs(crs)

        return gdf

    async def _load_boundary_data_from_bytes(
        self, boundary_data: bytes
    ) -> gpd.GeoDataFrame:
        """Load boundary data from bytes using BytesIO buffer"""
        boundary_buffer = BytesIO(boundary_data)
        boundary_buffer.name = "boundary.geojson"

        boundary_gdf = gpd.read_file(boundary_buffer).to_crs(PROJECTED_CRS)
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
        """Calculate zonal statistics for vegetation subset"""
        results = {}

        # Consolidate geometries into single polygon
        unified_geometry = gpd.GeoDataFrame(
            {"geometry": [veg_subset.geometry.union_all()]}, crs=PROJECTED_CRS
        )

        # Calculate statistics for each severity class
        severity_pixel_counts = {}
        severity_classes = ["unburned", "low", "moderate", "high"]

        for severity in severity_classes:
            mask = masks[severity]
            try:
                stats = mask.xvec.zonal_stats(
                    unified_geometry.geometry,
                    x_coords=metadata["x_coord"],
                    y_coords=metadata["y_coord"],
                    stats=["count", "mean", "stdev"],
                    all_touched=True,
                    method="exactextract",
                )

                if stats is not None and not np.isnan(stats.values).all():
                    pixel_count = float(stats.isel(zonal_statistics=0).values)
                    severity_pixel_counts[severity] = pixel_count
                    results[f"{severity}_ha"] = pixel_count * metadata["pixel_area_ha"]
                    results[f"{severity}_mean"] = float(
                        stats.isel(zonal_statistics=1).values
                    )
                    results[f"{severity}_std"] = float(
                        stats.isel(zonal_statistics=2).values
                    )
                else:
                    severity_pixel_counts[severity] = 0.0
                    results[f"{severity}_ha"] = 0.0
                    results[f"{severity}_mean"] = 0.0
                    results[f"{severity}_std"] = 0.0

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
            original_mask = masks["original"]
            stats = original_mask.xvec.zonal_stats(
                unified_geometry.geometry,
                x_coords=metadata["x_coord"],
                y_coords=metadata["y_coord"],
                stats=["mean", "std"],
                all_touched=True,
            )

            if stats is not None and not np.isnan(stats.values).all():
                results["mean_severity"] = float(stats.isel(zonal_statistics=0).values)
                results["std_dev"] = float(stats.isel(zonal_statistics=1).values)
            else:
                results["mean_severity"] = 0.0
                results["std_dev"] = 0.0
        except Exception as e:
            logger.warning(f"Error calculating overall stats: {str(e)}")
            results["mean_severity"] = 0.0
            results["std_dev"] = 0.0

        return results

    def _add_percentage_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add percentage columns to results DataFrame"""
        # Calculate severity percentages within each vegetation type
        for severity in ["unburned", "low", "moderate", "high"]:
            df[f"{severity}_percent"] = (
                df[f"{severity}_ha"] / df["total_ha"] * 100
            ).round(2)

        # Add percentage of total area for each vegetation type
        total_study_area = df["total_ha"].sum()
        df["total_percent"] = (df["total_ha"] / total_study_area * 100).round(2)

        return df

    def _create_json_structure(self, result_df: pd.DataFrame) -> Dict[str, Any]:
        """Create JSON structure for frontend visualization"""
        total_park_area = result_df["total_ha"].sum()
        vegetation_communities = []

        for veg_type in result_df.index:
            row = result_df.loc[veg_type]
            color = "#" + format(hash(str(veg_type)) % 0xFFFFFF, "06x")

            community_data = {
                "name": str(veg_type),
                "color": color,
                "total_hectares": round(row["total_ha"], 2),
                "percent_of_park": round((row["total_ha"] / total_park_area) * 100, 2),
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
            # Get geometry from fire severity STAC item
            stac_id = f"{context.fire_event_name}-severity-{context.job_id}"
            fire_stac_item = await context.stac_manager.get_items_by_id_and_coarseness(
                stac_id, "refined"
            )

            if not fire_stac_item:
                raise ValueError(
                    f"Fire severity STAC item not found for job ID {context.job_id}"
                )

            geometry = fire_stac_item["geometry"]
            bbox = fire_stac_item["bbox"]
            datetime_str = fire_stac_item["properties"]["datetime"]

            # Create vegetation matrix STAC item
            await context.stac_manager.create_veg_matrix_item(
                fire_event_name=context.fire_event_name,
                job_id=context.job_id,
                fire_veg_matrix_csv_url=asset_urls["vegetation_matrix_csv"],
                fire_veg_matrix_json_url=asset_urls["vegetation_matrix_json"],
                geometry=geometry,
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
