import logging
import time
from typing import Dict, Any, List, Union
import xarray as xr
import stackstac
import numpy as np
from shapely.geometry import shape
from geojson_pydantic import Polygon

from src.commands.interfaces.command import Command
from src.commands.interfaces.command_context import CommandContext
from src.commands.interfaces.command_result import CommandResult
from src.core.storage.interface import StorageInterface
from src.stac.stac_endpoint_handler import StacEndpointHandler
from src.util.cog_ops import create_cog_bytes

logger = logging.getLogger(__name__)


class FireSeverityAnalysisCommand(Command):
    """
    Command for analyzing fire severity using spectral indices.

    Migrates the business logic from src.process.spectral_indices.py into
    the command pattern architecture, removing tempfile dependencies and
    integrating with the storage abstraction layer.

    This command:
    - Fetches pre and post-fire satellite data via STAC
    - Calculates burn indices using the strategy pattern
    - Creates COG outputs via storage abstraction
    - Updates STAC metadata for discoverability
    """

    def get_command_name(self) -> str:
        return "fire_severity_analysis"

    def get_estimated_duration_seconds(self) -> float:
        return 300.0  # 5 minutes for satellite data processing

    def supports_retry(self) -> bool:
        return True

    def get_dependencies(self) -> List[str]:
        return []  # No command dependencies

    def get_required_permissions(self) -> List[str]:
        return ["stac:read", "storage:write", "computation:execute"]

    def validate_context(self, context: CommandContext) -> bool:
        """Validate that context has required data for fire severity analysis"""
        if not context.job_id or not context.fire_event_name:
            logger.error("job_id and fire_event_name are required")
            return False

        if not context.geometry:
            logger.error("geometry is required for spatial analysis")
            return False

        if not context.storage:
            logger.error("storage interface is required")
            return False

        if not context.index_registry:
            logger.error("index_registry is required for burn index calculations")
            return False

        # Check for required date ranges in computation config
        prefire_dates = context.get_computation_config("prefire_date_range")
        postfire_dates = context.get_computation_config("postfire_date_range")

        if not prefire_dates or not postfire_dates:
            logger.error(
                "prefire_date_range and postfire_date_range are required in computation_config"
            )
            return False

        return True

    async def execute(self, context: CommandContext) -> CommandResult:
        """Execute fire severity analysis workflow"""
        start_time = time.time()

        logger.info(
            f"Starting fire severity analysis for job {context.job_id}, "
            f"fire event: {context.fire_event_name}"
        )

        try:
            # Extract configuration
            prefire_date_range = context.get_computation_config("prefire_date_range")
            postfire_date_range = context.get_computation_config("postfire_date_range")
            collection = context.get_computation_config("collection", "sentinel-2-l2a")
            buffer_meters = context.get_computation_config("buffer_meters", 100)
            indices = context.get_computation_config(
                "indices", ["nbr", "dnbr", "rdnbr", "rbr"]
            )

            logger.info(
                f"Configuration - Prefire: {prefire_date_range}, "
                f"Postfire: {postfire_date_range}, Collection: {collection}, "
                f"Buffer: {buffer_meters}m, Indices: {indices}"
            )

            # Step 1: Fetch satellite data via STAC
            stac_data = await self._fetch_satellite_data(
                context,
                prefire_date_range,
                postfire_date_range,
                collection,
                buffer_meters,
            )

            # Step 2: Calculate burn indices using strategy pattern
            index_results = await self._calculate_burn_indices(
                context, stac_data, indices
            )

            # Step 3: Save results as COGs via storage abstraction
            asset_urls = await self._save_results_as_cogs(context, index_results)

            # Step 4: Update STAC metadata
            stac_item_url = await self._create_stac_metadata(
                context, asset_urls, prefire_date_range, postfire_date_range
            )

            execution_time = (time.time() - start_time) * 1000

            logger.info(
                f"Fire severity analysis completed for job {context.job_id} "
                f"in {execution_time:.2f}ms. Generated {len(asset_urls)} assets."
            )

            return CommandResult.success(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time,
                data={
                    "indices_calculated": list(index_results.keys()),
                    "stac_item_url": stac_item_url,
                    "analysis_complete": True,
                },
                asset_urls=asset_urls,
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(
                f"Fire severity analysis failed for job {context.job_id}: {str(e)}",
                exc_info=True,
            )

            return CommandResult.failure(
                job_id=context.job_id,
                fire_event_name=context.fire_event_name,
                command_name=self.get_command_name(),
                execution_time_ms=execution_time,
                error_message=str(e),
                error_details={
                    "error_type": type(e).__name__,
                    "stage": getattr(e, "_analysis_stage", "unknown"),
                },
            )

    async def _fetch_satellite_data(
        self,
        context: CommandContext,
        prefire_date_range: List[str],
        postfire_date_range: List[str],
        collection: str,
        buffer_meters: float,
    ) -> Dict[str, xr.DataArray]:
        """Fetch satellite data for pre and post-fire periods"""
        logger.info("Fetching satellite data via STAC")

        try:
            # Create STAC endpoint handler
            stac_handler = StacEndpointHandler()

            # Calculate full date range for single query
            full_date_range = [prefire_date_range[0], postfire_date_range[1]]

            # Search for items
            items, endpoint_config = await stac_handler.search_items(
                geometry=context.geometry,
                date_range=full_date_range,
                collections=[collection],
            )

            if not items:
                raise ValueError(
                    f"No satellite data found for date range {full_date_range}"
                )

            logger.info(f"Found {len(items)} STAC items for analysis")

            # Get band configuration
            nir_band, swir_band = stac_handler.get_band_names(endpoint_config)
            epsg_code = stac_handler.get_epsg_code(endpoint_config)

            logger.info(
                f"Using bands - NIR: {nir_band}, SWIR: {swir_band}, EPSG: {epsg_code}"
            )

            # Calculate buffered bounds
            buffered_bounds = self._get_buffered_bounds(context.geometry, buffer_meters)

            # Stack data using stackstac
            stacked_data = stackstac.stack(
                items,
                epsg=epsg_code,
                assets=[swir_band, nir_band],
                bounds=buffered_bounds,
                chunksize=(-1, 1, 512, 512),
            )

            # Split into pre and post fire datasets
            prefire_data = self._subset_data_by_date_range(
                stacked_data, prefire_date_range
            )
            postfire_data = self._subset_data_by_date_range(
                stacked_data, postfire_date_range
            )

            logger.info(
                f"Data shapes - Prefire: {prefire_data.shape}, Postfire: {postfire_data.shape}"
            )

            return {
                "prefire_data": prefire_data,
                "postfire_data": postfire_data,
                "nir_band": nir_band,
                "swir_band": swir_band,
            }

        except Exception as e:
            e._analysis_stage = "data_fetch"
            raise

    async def _calculate_burn_indices(
        self,
        context: CommandContext,
        stac_data: Dict[str, xr.DataArray],
        requested_indices: List[str],
    ) -> Dict[str, xr.DataArray]:
        """Calculate burn indices using the strategy pattern"""
        logger.info(f"Calculating burn indices: {requested_indices}")

        try:
            prefire_data = stac_data["prefire_data"]
            postfire_data = stac_data["postfire_data"]
            nir_band = stac_data["nir_band"]
            swir_band = stac_data["swir_band"]

            # Prepare context for index calculators
            calc_context = {
                "nir_band_name": nir_band,
                "swir_band_name": swir_band,
                "job_id": context.job_id,
                "geometry": context.geometry,
            }

            index_results = {}

            # Calculate each requested index using strategy pattern
            for index_name in requested_indices:
                calculator = context.index_registry.get_calculator(index_name)
                if calculator is None:
                    logger.warning(
                        f"Calculator for index '{index_name}' not found, skipping"
                    )
                    continue

                logger.info(f"Calculating index: {index_name}")

                # Calculate the index using the strategy
                index_result = await calculator.calculate(
                    prefire_data=prefire_data,
                    postfire_data=postfire_data,
                    context=calc_context,
                )

                # Compute to trigger dask evaluation
                computed_result = index_result.compute()
                index_results[index_name] = computed_result

                logger.info(f"Completed calculation for index: {index_name}")

            if not index_results:
                raise ValueError("No valid indices were calculated")

            logger.info(f"Successfully calculated {len(index_results)} indices")
            return index_results

        except Exception as e:
            e._analysis_stage = "index_calculation"
            raise

    async def _save_results_as_cogs(
        self, context: CommandContext, index_results: Dict[str, xr.DataArray]
    ) -> Dict[str, str]:
        """Save index results as Cloud Optimized GeoTIFFs via storage abstraction"""
        logger.info("Saving results as COGs via storage abstraction")

        try:
            asset_urls = {}

            for index_name, data in index_results.items():
                logger.info(f"Creating COG for index: {index_name}")

                # Ensure data is properly formatted
                processed_data = self._prepare_data_for_cog(data)

                # Save as COG through storage abstraction
                cog_path = f"{context.job_id}/fire_severity/{index_name}.tif"

                # Convert xarray to bytes for storage (simplified approach)
                # In production, this would use proper COG creation
                asset_url = await self._create_and_save_cog(
                    context.storage, processed_data, cog_path
                )

                asset_urls[index_name] = asset_url
                logger.info(f"Saved COG for {index_name} to {asset_url}")

            return asset_urls

        except Exception as e:
            e._analysis_stage = "cog_creation"
            raise

    async def _create_stac_metadata(
        self,
        context: CommandContext,
        asset_urls: Dict[str, str],
        prefire_date_range: List[str],
        postfire_date_range: List[str],
    ) -> str:
        """Create STAC metadata for the fire severity analysis results"""
        logger.info("Creating STAC metadata for fire severity analysis")

        try:
            # Prepare STAC item data
            stac_item_data = {
                "job_id": context.job_id,
                "fire_event_name": context.fire_event_name,
                "analysis_type": "fire_severity",
                "prefire_date_range": prefire_date_range,
                "postfire_date_range": postfire_date_range,
                "asset_urls": asset_urls,
                "geometry": context.geometry,
                "indices": list(asset_urls.keys()),
            }

            # Create STAC item via STAC manager
            stac_item_url = await context.stac_manager.create_fire_severity_item(
                stac_item_data
            )

            logger.info(f"Created STAC item: {stac_item_url}")
            return stac_item_url

        except Exception as e:
            e._analysis_stage = "stac_creation"
            raise

    def _get_buffered_bounds(
        self, geometry: Union[Polygon, Dict[str, Any]], buffer_meters: float
    ) -> tuple:
        """Calculate buffered bounds for the geometry (migrated from original code)"""
        # Handle both Polygon objects and dict representations
        if isinstance(geometry, dict):
            geom_shape = shape(geometry)
        else:
            geom_shape = shape(geometry.model_dump())

        minx, miny, maxx, maxy = geom_shape.bounds

        # Calculate width and height in degrees
        width = maxx - minx
        height = maxy - miny

        # Calculate buffer size in degrees (20% of width/height or 0.25 degree, whichever is smaller)
        buffer_x = min(width * 0.2, 0.25)
        buffer_y = min(height * 0.2, 0.25)

        # Create buffered bounds
        buffered_bounds = (
            minx - buffer_x,  # min_x
            miny - buffer_y,  # min_y
            maxx + buffer_x,  # max_x
            maxy + buffer_y,  # max_y
        )

        return buffered_bounds

    def _subset_data_by_date_range(
        self, stacked_data: xr.DataArray, date_range: List[str]
    ) -> xr.DataArray:
        """Subset stacked data by date range (migrated from original code)"""
        start_date, end_date = date_range

        # Convert string dates to numpy datetime64
        start = np.datetime64(start_date)
        end = np.datetime64(end_date)

        # Subset data by time
        return stacked_data.sel(time=slice(start, end))

    def _prepare_data_for_cog(self, data: xr.DataArray) -> xr.DataArray:
        """Prepare xarray data for COG creation (migrated from original code)"""
        # Ensure data is float32 and has proper nodata value
        processed = data.astype("float32")

        # Set nodata value for NaN values
        nodata = -9999.0
        processed = processed.rio.write_nodata(nodata)

        # Ensure CRS is set
        if not processed.rio.crs:
            processed.rio.set_crs("EPSG:4326", inplace=True)

        return processed

    async def _create_and_save_cog(
        self, storage: StorageInterface, data: xr.DataArray, cog_path: str
    ) -> str:
        """Create and save COG via storage abstraction"""
        # Use the proper COG creation utility
        cog_data = await create_cog_bytes(data)

        # Save via storage interface
        asset_url = await storage.save_bytes(
            data=cog_data, path=cog_path, temporary=False
        )

        return asset_url
