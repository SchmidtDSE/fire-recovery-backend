import xarray as xr
import stackstac
import numpy as np
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles
import os
from typing import List, Dict, Optional, Any
from geojson_pydantic import Polygon, Feature
from shapely.geometry import shape
from src.stac.stac_endpoint_handler import StacEndpointHandler

RUN_LOCAL = os.getenv("RUN_LOCAL") == "True"


async def process_remote_sensing_data(
    job_id: str,
    geometry: Polygon | Feature,
    stac_endpoint_handler: StacEndpointHandler,
    prefire_date_range: Optional[List[str]],
    postfire_date_range: Optional[List[str]],
) -> Dict[str, Any]:
    # Initialize workspace
    workspace = initialize_workspace(job_id)
    output_dir = workspace["output_dir"]
    status_file = workspace["status_file"]

    try:
        # Validate input date ranges
        if not prefire_date_range or not postfire_date_range:
            raise ValueError("Both prefire and postfire date ranges are required")

        # Calculate the full date range for a single query
        full_date_range = [prefire_date_range[0], postfire_date_range[1]]

        # Fetch all data using the endpoint handler
        items, endpoint_config = await stac_endpoint_handler.search_items(
            geometry=geometry,
            date_range=full_date_range,
            collections=["sentinel-2-l2a"],
        )

        # Get the band names and EPSG code for this endpoint
        nir_band, swir_band = stac_endpoint_handler.get_band_names(endpoint_config)
        epsg_code = stac_endpoint_handler.get_epsg_code(endpoint_config)

        # Fetch data using these parameters
        stacked_data = stackstac.stack(
            items,
            epsg=epsg_code,
            assets=[swir_band, nir_band],
            bounds=get_buffered_bounds(geometry, 100),
            chunksize=(-1, 1, 512, 512),
        )

        # Split into pre and post fire datasets
        prefire_data = subset_data_by_date_range(stacked_data, prefire_date_range)
        postfire_data = subset_data_by_date_range(stacked_data, postfire_date_range)

        print(f"Prefire shape: {prefire_data.shape}, dims: {prefire_data.dims}")
        print(f"Postfire shape: {postfire_data.shape}, dims: {postfire_data.dims}")

        # Calculate burn indices
        indices = calculate_burn_indices(
            prefire_data, postfire_data, nir_band, swir_band
        )

        # Create COGs for each metric
        cog_results = {}
        for name, data in indices.items():
            cog_path = f"{output_dir}/{name}.tif"
            cog_results[name] = create_cog(data, cog_path)

        # Update status
        all_valid = all(result["is_valid"] for result in cog_results.values())
        with open(status_file, "w") as f:
            f.write("completed" if all_valid else "failed_validation")

        return {
            "status": "completed" if all_valid else "failed_validation",
            "output_files": {
                name: result["path"] for name, result in cog_results.items()
            },
        }

    except Exception as e:
        # Update status on error
        with open(status_file, "w") as f:
            f.write(f"error: {str(e)}")
        return {"status": f"error: {str(e)}"}


def subset_data_by_date_range(
    stacked_data: xr.DataArray, date_range: List[str]
) -> xr.DataArray:
    """
    Subset stacked data by date range.

    Args:
        stacked_data: The stacked DataArray with a time dimension
        date_range: List of [start_date, end_date] as strings in ISO format

    Returns:
        Subset of the stacked data within the specified date range
    """
    start_date, end_date = date_range

    # Convert string dates to numpy datetime64
    start = np.datetime64(start_date)
    end = np.datetime64(end_date)

    # Subset data by time
    return stacked_data.sel(time=slice(start, end))


def initialize_workspace(job_id: str) -> Dict[str, str]:
    """Initialize the workspace and return paths to working files"""
    output_dir = f"tmp/{job_id}"
    os.makedirs(output_dir, exist_ok=True)

    status_file = f"{output_dir}/status.txt"
    with open(status_file, "w") as f:
        f.write("started")

    return {"output_dir": output_dir, "status_file": status_file}


def get_buffered_bounds(
    geometry: Polygon | Feature, buffer: float
) -> tuple[float, float, float, float]:
    # Extract the bounding box from the geometry
    # Convert pydantic object to dict for shapely
    if hasattr(geometry, 'model_dump'):
        geom_dict = geometry.model_dump()
        # If it's a Feature, extract the geometry part
        if geom_dict.get("type") == "Feature":
            geom_shape = shape(geom_dict["geometry"])
        else:
            geom_shape = shape(geom_dict)
    else:
        geom_shape = shape(geometry)
    minx, miny, maxx, maxy = geom_shape.bounds

    # Calculate width and height in degrees
    width = maxx - minx
    height = maxy - miny

    # Calculate buffer size in degrees (20% of width/height or .25 degree, whichever is smaller)
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


def calculate_nbr(
    data: xr.DataArray, nir_band_name: str, swir_band_name: str
) -> xr.DataArray:
    """Calculate Normalized Burn Ratio for a dataset"""
    nir_band = data.sel(band=nir_band_name)
    nir = nir_band.median(dim="time")

    swir_band = data.sel(band=swir_band_name)
    swir = swir_band.median(dim="time")

    return (nir - swir) / (nir + swir)


# @coiled.function(
#     name="calculate-burn-indices",
#     container="ghcr.io/schmidtdse/fire-coiled-runner:latest",
#     memory="4 GiB",
#     cpu=4,
#     n_workers=[0, 20],  # Autoscale between 1 and 20 workers
#     keepalive="6 hours",
#     local=False,
# )
# @cache(
#     key_builder=burn_indices_key_builder,
#     namespace="burn_indices",
#     expire=60 * 60 * 24 * 7,  # Cache for 7 days
# )
def calculate_burn_indices(
    prefire_data: xr.DataArray,
    postfire_data: xr.DataArray,
    nir_band_name: str,
    swir_band_name: str,
) -> Dict[str, xr.DataArray]:
    """Calculate various burn indices from pre and post fire data"""

    # Calculate NBR for both periods
    prefire_nbr = calculate_nbr(prefire_data, nir_band_name, swir_band_name)
    postfire_nbr = calculate_nbr(postfire_data, nir_band_name, swir_band_name)
    prefire_nbr, postfire_nbr = xr.align(prefire_nbr, postfire_nbr)

    # Calculate dNBR
    dnbr = prefire_nbr - postfire_nbr

    # Calculate RdNBR
    abs_sqrt_prefire_nbr = abs(prefire_nbr) ** 0.5
    # Avoid division by zero
    abs_sqrt_prefire_nbr = abs_sqrt_prefire_nbr.where(abs_sqrt_prefire_nbr != 0, 0.001)
    rdnbr = dnbr / abs_sqrt_prefire_nbr

    # Calculate RBR
    denominator = prefire_nbr + 1.001
    rbr = dnbr / denominator

    # Compute so that Coiled can distribute
    rbr.compute()
    rdnbr.compute()
    dnbr.compute()
    prefire_nbr.compute()
    postfire_nbr.compute()

    result = {
        "prefire_nbr": prefire_nbr,
        "postfire_nbr": postfire_nbr,
        "dnbr": dnbr,
        "rdnbr": rdnbr,
        "rbr": rbr,
    }

    # Write to cache
    # write_to_cache(cache_key=cache_key, result=result)

    return result


def create_cog(data: xr.DataArray, output_path: str) -> Dict[str, Any]:
    """Create a Cloud Optimized GeoTIFF from xarray data"""

    naive_tiff = output_path.replace(".tif", "_raw.tif")

    # Dask arrays are lazy - this is where dask-distributed will
    # actually compute the burn metrics from the various href'd COGs
    computed = data.compute()

    # Ensure data is float32 and has proper nodata value
    computed = computed.astype("float32")

    # Set nodata value for NaN values
    nodata = -9999.0
    computed = computed.rio.write_nodata(nodata)
    computed.rio.set_crs("EPSG:4326", inplace=True)

    # Write the naive GeoTIFF
    computed.rio.to_raster(naive_tiff, driver="GTiff", dtype="float32")

    cog_profile = cog_profiles.get("deflate")
    # Update the profile to include nodata value
    cog_profile.update(dtype="float32", nodata=nodata)

    cog_translate(
        naive_tiff,
        output_path,
        cog_profile,
        add_mask=True,
        overview_resampling="average",
        forward_band_tags=True,
        use_cog_driver=True,
    )

    is_valid, __errors, __warnings = cog_validate(output_path)

    # Clean up intermediate naive file
    os.remove(naive_tiff)

    return {"path": output_path, "is_valid": is_valid}
