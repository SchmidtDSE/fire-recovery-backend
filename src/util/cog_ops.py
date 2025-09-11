import os
import tempfile
from typing import Any, Dict, cast

import httpx
import rioxarray
import xarray as xr
from rio_cogeo.cogeo import cog_translate, cog_validate
from rio_cogeo.profiles import cog_profiles
from shapely.geometry import shape

from src.stac.stac_json_manager import STACJSONManager


async def get_fire_severity_cog_by_event(
    stac_manager: STACJSONManager, fire_event_name: str
) -> str:
    """
    Find the most recent fire severity COG for a given fire event.

    Args:
        stac_manager: STAC manager instance to search for items
        fire_event_name: Name of the fire event

    Returns:
        URL to the fire severity COG

    Raises:
        Exception: If no fire severity COG is found
    """
    # Search for fire severity items for this event
    stac_items = await stac_manager.search_items(
        fire_event_name=fire_event_name, product_type="fire-severity"
    )

    if not stac_items or len(stac_items) == 0:
        raise Exception(f"No fire severity data found for {fire_event_name}")

    # Use the most recent item
    severity_item = stac_items[0]
    cog_url = severity_item["assets"]["rbr"]["href"]

    return cog_url


async def download_cog_to_temp(cog_url: str) -> str:
    """
    Download a COG to a temporary file.

    Args:
        cog_url: URL to the COG

    Returns:
        Path to the downloaded COG temporary file
    """
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    temp_path = temp_file.name
    temp_file.close()

    # Download the COG
    async with httpx.AsyncClient() as client:
        response = await client.get(cog_url)
        if response.status_code != 200:
            os.unlink(temp_path)
            raise Exception(f"Failed to download COG: {response.status_code}")

        with open(temp_path, "wb") as f:
            f.write(response.content)

    return temp_path


def crop_cog_with_geometry(cog_path: str, geometry: Dict[str, Any]) -> xr.DataArray:
    """
    Crop a COG using a GeoJSON geometry.

    Args:
        cog_path: Path to the COG file
        geometry: GeoJSON geometry to use for cropping

    Returns:
        Cropped xarray DataArray
    """
    # Convert geometry to shapely object
    if "type" in geometry and geometry["type"] == "Feature":
        geom = shape(geometry["geometry"])
    elif (
        "type" in geometry
        and geometry["type"] == "FeatureCollection"
        and "features" in geometry
    ):
        geom = shape(geometry["features"][0]["geometry"])
    else:
        geom = shape(geometry)

    # Open the COG with rioxarray
    data = cast(xr.DataArray, rioxarray.open_rasterio(cog_path))

    # Crop the data with the geometry
    # Note: mask accepts a list of geometries
    cropped = data.rio.clip([geom], drop=True, all_touched=True)

    return cropped


async def create_cog_bytes(data: xr.DataArray) -> bytes:
    """
    Create a Cloud Optimized GeoTIFF from xarray data and return as bytes.

    Args:
        data: The xarray DataArray to convert to a COG

    Returns:
        COG data as bytes
    """

    # Compute the data (if it's a dask array)
    if hasattr(data, "compute"):
        computed = data.compute()
    else:
        computed = data

    # Ensure data is float32 and has proper nodata value
    computed = computed.astype("float32")

    # Set nodata value for NaN values
    nodata = -9999.0
    computed = computed.rio.write_nodata(nodata)

    # Make sure CRS is preserved
    if computed.rio.crs is None:
        computed.rio.set_crs("EPSG:4326", inplace=True)

    # Use local temporary files for COG processing
    with tempfile.NamedTemporaryFile(suffix="_raw.tif", delete=False) as naive_temp:
        naive_tiff_path = naive_temp.name
    
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as output_temp:
        output_path = output_temp.name
    
    try:
        # Write the naive GeoTIFF to temp file
        computed.rio.to_raster(naive_tiff_path, driver="GTiff", dtype="float32")

        # Configure and create the COG
        cog_profile = cog_profiles.get("deflate")
        cog_profile.update(dtype="float32", nodata=nodata)

        cog_translate(
            naive_tiff_path,
            output_path,
            cog_profile,
            add_mask=True,
            overview_resampling="average",
            forward_band_tags=True,
            use_cog_driver=True,
        )

        # Validate the COG
        is_valid, __errors, __warnings = cog_validate(output_path)

        if not is_valid:
            raise Exception("Failed to create a valid COG")

        # Read the COG as bytes
        with open(output_path, "rb") as f:
            cog_bytes = f.read()
            
    finally:
        # Clean up temporary files
        for temp_path in [naive_tiff_path, output_path]:
            if os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass  # Ignore cleanup errors
    
    return cog_bytes
