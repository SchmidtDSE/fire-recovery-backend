import os
import tempfile
from typing import Dict, List, Any, Optional, Tuple

import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
import xarray as xr
import xvec
import httpx
from rasterio.transform import from_origin
from contextlib import contextmanager


@contextmanager
def temp_file(suffix: str = "", content: bytes = None):
    """Context manager for temporary files with automatic cleanup"""
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            if content:
                tmp.write(content)
            temp_path = tmp.name
        yield temp_path
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception as e:
                print(f"Failed to remove temporary file {temp_path}: {str(e)}")


async def download_file_to_temp(url: str, suffix: str = "") -> str:
    """Download a file to a temporary location"""
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        response.raise_for_status()

        with temp_file(suffix=suffix) as temp_path:
            with open(temp_path, "wb") as f:
                f.write(response.content)
            return temp_path


async def create_veg_fire_matrix(
    veg_gpkg_path: str,
    fire_cog_path: str,
    severity_breaks: List[float] = None,
) -> pd.DataFrame:
    """
    Create a matrix showing hectares of each vegetation type affected by different fire severity levels.

    Args:
        veg_gpkg_path: Path to the vegetation geopackage file
        fire_cog_path: Path to the fire severity COG file
        severity_breaks: List of breaks [low/moderate, moderate/high]

    Returns:
        DataFrame with vegetation types as rows and severity classes as columns
    """
    # Default severity breaks if none provided
    if severity_breaks is None:
        severity_breaks = [
            0.27,
            0.66,
        ]  # Default RBR breaks for low/moderate and moderate/high

    # 1. Load vegetation data
    gdf = gpd.read_file(veg_gpkg_path)

    # Make sure we have a vegetation type column
    if "MapUnit_ID" not in gdf.columns and "VEG_TYPE" not in gdf.columns:
        # Try to find a suitable column for vegetation type
        for col in gdf.columns:
            if col.lower() in [
                "vegetation",
                "vegtype",
                "veg_type",
                "veg",
                "type",
                "class",
            ]:
                gdf["veg_type"] = gdf[col]
                break
        else:
            # If no suitable column found, use the first string column
            for col in gdf.columns:
                if gdf[col].dtype == "object":
                    gdf["veg_type"] = gdf[col]
                    break
            else:
                raise ValueError(
                    "No suitable vegetation type column found in geopackage"
                )
    else:
        # Use existing column
        veg_type_col = "MapUnit_ID" if "MapUnit_ID" in gdf.columns else "VEG_TYPE"
        gdf["veg_type"] = gdf[veg_type_col]

    # 2. Load the fire COG with rasterio to get basic info
    with rasterio.open(fire_cog_path) as src:
        fire_data = src.read(1)
        crs = src.crs
        transform = src.transform

        # Calculate pixel area in hectares (assuming projection units are meters)
        pixel_width = abs(src.transform.a)
        pixel_height = abs(src.transform.e)
        pixel_area_ha = (pixel_width * pixel_height) / 10000  # Convert m² to ha

    # 3. Make sure vegetation data uses the same CRS as the fire data
    if gdf.crs != crs:
        gdf = gdf.to_crs(crs)

    # 4. Load the fire COG as xarray
    fire_ds = xr.open_dataset(fire_cog_path, engine="rasterio")

    # 5. Initialize result DataFrame
    veg_types = gdf["veg_type"].unique()
    result = pd.DataFrame(
        0,
        index=veg_types,
        columns=["unburned_ha", "low_ha", "moderate_ha", "high_ha", "total_ha"],
    )

    # 6. Process each vegetation type
    for veg_type in veg_types:
        # Filter to just this vegetation type
        veg_subset = gdf[gdf["veg_type"] == veg_type]

        # Calculate total area of this vegetation type in hectares
        total_area_ha = veg_subset.geometry.area.sum() / 10000  # Convert m² to ha
        result.loc[veg_type, "total_ha"] = total_area_ha

        # Create masks for each severity class
        # Use xvec's GeoAdapter for zonal statistics
        with xvec.GeoAdapter(veg_subset.geometry, crs=crs) as adapter:
            # Create masked arrays for each fire severity class
            unburned_mask = np.logical_and(fire_data >= -0.1, fire_data < 0.1).astype(
                np.float32
            )
            low_mask = np.logical_and(
                fire_data >= 0.1, fire_data < severity_breaks[0]
            ).astype(np.float32)
            moderate_mask = np.logical_and(
                fire_data >= severity_breaks[0], fire_data < severity_breaks[1]
            ).astype(np.float32)
            high_mask = (fire_data >= severity_breaks[1]).astype(np.float32)

            # Calculate statistics for each severity class
            unburned_stats = adapter.extract(unburned_mask, transform, stats=["sum"])
            low_stats = adapter.extract(low_mask, transform, stats=["sum"])
            moderate_stats = adapter.extract(moderate_mask, transform, stats=["sum"])
            high_stats = adapter.extract(high_mask, transform, stats=["sum"])

            # Calculate hectares for each severity class
            result.loc[veg_type, "unburned_ha"] = sum(
                s["sum"] * pixel_area_ha for s in unburned_stats if s["sum"] is not None
            )
            result.loc[veg_type, "low_ha"] = sum(
                s["sum"] * pixel_area_ha for s in low_stats if s["sum"] is not None
            )
            result.loc[veg_type, "moderate_ha"] = sum(
                s["sum"] * pixel_area_ha for s in moderate_stats if s["sum"] is not None
            )
            result.loc[veg_type, "high_ha"] = sum(
                s["sum"] * pixel_area_ha for s in high_stats if s["sum"] is not None
            )

    # Add percentage columns
    for severity in ["unburned", "low", "moderate", "high"]:
        result[f"{severity}_percent"] = (
            result[f"{severity}_ha"] / result["total_ha"] * 100
        ).round(2)

    return result


async def process_veg_map(
    veg_gpkg_url: str,
    fire_cog_url: str,
    output_dir: str,
    job_id: str,
    severity_breaks: List[float] = None,
) -> Dict[str, Any]:
    """
    Process vegetation map against fire severity COG

    Args:
        veg_gpkg_url: URL to vegetation geopackage
        fire_cog_url: URL to fire severity COG
        output_dir: Directory to save output CSV
        job_id: Unique job identifier
        severity_breaks: Optional custom breaks for severity classification

    Returns:
        Dict with status and path to output CSV
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        output_csv = os.path.join(output_dir, f"{job_id}_veg_fire_matrix.csv")

        # Download input files
        veg_gpkg_path = await download_file_to_temp(veg_gpkg_url, suffix=".gpkg")
        fire_cog_path = await download_file_to_temp(fire_cog_url, suffix=".tif")

        # Process the vegetation map against fire severity
        result_df = await create_veg_fire_matrix(
            veg_gpkg_path=veg_gpkg_path,
            fire_cog_path=fire_cog_path,
            severity_breaks=severity_breaks,
        )

        # Save the result to CSV
        result_df.to_csv(output_csv)

        return {"status": "completed", "output_csv": output_csv}

    except Exception as e:
        print(f"Error processing vegetation map: {str(e)}")
        return {"status": "error", "error_message": str(e)}
