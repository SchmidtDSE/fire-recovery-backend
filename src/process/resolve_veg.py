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
import os
import tempfile
from typing import Dict, List, Any, Optional, Tuple
import hashlib
import pickle

PROJECTED_CRS = "EPSG:32611"  # UTM 11N


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

        fd, temp_path = tempfile.mkstemp(suffix=suffix)
        os.close(fd)

        with open(temp_path, "wb") as f:
            f.write(response.content)

        # We'll rely on the calling function to clean up this file
        return temp_path


def load_vegetation_data(
    veg_gpkg_path: str, original_url: str = None, crs=None
) -> gpd.GeoDataFrame:
    """
    Load vegetation data from geopackage and ensure correct CRS

    Args:
        veg_gpkg_path: Path to the vegetation geopackage file
        original_url: Original URL of the geopackage for identifying data source
        crs: Optional CRS to reproject vegetation data to

    Returns:
        GeoDataFrame with vegetation data
    """
    # TODO: Hardcoded for JOTR geopackage for now

    # Use the original URL for comparisons if provided, otherwise use path
    url_to_check = original_url if original_url else veg_gpkg_path

    # JOTR
    if (
        url_to_check
        == "https://storage.googleapis.com/national_park_service/joshua_tree/jotr_gpkg/jotrgeodata.gpkg"
    ):
        gdf = gpd.read_file(veg_gpkg_path, layer="JOTR_VegPolys")
        gdf["veg_type"] = gdf["MapUnit_Name"]

    # MOJN
    if (
        url_to_check
        == "https://storage.googleapis.com/national_park_service/mock_assets_frontend/MN_Geo/MOJN.gpkg"
    ):
        gdf = gpd.read_file(veg_gpkg_path)
        gdf["veg_type"] = gdf["MAP_DESC"]

    # Ensure vegetation data uses the same CRS as the fire data if provided
    if crs and gdf.crs != crs:
        gdf = gdf.to_crs(crs)

    return gdf


def load_fire_data(fire_cog_path: str) -> Tuple[xr.Dataset, Dict]:
    """
    Load fire severity data and extract key information

    Args:
        fire_cog_path: Path to the fire severity COG file

    Returns:
        Tuple of (xarray dataset, dict with metadata)
    """
    # Get basic metadata with rasterio
    with rasterio.open(fire_cog_path) as src:
        crs = src.crs
        transform = src.transform

        # Calculate pixel area in hectares (assuming projection units are meters)
        pixel_width = abs(src.transform.a)
        pixel_height = abs(src.transform.e)
        pixel_area_ha = (pixel_width * pixel_height) / 10000  # Convert m² to ha

    # Load as xarray dataset for analysis
    fire_ds = xr.open_dataset(fire_cog_path, engine="rasterio")

    # Extract the main data variable
    data_var = list(fire_ds.data_vars)[0]

    metadata = {
        "crs": crs,
        "transform": transform,
        "pixel_area_ha": pixel_area_ha,
        "data_var": data_var,
        "x_coord": "x" if "x" in fire_ds.coords else "longitude",
        "y_coord": "y" if "y" in fire_ds.coords else "latitude",
    }

    return fire_ds, metadata


def create_severity_masks(
    fire_ds: xr.Dataset, data_var: str, severity_breaks: List[float]
) -> Dict[str, xr.DataArray]:
    """
    Create masks for different fire severity classes

    Args:
        fire_ds: Fire severity dataset
        data_var: Name of the data variable in the dataset
        severity_breaks: List of breaks [low/moderate, moderate/high]

    Returns:
        Dictionary of masks for each severity class
    """
    fire_data = fire_ds[data_var]

    masks = {
        "unburned": fire_data.where((fire_data >= -0.1) & (fire_data < 0.1), 0),
        "low": fire_data.where(
            (fire_data >= 0.1) & (fire_data < severity_breaks[0]), 0
        ),
        "moderate": fire_data.where(
            (fire_data >= severity_breaks[0]) & (fire_data < severity_breaks[1]), 0
        ),
        "high": fire_data.where(fire_data >= severity_breaks[1], 0),
    }

    # Convert to projected
    for severity, mask in masks.items():
        if mask.rio.crs != PROJECTED_CRS:
            masks[severity] = mask.rio.reproject(PROJECTED_CRS)

    return masks


def calculate_zonal_stats(
    masks: Dict[str, xr.DataArray],
    veg_subset: gpd.GeoDataFrame,
    x_coord: str,
    y_coord: str,
    pixel_area_ha: float,
) -> Dict[str, float]:
    """
    Calculate zonal statistics for a vegetation subset
    """
    results = {}

    # Ensure that the geometry is in the same CRS as the masks
    assert veg_subset.crs.to_string() == PROJECTED_CRS, (
        "Vegetation subset CRS does not match masks CRS"
    )

    # Consolidate geometries for this MapUnit_ID into a single geometry
    print(f"Consolidating {len(veg_subset)} geometries for MapUnit_ID")
    unified_geometry = gpd.GeoDataFrame(
        {"geometry": [veg_subset.unary_union]}, crs=PROJECTED_CRS
    )

    # First calculate area statistics for each severity class
    for severity, mask in masks.items():
        if severity == "original":
            continue  # Skip original for area calculations

        try:
            # Ensure the mask is in the correct CRS
            assert mask.rio.crs.to_string() == PROJECTED_CRS, (
                f"Mask for {severity} does not match projected CRS"
            )

            # Perform zonal statistics on the unified geometry
            stats = mask.xvec.zonal_stats(
                unified_geometry.geometry,
                x_coords=x_coord,
                y_coords=y_coord,
                stats="sum",
                all_touched=True,
            )

            # Extract numeric value from DataArray and handle NaNs
            if stats is not None and not np.isnan(stats.values).all():
                sum_value = float(stats.sum(skipna=True).values)
            else:
                sum_value = 0.0

            # Calculate hectares
            results[f"{severity}_ha"] = sum_value * pixel_area_ha

        except Exception as e:
            print(f"Error calculating {severity} stats: {str(e)}")
            results[f"{severity}_ha"] = 0.0

    # Now calculate mean and std dev from the original fire data
    try:
        original_mask = masks["original"]
        stats = original_mask.xvec.zonal_stats(
            unified_geometry.geometry,
            x_coords=x_coord,
            y_coords=y_coord,
            stats=["mean", "std"],
            all_touched=True,
        )

        if stats is not None and not np.isnan(stats.values).all():
            results["mean_severity"] = float(
                stats.isel(zonal_statistics=0).values
            )  # mean
            results["std_dev"] = float(stats.isel(zonal_statistics=1).values)  # std
        else:
            results["mean_severity"] = 0.0
            results["std_dev"] = 0.0
    except Exception as e:
        print(f"Error calculating mean/std stats: {str(e)}")
        results["mean_severity"] = 0.0
        results["std_dev"] = 0.0

    return results


def add_percentage_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add percentage columns to the results dataframe

    Args:
        df: DataFrame with area calculations

    Returns:
        DataFrame with added percentage columns
    """
    for severity in ["unburned", "low", "moderate", "high"]:
        df[f"{severity}_percent"] = (df[f"{severity}_ha"] / df["total_ha"] * 100).round(
            2
        )

    return df


def generate_cache_key(veg_gpkg_path: str, fire_ds: xr.Dataset) -> str:
    """Generate a unique cache key based on vegetation path and fire dataset"""
    # Use the veg_gpkg_path as part of the key
    veg_key = veg_gpkg_path.split("/")[-1]

    # Create a hash of the fire dataset
    fire_data = fire_ds[list(fire_ds.data_vars)[0]]
    # Convert a sample of the data to string for hashing
    # Using just a slice to avoid hashing the whole dataset
    fire_sample = str(fire_data[0:8, 0:8].values)
    fire_hash = hashlib.md5(fire_sample.encode()).hexdigest()

    return f"veg_fire_matrix:{veg_key}:{fire_hash}"


# def attempt_read_from_cache(cache_key: str):
#     """Try to read cached result from disk"""
#     # Create cache directory if it doesn't exist
#     cache_dir = "tmp/cache"
#     os.makedirs(cache_dir, exist_ok=True)

#     cache_file = f"{cache_dir}/{cache_key}.pkl"

#     # Check if cached result exists
#     if os.path.exists(cache_file):
#         try:
#             print(f"Loading cached result from {cache_file}")
#             with open(cache_file, "rb") as f:
#                 return pickle.loads(f.read())
#         except Exception as e:
#             print(f"Error loading cached result: {e}")

#     return None


# def write_to_cache(cache_key: str, result):
#     """Write result to cache"""
#     # Create cache directory if it doesn't exist
#     cache_dir = "tmp/cache"
#     os.makedirs(cache_dir, exist_ok=True)

#     cache_file = f"{cache_dir}/{cache_key}.pkl"

#     try:
#         print(f"Saving result to cache: {cache_file}")
#         with open(cache_file, "wb") as f:
#             f.write(
#                 pickle.dumps(result, protocol=-1)
#             )  # Use highest protocol for efficiency
#     except Exception as e:
#         print(f"Error caching result: {e}")


async def create_veg_fire_matrix(
    original_veg_gpkg_url: str,
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

    # Load fire data and get metadata
    fire_ds, metadata = load_fire_data(fire_cog_path)

    # Generate cache key
    # cache_key = generate_cache_key(original_veg_gpkg_url, fire_ds)

    # # Try to load from cache first
    # cached_result = attempt_read_from_cache(cache_key)
    # if cached_result is not None:
    #     print(f"Using cached vegetation fire matrix result")
    #     return cached_result

    # print(f"No cache found. Computing vegetation fire matrix...")

    # Extract original fire data for mean severity calculations
    fire_data = fire_ds[metadata["data_var"]]
    if fire_data.rio.crs != PROJECTED_CRS:
        fire_data = fire_data.rio.reproject(PROJECTED_CRS)

    # Load vegetation data
    gdf = load_vegetation_data(
        veg_gpkg_path, original_url=original_veg_gpkg_url, crs=metadata["crs"]
    )
    gdf = gdf.to_crs(PROJECTED_CRS)

    # Create severity masks
    masks = create_severity_masks(fire_ds, metadata["data_var"], severity_breaks)

    # Add original fire data to masks for mean calculation
    masks["original"] = fire_data

    # Initialize result DataFrame with float dtype to avoid warnings
    veg_types = gdf["veg_type"].unique()
    result = pd.DataFrame(
        0.0,
        index=veg_types,
        columns=["unburned_ha", "low_ha", "moderate_ha", "high_ha", "total_ha"],
        dtype=float,
    )

    # Get total park area for percentage calculations
    total_park_area = gdf.geometry.area.sum() / 10000  # Convert m² to ha

    # Update the result with calculated statistics
    for veg_type in veg_types:
        # Filter to just this vegetation type
        veg_subset = gdf[gdf["veg_type"] == veg_type]

        # Calculate total area of this vegetation type in hectares
        total_area_ha = float(veg_subset.geometry.area.sum() / 10000)
        result.loc[veg_type, "total_ha"] = total_area_ha

        # Calculate zonal statistics for each severity class
        stats = calculate_zonal_stats(
            masks,
            veg_subset,
            metadata["x_coord"],
            metadata["y_coord"],
            metadata["pixel_area_ha"],
        )

        # Update result with calculated statistics
        for key, value in stats.items():
            if key in result.columns:
                result.loc[veg_type, key] = float(value)

        # Add mean and std dev directly from zonal stats
        result.loc[veg_type, "mean_severity"] = stats.get("mean_severity", 0)
        result.loc[veg_type, "std_dev"] = stats.get("std_dev", 0)

    # Add percentage columns
    result = add_percentage_columns(result)

    frontend_df = pd.DataFrame(
        {
            "Color": [
                "#" + format(hash(str(veg)) % 0xFFFFFF, "06x") for veg in result.index
            ],
            "Vegetation Community": result.index,
            "Hectares": result["total_ha"].round(2),
            "% of Park": ((result["total_ha"] / total_park_area) * 100).round(2),
            "% of Burn Area": (
                (result["low_ha"] + result["moderate_ha"] + result["high_ha"])
                / (
                    result["low_ha"].sum()
                    + result["moderate_ha"].sum()
                    + result["high_ha"].sum()
                )
                * 100
            ).round(2),
            "Mean Severity": result["mean_severity"].round(3),
            "Std Dev": result["std_dev"].round(3),
        }
    )

    # Cache the result before returning
    # write_to_cache(cache_key, frontend_df)

    return frontend_df


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
            original_veg_gpkg_url=veg_gpkg_url,
            veg_gpkg_path=veg_gpkg_path,
            fire_cog_path=fire_cog_path,
            severity_breaks=severity_breaks,
        )

        # Save the result to CSV (already formatted for frontend)
        result_df.to_csv(output_csv, index=False)

        # Clean up temporary files
        for path in [veg_gpkg_path, fire_cog_path]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    print(f"Failed to remove temporary file {path}: {str(e)}")

        return {"status": "completed", "output_csv": output_csv}

    except Exception as e:
        print(f"Error processing vegetation map: {str(e)}")
        return {"status": "error", "error_message": str(e)}
