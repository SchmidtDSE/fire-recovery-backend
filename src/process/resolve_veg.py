import os
import tempfile
from typing import Dict, List, Any, Optional, Tuple

from geojson_pydantic import Polygon
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
from geopandas import GeoDataFrame
import json

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
    """
    # Load as xarray dataset for analysis
    fire_ds = xr.open_dataset(fire_cog_path, engine="rasterio")

    # Extract the main data variable
    data_var = list(fire_ds.data_vars)[0]

    # Project to UTM coordinates if needed
    if fire_ds.rio.crs != PROJECTED_CRS:
        fire_ds = fire_ds.rio.reproject(PROJECTED_CRS)

    # ALWAYS calculate pixel area from the final projected dataset
    projected_transform = fire_ds.rio.transform()
    pixel_width = abs(projected_transform[0])
    pixel_height = abs(projected_transform[4])
    pixel_area_ha = (pixel_width * pixel_height) / 10000  # Convert m² to ha

    # Add assertion to verify CRS
    assert fire_ds.rio.crs == PROJECTED_CRS, "Fire data must be in UTM projection"

    metadata = {
        "crs": PROJECTED_CRS,
        "transform": fire_ds.rio.transform(),
        "pixel_area_ha": pixel_area_ha,
        "data_var": data_var,
        "x_coord": "x" if "x" in fire_ds.coords else "longitude",
        "y_coord": "y" if "y" in fire_ds.coords else "latitude",
    }

    return fire_ds, metadata


def create_severity_masks(
    fire_data: xr.Dataset,
    severity_breaks: List[float],
    boundary: GeoDataFrame,
) -> Dict[str, xr.Dataset]:
    """
    Create masks for different fire severity classes
    """
    assert fire_data.rio.crs is not None, "Fire data must have a CRS defined"
    assert fire_data.rio.crs.to_string() == PROJECTED_CRS

    # filter fire data to the specified boundary
    fire_data = fire_data.rio.clip(
        boundary.geometry.apply(lambda geom: geom.__geo_interface__),
        boundary.crs,
        drop=True,
    )

    UNBURNED_CLASSIFICATION_UPPER_BOUND = severity_breaks[0]
    LOW_CLASSIFICATION_UPPER_BOUND = severity_breaks[1]
    MED_CLASSIFICATION_UPPER_BOUND = severity_breaks[2]

    # Create masks with actual values where condition is met, np.nan elsewhere
    masks = {
        "unburned": fire_data.where(
            (fire_data >= -1) & (fire_data < UNBURNED_CLASSIFICATION_UPPER_BOUND),
            np.nan,
        ),
        "low": fire_data.where(
            (fire_data >= UNBURNED_CLASSIFICATION_UPPER_BOUND)
            & (fire_data < LOW_CLASSIFICATION_UPPER_BOUND),
            np.nan,
        ),
        "moderate": fire_data.where(
            (fire_data >= LOW_CLASSIFICATION_UPPER_BOUND)
            & (fire_data < MED_CLASSIFICATION_UPPER_BOUND),
            np.nan,
        ),
        "high": fire_data.where(fire_data >= MED_CLASSIFICATION_UPPER_BOUND, np.nan),
    }

    # Keep original fire data for overall statistics
    masks["original"] = fire_data

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

    # Ensure all masks have the same CRS
    for severity, mask in masks.items():
        if hasattr(mask, "rio") and mask.rio.crs:
            assert mask.rio.crs.to_string() == PROJECTED_CRS, (
                f"Mask {severity} has incorrect CRS"
            )

    # Consolidate geometries for this MapUnit_ID into a single geometry
    print(f"Consolidating {len(veg_subset)} geometries for MapUnit_ID")
    unified_geometry = gpd.GeoDataFrame(
        {"geometry": [veg_subset.unary_union]}, crs=PROJECTED_CRS
    )

    # Calculate statistics for each severity class
    severity_pixel_counts = {}
    severity_classes = ["unburned", "low", "moderate", "high"]

    for severity in severity_classes:
        mask = masks[severity]
        try:
            stats = mask.xvec.zonal_stats(
                unified_geometry.geometry,
                x_coords=x_coord,
                y_coords=y_coord,
                stats=["count", "mean", "stdev"],
                all_touched=True,
                method="exactextract",
            )

            if stats is not None and not np.isnan(stats.values).all():
                pixel_count = float(stats.isel(zonal_statistics=0).values)
                severity_pixel_counts[severity] = pixel_count
                results[f"{severity}_ha"] = pixel_count * pixel_area_ha
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
            print(f"Error calculating {severity} stats: {str(e)}")
            severity_pixel_counts[severity] = 0.0
            results[f"{severity}_ha"] = 0.0
            results[f"{severity}_mean"] = 0.0
            results[f"{severity}_std"] = 0.0

    # Use sum of severity pixels for total instead of original mask
    total_severity_pixels = sum(severity_pixel_counts.values())
    results["total_pixel_count"] = total_severity_pixels

    # Calculate overall statistics from the original data for mean/std only
    try:
        original_mask = masks["original"]
        stats = original_mask.xvec.zonal_stats(
            unified_geometry.geometry,
            x_coords=x_coord,
            y_coords=y_coord,
            stats=["mean", "std"],  # Remove count since we're using severity sum
            all_touched=True,
        )

        if stats is not None and not np.isnan(stats.values).all():
            results["mean_severity"] = float(stats.isel(zonal_statistics=0).values)
            results["std_dev"] = float(stats.isel(zonal_statistics=1).values)
        else:
            results["mean_severity"] = 0.0
            results["std_dev"] = 0.0
    except Exception as e:
        print(f"Error calculating overall stats: {str(e)}")
        results["mean_severity"] = 0.0
        results["std_dev"] = 0.0

    return results


def create_veg_json_structure(result_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Convert the result DataFrame to the JSON structure for visualization
    """
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

        # Build severity breakdown using the existing percentage columns
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

    # Validation: check if percentages add up to ~100%
    total_percentages = df[
        ["unburned_percent", "low_percent", "moderate_percent", "high_percent"]
    ].sum(axis=1)
    for idx, total_pct in enumerate(total_percentages):
        if abs(total_pct - 100.0) > 1.0:  # Allow 1% tolerance for rounding
            veg_type = df.index[idx]
            print(
                f"Warning: {veg_type} percentages sum to {total_pct:.1f}% (should be ~100%)"
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


async def create_veg_fire_matrix(
    original_veg_gpkg_url: str,
    veg_gpkg_path: str,
    fire_cog_path: str,
    severity_breaks: List[float],
    geojson_path: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Create a matrix showing hectares of each vegetation type affected by different fire severity levels.

    Returns:
        Tuple containing:
        - DataFrame for CSV export (frontend_df with colors and percentages)
        - Dictionary for JSON export (structured for visualization)
    """
    # Load fire data and get metadata
    fire_ds, metadata = load_fire_data(fire_cog_path)

    # Extract original fire data for mean severity calculations
    fire_data = fire_ds[metadata["data_var"]]
    assert fire_data.rio.crs == PROJECTED_CRS, "Fire data should already be projected"

    # Load vegetation data
    veg_gdf = load_vegetation_data(
        veg_gpkg_path, original_url=original_veg_gpkg_url, crs=metadata["crs"]
    )
    assert veg_gdf.crs.to_string() == PROJECTED_CRS, (
        "Vegetation data must be in UTM projection"
    )

    # Load GeoJSON boundary as Polygon
    boundary_projected = gpd.read_file(geojson_path).to_crs(PROJECTED_CRS)
    assert boundary_projected.crs.to_string() == PROJECTED_CRS, (
        "Boundary must be in UTM projection"
    )

    # Clip vegetation data to the boundary
    fire_boundary_geom = boundary_projected.geometry.unary_union
    buffer_distance = 100  # Buffer in projection units (meters)
    fire_boundary_buffered = fire_boundary_geom.buffer(buffer_distance)
    veg_gdf = gpd.clip(veg_gdf, fire_boundary_buffered)

    # Create severity masks
    masks = create_severity_masks(fire_data, severity_breaks, boundary_projected)
    masks["original"] = fire_data

    # Initialize result DataFrame with float dtype to avoid warnings
    veg_types = veg_gdf["veg_type"].unique()
    severity_columns = ["unburned_ha", "low_ha", "moderate_ha", "high_ha", "total_ha"]
    result = pd.DataFrame(0.0, index=veg_types, columns=severity_columns, dtype=float)

    # Process each vegetation type
    for veg_type in veg_types:
        veg_subset = veg_gdf[veg_gdf["veg_type"] == veg_type]

        # Calculate zonal statistics for each severity class
        stats = calculate_zonal_stats(
            masks,
            veg_subset,
            metadata["x_coord"],
            metadata["y_coord"],
            metadata["pixel_area_ha"],
        )

        # Update result with all calculated statistics
        total_pixel_count = stats.get("total_pixel_count", 0)
        result.loc[veg_type, "total_ha"] = total_pixel_count * metadata["pixel_area_ha"]

        # Update all columns that exist in both stats and result
        for key, value in stats.items():
            if key in result.columns:
                result.loc[veg_type, key] = float(value)

        # Add additional stats columns for JSON structure
        for severity in ["unburned", "low", "moderate", "high"]:
            result.loc[veg_type, f"{severity}_mean"] = stats.get(f"{severity}_mean", 0)
            result.loc[veg_type, f"{severity}_std"] = stats.get(f"{severity}_std", 0)

        result.loc[veg_type, "mean_severity"] = stats.get("mean_severity", 0)
        result.loc[veg_type, "std_dev"] = stats.get("std_dev", 0)

    # Add percentage columns
    frontend_df = add_percentage_columns(result)

    # Create JSON structure for visualization
    json_structure = create_veg_json_structure(frontend_df)

    return frontend_df, json_structure


async def process_veg_map(
    veg_gpkg_url: str,
    fire_cog_url: str,
    output_dir: str,
    job_id: str,
    severity_breaks: List[float],
    geojson_url: str,
) -> Dict[str, Any]:
    """
    Process vegetation map against fire severity COG

    Args:
        veg_gpkg_url: URL to vegetation geopackage
        fire_cog_url: URL to fire severity COG
        output_dir: Directory to save output CSV and JSON
        job_id: Unique job identifier
        severity_breaks: Custom breaks for severity classification
        geojson_url: URL to the GeoJSON of the fire boundary

    Returns:
        Dict with status and paths to output files
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        output_csv = os.path.join(output_dir, f"{job_id}_veg_fire_matrix.csv")
        output_json = os.path.join(output_dir, f"{job_id}_veg_fire_matrix.json")

        # Download input files
        veg_gpkg_path = await download_file_to_temp(veg_gpkg_url, suffix=".gpkg")
        fire_cog_path = await download_file_to_temp(fire_cog_url, suffix=".tif")
        geojson_path = await download_file_to_temp(geojson_url, suffix=".geojson")

        # Process the vegetation map against fire severity
        frontend_df, json_structure = await create_veg_fire_matrix(
            original_veg_gpkg_url=veg_gpkg_url,
            veg_gpkg_path=veg_gpkg_path,
            fire_cog_path=fire_cog_path,
            severity_breaks=severity_breaks,
            geojson_path=geojson_path,
        )

        # Save both formats
        frontend_df.to_csv(output_csv, index=False)

        with open(output_json, "w") as f:
            json.dump(json_structure, f, indent=2)

        # Clean up temporary files
        for path in [veg_gpkg_path, fire_cog_path, geojson_path]:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    print(f"Failed to remove temporary file {path}: {str(e)}")

        return {
            "status": "completed",
            "output_csv": output_csv,
            "output_json": output_json,
        }

    except Exception as e:
        print(f"Error processing vegetation map: {str(e)}")
        return {"status": "error", "error_message": str(e)}
