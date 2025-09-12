"""
Utility functions for boundary refinement operations.

These functions are extracted from the router to avoid circular imports
and to be reusable across different commands and routers.
"""

import json
from typing import Any, Dict, List, Tuple

from geojson_pydantic import Polygon, Feature
from shapely.geometry import shape

from src.core.storage.storage_factory import StorageFactory
from src.util.cog_ops import create_cog_bytes, crop_cog_with_geometry, download_cog_to_temp
from src.util.polygon_ops import polygon_to_valid_geojson
from src.util.upload_blob import upload_to_gcs


async def process_and_upload_geojson(
    geometry: Polygon | Feature | dict,
    fire_event_name: str,
    job_id: str,
    filename: str,
    storage_factory: StorageFactory,
) -> Tuple[str, Dict[str, Any], List[float]]:
    """
    Validate, save and upload a GeoJSON boundary

    Args:
        geometry: The geometry or GeoJSON to process
        fire_event_name: Name of the fire event
        job_id: Job ID for the processing task
        filename: Base filename for the GeoJSON (without extension)
        storage_factory: Storage factory instance for getting storage providers

    Returns:
        Tuple containing:
        - URL to the uploaded GeoJSON
        - Validated GeoJSON object
        - Bounding box coordinates [minx, miny, maxx, maxy]
    """
    # Convert the Polygon/geometry to a valid GeoJSON object
    valid_geojson = polygon_to_valid_geojson(geometry)

    # Save directly to temp storage and upload
    temp_storage = storage_factory.get_temp_storage()
    geojson_bytes = json.dumps(valid_geojson.model_dump()).encode("utf-8")

    # Generate temp path for intermediate storage
    temp_path = f"{job_id}/{filename}.geojson"
    await temp_storage.save_bytes(geojson_bytes, temp_path, temporary=True)

    # Upload to GCS
    blob_name = f"{fire_event_name}/{job_id}/{filename}.geojson"
    geojson_url = await upload_to_gcs(geojson_bytes, blob_name, storage_factory)

    # Extract bbox from geometry for STAC
    valid_geojson_dict = valid_geojson.model_dump()
    geom_shape = shape(valid_geojson_dict["features"][0]["geometry"])
    bbox = geom_shape.bounds  # (minx, miny, maxx, maxy)

    return geojson_url, valid_geojson_dict, list(bbox)


async def process_cog_with_boundary(
    original_cog_url: str,
    valid_geojson: Dict[str, Any],
    fire_event_name: str,
    job_id: str,
    output_filename: str,
    storage_factory: StorageFactory,
) -> str:
    """
    Process a COG with a boundary: download, crop, create new COG, and upload

    Args:
        original_cog_url: URL to the original COG
        valid_geojson: The validated GeoJSON to crop with
        fire_event_name: Name of the fire event
        job_id: Job ID for the processing task
        output_filename: Filename for the output COG (without extension)

    Returns:
        URL to the uploaded processed COG
    """
    # Download the original COG to a temporary file
    tmp_cog_path = await download_cog_to_temp(original_cog_url)

    # Crop the COG with the refined boundary
    cropped_data = crop_cog_with_geometry(tmp_cog_path, valid_geojson)

    # Create a new COG from the cropped data as bytes
    cog_bytes = await create_cog_bytes(cropped_data)

    # Upload the refined COG to GCS
    cog_blob_name = f"{fire_event_name}/{job_id}/{output_filename}.tif"
    cog_url = await upload_to_gcs(cog_bytes, cog_blob_name, storage_factory)

    return cog_url