from fastapi import APIRouter, HTTPException, Query
from typing import List, Dict, Any, Optional
import os
from datetime import datetime
import asyncio
from src.stac.stac_geoparquet_manager import STACGeoParquetManager

# Initialize router
router = APIRouter(
    prefix="/stac",
    tags=["STAC API"],
    responses={404: {"description": "Not found"}},
)

# Initialize the STAC GeoParquet manager
BASE_URL = os.environ.get("STAC_BASE_URL", "https://storage.googleapis.com/national_park_service/stac")
STORAGE_DIR = os.environ.get("STAC_STORAGE_DIR", "/tmp/stac_geoparquet")
stac_manager = STACGeoParquetManager(BASE_URL, STORAGE_DIR)

@router.get("/catalog", response_model=Dict[str, Any])
async def get_catalog():
    """
    Get the root STAC catalog
    """
    # This could be dynamic based on available fire events
    catalog = {
        "type": "Catalog",
        "id": "fire-recovery-analysis",
        "stac_version": "1.0.0",
        "description": "Fire Recovery Analysis Products",
        "links": [
            {
                "rel": "self",
                "href": f"{BASE_URL}/catalog.json",
                "type": "application/json"
            }
        ]
    }
    
    # Add links to fire event collections
    storage_dir = os.path.abspath(STORAGE_DIR)
    for file in os.listdir(storage_dir):
        if file.endswith(".parquet"):
            fire_event_name = file.replace(".parquet", "")
            catalog["links"].append({
                "rel": "child",
                "href": f"/stac/collections/{fire_event_name}",
                "type": "application/json",
                "title": fire_event_name
            })
    
    return catalog

@router.get("/collections/{fire_event_name}", response_model=Dict[str, Any])
async def get_collection(fire_event_name: str):
    """
    Get a STAC collection for a fire event
    """
    # Check if collection exists
    parquet_path = stac_manager.get_parquet_path(fire_event_name)
    if not os.path.exists(parquet_path):
        raise HTTPException(status_code=404, detail=f"Collection not found: {fire_event_name}")
    
    # Create a collection response
    collection = {
        "type": "Collection",
        "id": fire_event_name,
        "stac_version": "1.0.0",
        "description": f"Analysis products for {fire_event_name} fire event",
        "license": "proprietary",
        "extent": {
            "spatial": {
                "bbox": [[-180, -90, 180, 90]]  # This should be updated with actual bounds
            },
            "temporal": {
                "interval": [[datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"), None]]
            }
        },
        "links": [
            {
                "rel": "self",
                "href": f"{BASE_URL}/{fire_event_name}/collection.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "href": f"{BASE_URL}/catalog.json",
                "type": "application/json"
            },
            {
                "rel": "parent",
                "href": f"{BASE_URL}/catalog.json",
                "type": "application/json"
            },
            {
                "rel": "items",
                "href": f"/stac/collections/{fire_event_name}/items",
                "type": "application/geo+json"
            },
            {
                "rel": "geoparquet",
                "href": stac_manager.get_parquet_url(fire_event_name),
                "type": "application/x-parquet"
            }
        ]
    }
    
    return collection

@router.get("/collections/{fire_event_name}/items", response_model=Dict[str, Any])
async def get_items(
    fire_event_name: str,
    product_type: Optional[str] = Query(None, description="Filter by product type"),
    bbox: Optional[List[float]] = Query(None, description="Bounding box [west, south, east, north]"),
    datetime: Optional[str] = Query(None, description="Datetime range (e.g. 2023-01-01/2023-12-31)")
):
    """
    Get STAC items for a fire event with optional filtering
    """
    # Parse datetime range if provided
    datetime_range = None
    if datetime:
        parts = datetime.split('/')
        if len(parts) == 2:
            datetime_range = parts
    
    # Search for items
    items = await stac_manager.search_items(
        fire_event_name=fire_event_name,
        product_type=product_type,
        bbox=bbox,
        datetime_range=datetime_range
    )
    
    # Create a FeatureCollection response
    feature_collection = {
        "type": "FeatureCollection",
        "features": items,
        "links": [
            {
                "rel": "self",
                "href": f"{BASE_URL}/{fire_event_name}/items",
                "type": "application/geo+json"
            },
            {
                "rel": "parent",
                "href": f"{BASE_URL}/{fire_event_name}/collection.json",
                "type": "application/json"
            },
            {
                "rel": "root",
                "href": f"{BASE_URL}/catalog.json",
                "type": "application/json"
            },
            {
                "rel": "geoparquet",
                "href": stac_manager.get_parquet_url(fire_event_name),
                "type": "application/x-parquet"
            }
        ]
    }
    
    return feature_collection

@router.get("/collections/{fire_event_name}/items/{item_id}", response_model=Dict[str, Any])
async def get_item(fire_event_name: str, item_id: str):
    """
    Get a specific STAC item by ID
    """
    item = await stac_manager.get_item_by_id(fire_event_name, item_id)
    
    if not item:
        raise HTTPException(status_code=404, detail=f"Item not found: {item_id}")
    
    return item