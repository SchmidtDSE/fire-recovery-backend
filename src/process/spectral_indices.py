import coiled
import dask.distributed
import rioxarray
from pystac_client import Client as PystacClient
import stackstac
import numpy as np
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles
import os
from typing import List, Dict, Optional, Any
from geojson_pydantic import Polygon

RUN_LOCAL = os.getenv("RUN_LOCAL") == "True"

# @coiled.function(
#     name="process-remote-sensing",
#     container="ghcr.io/schmidtdse/fire-coiled-runner:latest",
#     memory="4 GiB", 
#     cpu=4, 
#     # n_workers=5,
#     n_workers = 1,
#     # local=RUN_LOCAL
#     local=True
# )
def process_remote_sensing_data(
    job_id: str, 
    stac_url: str, 
    geometry: Polygon,
    prefire_date_range: Optional[List[str]], 
    postfire_date_range: Optional[List[str]]
) -> Dict[str, Any]:

    # Initialize workspace
    workspace = initialize_workspace(job_id)
    output_dir = workspace["output_dir"]
    status_file = workspace["status_file"]
    
    try:
        # Get the client
        # client = dask.distributed.get_client()

        # Access STAC catalog
        catalog = PystacClient.open(stac_url)
        
        # Validate input date ranges
        if not prefire_date_range:
            raise ValueError("Prefire date range is required")
        if not postfire_date_range:
            raise ValueError("Postfire date range is required")
        
        # Fetch data
        prefire_data = fetch_stac_data(catalog, geometry, prefire_date_range)
        postfire_data = fetch_stac_data(catalog, geometry, postfire_date_range)
        
        # Calculate burn indices
        indices = calculate_burn_indices(prefire_data, postfire_data)
        
        # Create COGs for each metric
        cog_results = {}
        for name, data in indices.items():
            cog_path = f"{output_dir}/{name}.tif"
            cog_results[name] = create_cog(data, cog_path)
        
        # Write validation details to file
        with open(f"{output_dir}/validation.txt", "w") as f:
            for name, result in cog_results.items():
                f.write(f"{name}: {result['validation']}\n")
        
        # Update status
        all_valid = all(result["is_valid"] for result in cog_results.values())
        with open(status_file, "w") as f:
            f.write("completed" if all_valid else "failed_validation")
        
        return {
            "status": "completed" if all_valid else "failed_validation",
            "output_files": {name: result["path"] for name, result in cog_results.items()},
            "validation": {name: result["validation"] for name, result in cog_results.items()}
        }
            
    except Exception as e:
        # Update status on error
        with open(status_file, "w") as f:
            f.write(f"error: {str(e)}")
        return {"status": f"error: {str(e)}"}

def initialize_workspace(job_id: str) -> Dict[str, str]:
    """Initialize the workspace and return paths to working files"""
    output_dir = f"/tmp/{job_id}"
    os.makedirs(output_dir, exist_ok=True)
    
    status_file = f"{output_dir}/status.txt"
    with open(status_file, "w") as f:
        f.write("started")
    
    return {
        "output_dir": output_dir,
        "status_file": status_file
    }

def fetch_stac_data(catalog: PystacClient, geometry: dict, date_range: List[str]):
    """Fetch and stack STAC items for the given bbox and date range"""
    search = {"intersects": geometry, "datetime": "/".join(date_range)}
    items = catalog.search(**search).get_all_items()
    return stackstac.stack(items, resolution=30)

def calculate_nbr(data):
    """Calculate Normalized Burn Ratio for a dataset"""
    nir = data.sel(band="nir").mean(dim="time")
    swir = data.sel(band="swir22").mean(dim="time")
    return (nir - swir) / (nir + swir)

def calculate_burn_indices(prefire_data, postfire_data):
    """Calculate various burn indices from pre and post fire data"""
    # Calculate NBR for both periods
    prefire_nbr = calculate_nbr(prefire_data)
    postfire_nbr = calculate_nbr(postfire_data)
    
    # Calculate dNBR
    dnbr = prefire_nbr - postfire_nbr
    
    # Calculate RdNBR
    abs_sqrt_prefire_nbr = abs(prefire_nbr)**0.5
    # Avoid division by zero
    abs_sqrt_prefire_nbr = abs_sqrt_prefire_nbr.where(abs_sqrt_prefire_nbr != 0, 0.001)
    rdnbr = dnbr / abs_sqrt_prefire_nbr
    
    # Calculate RBR
    denominator = prefire_nbr + 1.001
    rbr = dnbr / denominator
    
    return {
        "prefire_nbr": prefire_nbr,
        "postfire_nbr": postfire_nbr,
        "dnbr": dnbr,
        "rdnbr": rdnbr,
        "rbr": rbr
    }

def create_cog(data, output_path: str) -> Dict[str, Any]:
    """Create a Cloud Optimized GeoTIFF from xarray data"""
    intermediate_tiff = f"{output_path}_raw.tif"
    data.compute().rio.to_raster(intermediate_tiff)
    
    cog_profile = cog_profiles.get("deflate")
    cog_translate(
        intermediate_tiff,
        output_path,
        cog_profile,
        add_mask=True,
        overview_level=5,
        overview_resampling="average"
    )
    
    validation_result = cog_validate(output_path)
    is_valid = validation_result["valid_cog"]
    
    # Clean up intermediate file
    os.remove(intermediate_tiff)
    
    return {
        "path": output_path,
        "is_valid": is_valid,
        "validation": validation_result
    }