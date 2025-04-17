import coiled
import dask.distributed
import rioxarray
import pystac
import stackstac
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles
import os
import uuid
from typing import List, Dict, Optional, Any

@coiled.function(
    name="process-remote-sensing",
    software="ghcr.io/yourusername/fire-recovery-env:latest",
    memory="16 GiB", 
    cpu=4, 
    workers=5, 
    run_on_coiled=True # Toggle for running on Coiled / local
)
def process_remote_sensing_data(
    job_id: str, 
    stac_url: str, 
    bbox: List[float], 
    time_range: Optional[List[str]], 
    operation: str, 
    include_attribute_table: bool
) -> Dict[str, Any]:

    # Create output directory
    output_dir = f"/tmp/{job_id}"
    os.makedirs(output_dir, exist_ok=True)
    intermediate_tiff = f"{output_dir}/result_raw.tif"
    final_cog = f"{output_dir}/result.tif"
    status_file = f"{output_dir}/status.txt"
    
    # Update status
    with open(status_file, "w") as f:
        f.write("started")
    
    try:
        # When using the @coiled.function decorator
        # you can access the client directly
        client = dask.distributed.get_client()
        
        # Access STAC catalog
        catalog = pystac.Client.open(stac_url)
        
        # Search for items based on bbox and time range
        search_params = {"bbox": bbox}
        if time_range:
            search_params["datetime"] = "/".join(time_range)
        
        items = catalog.search(**search_params).get_all_items()
        
        # Load data with stackstac/rioxarray
        data = stackstac.stack(items, resolution=30)
        
        # Perform distributed computation based on operation
        if operation == "ndvi":
            nir = data.sel(band="nir")
            red = data.sel(band="red")
            ndvi = (nir - red) / (nir + red)
            result = ndvi.compute()
        elif operation == "cloud_mask":
            qa = data.sel(band="qa")
            cloud_mask = (qa & 0x8000) > 0
            result = cloud_mask.compute()
        else:
            result = data.mean(dim="time").compute()
        
        # Save result to intermediate GeoTIFF
        result.rio.to_raster(intermediate_tiff)
        
        # Define COG profile
        cog_profile = cog_profiles.get("deflate")
        
        # Add RAT if requested
        if include_attribute_table:
            attribute_data = {
                "Value": [0, 1, 2, 3, 4],
                "Label": ["No Data", "Very Low", "Low", "Medium", "High"],
                "Color": ["#000000", "#0000FF", "#00FF00", "#FFFF00", "#FF0000"]
            }
            
            rat_options = {
                "tags": {"RASTER_ATTRIBUTE_TABLE": str(attribute_data)}
            }
            cog_profile.update(rat_options)
        
        # Convert to valid COG using rio-cogeo
        cog_translate(
            intermediate_tiff,
            final_cog,
            cog_profile,
            add_mask=True,
            overview_level=5,
            overview_resampling="average"
        )
        
        # Validate the COG
        validation_result = cog_validate(final_cog)
        is_valid = validation_result["valid_cog"]
        
        # Update status
        with open(status_file, "w") as f:
            f.write("completed" if is_valid else "failed_validation")
        
        # Store validation details
        with open(f"{output_dir}/validation.txt", "w") as f:
            f.write(str(validation_result))
            
        return {
            "status": "completed" if is_valid else "failed_validation",
            "output_file": final_cog,
            "validation": validation_result
        }
            
    except Exception as e:
        # Update status on error
        with open(status_file, "w") as f:
            f.write(f"error: {str(e)}")
        return {"status": f"error: {str(e)}"}