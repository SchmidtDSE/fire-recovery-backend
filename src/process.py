from fastapi import FastAPI, BackgroundTasks, File, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
import coiled
import dask.distributed
import rioxarray
import pystac
import stackstac
from rio_cogeo.cogeo import cog_validate, cog_translate
from rio_cogeo.profiles import cog_profiles
import os
import uuid

async def process_remote_sensing_data(job_id, stac_url, bbox, time_range, operation, include_attribute_table):
    # Create output directory
    output_dir = f"/tmp/{job_id}"
    os.makedirs(output_dir, exist_ok=True)
    intermediate_tiff = f"{output_dir}/result_raw.tif"
    final_cog = f"{output_dir}/result.tif"
    status_file = f"{output_dir}/status.txt"
    
    # Update status
    with open(status_file, "w") as f:
        f.write("started")
    
    # Create or connect to a Coiled cluster
    cluster = coiled.Cluster(
        name=f"remote-sensing-cluster-{job_id}",
        n_workers=5,
        software="coiled/default-py310",  # Use a software environment with your dependencies
    )
    client = dask.distributed.Client(cluster)
    
    try:
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
            # Calculate NDVI
            nir = data.sel(band="nir")
            red = data.sel(band="red")
            ndvi = (nir - red) / (nir + red)
            result = ndvi.compute()  # Triggers distributed computation
        elif operation == "cloud_mask":
            # Simple cloud detection (example)
            qa = data.sel(band="qa")
            cloud_mask = (qa & 0x8000) > 0  # Example bitwise operation
            result = cloud_mask.compute()
        else:
            # Default: temporal mean
            result = data.mean(dim="time").compute()
        
        # Save result to intermediate GeoTIFF
        result.rio.to_raster(intermediate_tiff)
        
        # Define COG profile
        cog_profile = cog_profiles.get("deflate")
        
        # Add RAT if requested
        if include_attribute_table:
            # Generate attribute table data
            # This is a simple example that adds class labels
            attribute_data = {
                "Value": [0, 1, 2, 3, 4],
                "Label": ["No Data", "Very Low", "Low", "Medium", "High"],
                "Color": ["#000000", "#0000FF", "#00FF00", "#FFFF00", "#FF0000"]
            }
            
            # Set RAT options in the profile
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
            
    except Exception as e:
        # Update status on error
        with open(status_file, "w") as f:
            f.write(f"error: {str(e)}")
    finally:
        # Clean up resources
        await client.close()
        await cluster.close()