from typing import List
from pydantic import BaseModel, Field


class ProcessingRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    geometry: dict = Field(..., description="GeoJSON of bounding box AOI")
    prefire_date_range: list[str] = Field(
        ...,
        description="Date range for prefire imagery (e.g. ['2023-01-01', '2023-12-31'])",
    )
    postfire_date_range: list[str] = Field(
        ...,
        description="Date range for postfire imagery (e.g. ['2024-01-01', '2024-12-31'])",
    )


class RefineRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    refine_geojson: dict = Field(..., description="GeoJSON to be refined")
    job_id: str = Field(
        ..., description="Job ID of the original fire severity analysis"
    )


class VegMapResolveRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    veg_gpkg_url: str = Field(..., description="URL to the vegetation map GeoPackage")
    fire_cog_url: str = Field(..., description="URL to the fire severity COG")
    job_id: str = Field(
        ..., description="Job ID of the original fire severity analysis"
    )
    severity_breaks: List[float] = Field(
        ...,
        description="List of classifation breaks for discrete fire severity classification (e.g. [0, .2, .4, .8])",
    )
    geojson_url: str = Field(..., description="URL to the GeoJSON of the fire boundary")


class GeoJSONUploadRequest(BaseModel):
    fire_event_name: str = Field(..., description="Name of the fire event")
    geojson: dict = Field(..., description="GeoJSON data to upload")
