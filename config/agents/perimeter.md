# AGENTS.md — $fire_event_name perimeter

Guidance for AI agents working with this collection: the fire perimeter submitted with the
burn severity analysis of $fire_event_name.

## Overview

One feature: the perimeter exactly as the requester sent it, with coordinates rounded to a
millionth of a degree (about 0.11 m). **It was not checked against an official perimeter.**
Use it as the zone for summarising the analysis's rasters. The analysis itself covers a
buffered box around it.

| File | What it is |
|---|---|
| `perimeter.parquet` | GeoParquet 1.1, one row, MultiPolygon in EPSG:4326, with a `bbox` covering column |
| `perimeter.pmtiles` | Vector tiles of the same geometry, source layer `perimeter` |
| `styles/default.json` | A MapLibre style for the tiles |

## Accessing the data

Collection URL: `$catalog_url/$fire_id-perimeter/`

```python
import geopandas as gpd

perimeter = gpd.read_parquet("$catalog_url/$fire_id-perimeter/perimeter.parquet")
hectares = float(perimeter.to_crs(perimeter.estimate_utm_crs()).area.sum() / 1e4)
```

In a browser, point MapLibre with the pmtiles protocol at `perimeter.pmtiles`, or load
`styles/default.json`.

## Schema & field notes

| Column | Type | Meaning |
|---|---|---|
| `fire_id` | string | The analysis this perimeter belongs to: `$fire_id` |
| `fire_event_name` | string | The name the analysis was requested under |
| `geometry` | MultiPolygon, WKB | Longitude and latitude, EPSG:4326 |
| `bbox` | struct | The geometry's bounding box (GeoParquet covering) |

## Data quality & usage notes

- **It is the requester's geometry, not an authority's.** If an official perimeter exists,
  the two can differ. Say which one a number is based on.
- **Rings are kept as submitted.** Holes and multiple parts are preserved. Nothing was
  simplified, buffered or repaired.
- **Clip in the raster's CRS.** The rasters are in UTM. Reproject the perimeter to the
  raster's `proj:code` before masking (`perimeter.to_crs(src.crs)`).
- **The burned area isn't the perimeter.** Severity inside a perimeter varies, and some
  pixels inside it may be unburned. Areas of change can extend outside it. Summarise the
  rasters with a threshold rather than taking the perimeter's area as the burned area.

## Example queries

Pixels above a threshold inside the perimeter:

```python
import geopandas as gpd
import rasterio
from rasterio.mask import mask

perimeter = gpd.read_parquet("$catalog_url/$fire_id-perimeter/perimeter.parquet")
with rasterio.open("$catalog_url/$fire_id/rbr/rbr.tif") as src:
    inside, _ = mask(src, perimeter.to_crs(src.crs).geometry, crop=True, filled=False)
    pixel_area_m2 = abs(src.res[0] * src.res[1])
threshold = 0.1  # the analyst's choice
print(float((inside[0] > threshold).sum()) * pixel_area_m2 / 1e4, "ha")
```

## Related collections

- `../$fire_id/`: the severity rasters this perimeter was submitted with.
- `../$fire_id-hex/`: H3 tables of the same analysis, if requested. Their cells cover the
  whole analysis grid, so clip them with this perimeter too.
