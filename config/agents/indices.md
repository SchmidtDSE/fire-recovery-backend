# AGENTS.md — $fire_event_name burn severity

Guidance for AI agents working with this collection: the burn severity rasters of one
analysis of $fire_event_name. The README beside this file has the extent, dates and full
method.

## Overview

One STAC item per index, each with one Cloud-Optimized GeoTIFF and a thumbnail:

| Item | COG | Index |
|---|---|---|
| `dnbr/dnbr.json` | `dnbr/dnbr.tif` | dNBR = prefire NBR − postfire NBR |
| `rdnbr/rdnbr.json` | `rdnbr/rdnbr.tif` | RdNBR = dNBR / sqrt(\|prefire NBR\|) (Miller & Thode 2007) |
| `rbr/rbr.json` | `rbr/rbr.tif` | RBR = dNBR / (prefire NBR + 1.001) (Parks et al. 2014) |

NBR = (NIR − SWIR) / (NIR + SWIR), from a per-pixel median of each window's cloud-free
observations. `items.parquet` lists the items as stac-geoparquet, for querying many
analyses at once.

## Accessing the data

Collection URL: `$catalog_url/$fire_id/`

```python
import rasterio

with rasterio.open("$catalog_url/$fire_id/rbr/rbr.tif") as src:
    rbr = src.read(1, masked=True)
    pixel_area_m2 = abs(src.res[0] * src.res[1])

threshold = 0.1  # the analyst's choice
hectares = float((rbr > threshold).sum()) * pixel_area_m2 / 1e4
```

To summarise inside a zone, reproject the zone to the raster's CRS, then use exactextract
for coverage-weighted results, or `rasterio.mask.mask(src, shapes, crop=True,
filled=False)` for the pixels whose centres fall inside.

## Schema & field notes

- **The grid:** the three COGs share one grid exactly: the same CRS, transform and shape. It
  is in a UTM zone (`proj:code`) at the sensor's native resolution (20 m for Sentinel-2,
  30 m for Landsat), aligned to the sensor's own pixel edges. `fire:grid` on each item
  records it.
- **Values:** float32, unscaled, dimensionless. Nodata is -9999: pixels with no clear
  observation in one of the windows.
- **Sign:** higher means more loss of vegetation. Values near 0 mean little change. Negative
  values mean greening, regrowth, or change unrelated to fire, such as seasonal drying or
  water.
- **Dates:** each window's scenes are listed in `fire:prefire_scenes` and
  `fire:postfire_scenes`, each with its id, acquisition time, link and processing details.
  The item's `datetime` is the end of the postfire window.
- **Display:** the collection's `renders` hold a fixed stretch per index (dNBR −0.1 to 0.66,
  RdNBR 0 to 0.8, RBR 0 to 0.5) on matplotlib's YlOrRd, which is TiTiler's `ylorrd`. These
  are display choices, not class breaks.

## Data quality & usage notes

- **RdNBR blows up where prefire vegetation is sparse.** Its denominator is
  sqrt(|prefire NBR|), so where prefire NBR is near zero (bare ground, desert scrub, rock)
  RdNBR reaches the hundreds. (A denominator of exactly zero is replaced by 0.001.) RBR was
  designed to avoid this. Prefer RBR or dNBR in sparse vegetation. If you must use RdNBR,
  report medians or clip the values, never a plain mean.
- **The rasters cover a buffered box, not the fire.** Clip to a perimeter or zone before
  summarising.
- **Composites, not scenes.** Each pixel is the median of its clear observations in the
  window. Clouds, shadows and snow are masked first (the classes are in `fire:mask`). A
  pixel's value can therefore come from different dates for prefire and postfire.
- **No classes are stored.** The analyst chooses the thresholds.

## Example queries

Mean RBR inside a perimeter, counting pixels by centre:

```python
import geopandas as gpd
import rasterio
from rasterio.mask import mask

zone = gpd.read_parquet("$catalog_url/$fire_id-perimeter/perimeter.parquet")
with rasterio.open("$catalog_url/$fire_id/rbr/rbr.tif") as src:
    inside, _ = mask(src, zone.to_crs(src.crs).geometry, crop=True, filled=False)
print(float(inside[0].mean()))
```

(The perimeter collection exists only for requests made with a perimeter. Any zone works.)

## Related collections

- `../$fire_id-perimeter/`: the perimeter the analysis was requested with, if any.
- `../$fire_id-hex/`: the same pixels sampled onto H3 cells, if requested. Use it for joins
  with other H3 layers, and these COGs for exact numbers.
