# AGENTS.md — $fire_event_name burn severity

Guidance for AI agents working with this catalog: one burn severity analysis of
$fire_event_name, published by the Schmidt Center for Data Science & Environment (UC Berkeley).
The README beside this file has the extent, dates and method in prose. This file covers what
you need to use the data correctly.

## Overview

A static STAC catalog (Portolan profile) on public object storage. There is no API and no
authentication: every file is a plain HTTPS GET, and the large files support range requests.
The catalog holds one analysis: three burn severity indices (dNBR, RdNBR, RBR) computed from
one prefire and one postfire composite of satellite surface reflectance.

- Catalog root: `$catalog_url/catalog.json`
- Analysis id: `$fire_id`, the fire's name followed by the first eight characters of the job
  id. The same request always produces the same id. A change of method
  (`fire:pipeline_version`) produces a new one.

## Collections

Follow the `child` links in `catalog.json`. Each collection has its own README.md and
AGENTS.md.

| Collection | Present | What it holds |
|---|---|---|
| `$fire_id/` | Always | The three indices as Cloud-Optimized GeoTIFFs, one STAC item per index, on the sensor's native UTM grid. **The source of truth for any number you report.** |
| `$fire_id-perimeter/` | Only if the request was made with a perimeter | The perimeter as submitted, as GeoParquet and PMTiles. |
| `$fire_id-hex/` | Only if the request asked for H3 resolutions | Each index sampled onto H3 cells, one Parquet table per resolution, for SQL and joins with other H3 layers. Approximate. |

## Data access patterns

- **Rasters:** open the COG URLs directly with GDAL, rasterio, rioxarray or geotiff.js; don't
  download them first. They are in a local UTM zone (each item's `proj:code`), not in
  longitude and latitude.
- **Tables:** read the Parquet URLs with DuckDB (`read_parquet('https://…')`), pandas or
  geopandas. The perimeter and the H3 tables are in longitude and latitude (EPSG:4326) or H3
  cells.
- **Metadata:** the method is recorded on each item: `processing:lineage` (prose), each
  asset's `processing:expression` (the formula), and `fire:*` properties (`fire:grid`,
  `fire:prefire_scenes`, `fire:postfire_scenes`, `fire:mask`, `fire:pipeline_version`,
  `fire:job_id`).

```python
import rasterio

with rasterio.open("$catalog_url/$fire_id/rbr/rbr.tif") as src:
    rbr = src.read(1, masked=True)  # nodata (-9999) masked
    pixel_area_m2 = abs(src.res[0] * src.res[1])
```

## Using the numbers

- **No severity classes are stored.** The indices are continuous. Thresholds are the
  analyst's choice: ask, or say which breaks you used and where they come from. Don't
  present any one set of class breaks as the analysis's own.
- **Exact numbers come from the COGs.** Area above a threshold, or a mean inside a zone, is
  computed on the pixels, weighted by how much of each pixel lies in the zone (for example
  with exactextract). The H3 tables are a cell-centre sample: good for exploring and for
  joins, and labelled as estimates when reported. 
- **The analysis covers a buffered box, not the fire.** The rasters extend past the
  perimeter on every side. Clip to a zone before summarising, or the unburned buffer dilutes
  every mean.

## License

CC-BY-4.0. Credit the Schmidt Center for Data Science & Environment, UC Berkeley
(dse@berkeley.edu). The source imagery's provider and license are in the README's Source
section and each item's `fire:source`.
