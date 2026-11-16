# AGENTS.md — $fire_event_name burn severity on H3 cells

Guidance for AI agents working with this collection: the analysis's dNBR, RdNBR and RBR
sampled onto H3 cells at $levels, for SQL and joins with other H3 layers.

## Overview

One Parquet table per resolution, `hex-r{n}.parquet`, each a collection-level asset. A row is
one H3 cell whose centre falls in the analysis grid and lands on at least one valid pixel.
**Each index column holds the pixel under the cell's centre**: a sample, not a mean and not
coverage-weighted.

- **Cells smaller than a pixel** (r12 and r13 against 20 or 30 m pixels) repeat their
  pixel's value. Counting them, each weighted by its own area, closely reproduces the
  pixels' area.
- **Cells larger than a pixel** (r11 and coarser) take one pixel from many. Don't read them
  as zone averages. To summarise a coarse cell, aggregate the finest table by its parent
  column instead.

Exact numbers come from the COGs in `../$fire_id/`. Report H3 results as estimates.

## Accessing the data

Collection URL: `$catalog_url/$fire_id-hex/`. The finest table here is
`hex-r$finest.parquet`.

The examples are DuckDB SQL. They run as written in duckdb-wasm in a browser, which reads
the tables over HTTPS directly. Cell functions such as `h3_cell_area` and
`h3_cell_to_parent` come from DuckDB's community h3 extension:

```sql
INSTALL h3 FROM community;
LOAD h3;
```

Area above a threshold, using each cell's own area:

```sql
SELECT sum(h3_cell_area(h$finest, 'm^2')) / 1e4 AS hectares
FROM read_parquet('$catalog_url/$fire_id-hex/hex-r$finest.parquet')
WHERE rbr > 0.1;   -- 0.1: the analyst's threshold
```

Mean RBR and the share of cells above a threshold, per r8 cell:

```sql
SELECT h8, count(*) AS cells, avg(rbr) AS mean_rbr,
       avg((rbr > 0.1)::INT) AS share_above
FROM read_parquet('$catalog_url/$fire_id-hex/hex-r$finest.parquet')
GROUP BY h8
ORDER BY h8;
```

The same SQL runs in native DuckDB, including from Python, where h3-py offers the same cell
functions.

## Schema & field notes

| Column | Type | Meaning |
|---|---|---|
| `h{n}` | uint64 | The row's cell at the table's own resolution. Always the first column. Unique and sorted |
| `h10` | uint64 | Parent at r10. Present in r11 and finer tables |
| `h8` | uint64 | Parent at r8. The join key shared by the GLEN catalog's H3 layers |
| `h0` | int64 | Parent at r0. Signed by the catalog's convention, unlike the other cell columns |
| `h3` | string | The row's own cell as H3's hexadecimal string |
| `dnbr`, `rdnbr`, `rbr` | float | The pixel under the cell's centre. Unscaled; null where that pixel is nodata |

Each asset in `collection.json` carries `h3:native_resolution`, `h3:parent_resolutions`, its
own `table:row_count` and `table:columns`.

## Data quality & usage notes

- **Cell areas vary with location.** Weight by each cell's own area (`h3_cell_area`), not
  H3's average for the resolution, which can be off by 10% or more for a given place.
- **The cells cover the whole analysis grid**, a buffered box around the fire, not just the
  fire. Clip with a zone first, for example the perimeter collection, by testing each cell's
  centre against the polygon.
- **Coarser levels:** for a resolution between those published, group the finest table by
  `h3_cell_to_parent(h$finest, resolution)`.
- **RdNBR is unstable where prefire vegetation is sparse** and reaches the hundreds there.
  See `../$fire_id/AGENTS.md`. Prefer RBR or dNBR, or report medians.
- **No severity classes are stored.** The analyst chooses thresholds.

## Example queries

Mean RBR per r11 cell, from the finest table's children rather than an r11 sample:

```sql
SELECT h3_cell_to_parent(h$finest, 11) AS h11, avg(rbr) AS mean_rbr, count(*) AS children
FROM read_parquet('$catalog_url/$fire_id-hex/hex-r$finest.parquet')
GROUP BY h11;
```

Join to another H3 layer on the shared key:

```sql
SELECT o.*, s.mean_rbr
FROM read_parquet('other_layer.parquet') AS o
JOIN (
  SELECT h8, avg(rbr) AS mean_rbr
  FROM read_parquet('$catalog_url/$fire_id-hex/hex-r$finest.parquet')
  GROUP BY h8
) AS s USING (h8);
```

## Related collections

- `../$fire_id/`: the COGs these cells were sampled from. Use them for exact numbers.
- `../$fire_id-perimeter/`: the perimeter, if the request had one. Use it to clip the cells.
