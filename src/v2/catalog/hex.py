"""H3 tables of the index COGs, for requests that ask for them (options.h3_resolutions).

GLEN joins layers on H3 cells and its SQL reads Parquet, not COGs. Each
requested resolution becomes one table: one row per cell, holding the pixel of
each index under the cell's centre. A cell-centre sample, not a mean: a table
at r12 or r13, whose cells are smaller than a 20 m pixel, has a value in every
cell, and coarser levels can be derived from it in SQL. Exact numbers in a zone
stay geo-query-wasm's ``zonal_stats`` on the COGs.

Layout, beside the analysis's own collection::

    {fire_id}/
      {fire_id}-hex/             collection.json, README.md, AGENTS.md,
                                 hex-r12.parquet, hex-r13.parquet, ...

The tables have no geometry, so portolan tracks them as collection assets only
with tabular support on (build.py). The collection hook titles each asset and
adds its H3 resolutions and the column dictionary (portolan_plugin.py).

Columns of ``hex-r{n}.parquet``, sorted by cell, only cells where at least one
index is valid:

- ``h{n}`` (uint64): the cell; first, as GLEN's hex tiles take it.
- ``h10`` (uint64, r11 and finer), ``h8`` (uint64, r9 and finer), ``h0``
  (int64): its parents. h8 is the GLEN catalog's join key.
- ``h3`` (string): the cell as H3's hex string.
- one float32 column per index: the pixel under the centre, null at nodata.

Sampling holds the cell ids, one block of cell centres and one index band at
a time. The H3 cell cap (MAX_H3_CELLS) bounds both: a grid with many cells is
small, and the largest grid holds few cells.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import h3
import h3ronpy
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import yaml
from h3ronpy.vector import cells_to_coordinates, geometry_to_cells
from odc.geo.crs import CRS
from odc.geo.geobox import GeoBox
from pyproj import Transformer
from rasterio.transform import rowcol

from src.v2.raster.indices import INDICES

# A fire_id ends in eight hex digits, so no analysis collection has this suffix.
COLLECTION_SUFFIX = "-hex"
WORDING_FILE = Path(__file__).resolve().parents[3] / "config" / "hex.yaml"
# Coarser parents every table carries, when coarser than its own cells: the
# finest level the GLEN catalog uses, its join key, and the root.
PARENT_RESOLUTIONS = (10, 8, 0)
STRING_COLUMN = "h3"
# Cells sampled per block.
BLOCK_CELLS = 1_000_000
ROW_GROUP_ROWS = 100_000
# table:columns types, as portolan writes them from the Parquet schema.
CELL_TYPE = "uint64"
ROOT_TYPE = "int64"
STRING_TYPE = "string"
VALUE_TYPE = "float"


@dataclass(frozen=True)
class HexTables:
    """H3 tables to publish: the request's name and its resolutions, ascending."""

    fire_event_name: str
    resolutions: Tuple[int, ...]


def collection_id(fire_id: str) -> str:
    return f"{fire_id}{COLLECTION_SUFFIX}"


def is_hex_collection(collection_id: str) -> bool:
    return collection_id.endswith(COLLECTION_SUFFIX)


def hex_file(resolution: int) -> str:
    return f"hex-r{resolution}.parquet"


def hex_asset_key(resolution: int) -> str:
    """The key portolan gives the table's collection asset: the file's stem."""
    return f"hex-r{resolution}"


def cell_column(resolution: int) -> str:
    return f"h{resolution}"


def parent_resolutions(resolution: int) -> List[int]:
    """The parents a table at ``resolution`` carries, finest first."""
    return [p for p in PARENT_RESOLUTIONS if p < resolution]


@lru_cache(maxsize=1)
def wording() -> Dict[str, Any]:
    """config/hex.yaml: what the catalog says about the hex tables."""
    loaded: Dict[str, Any] = yaml.safe_load(WORDING_FILE.read_text())
    return loaded


def one_line(text: str, **values: Any) -> str:
    return " ".join(text.format(**values).split())


def cell_description(resolution: int) -> str:
    words = wording()
    text = one_line(words["cell_column"], resolution=resolution)
    note = words["cell_notes"].get(resolution)
    return f"{text} {one_line(note)}" if note else text


def table_columns(resolution: int) -> List[Dict[str, str]]:
    """One table's columns, in order, with their table:columns types."""
    columns = [
        {"name": cell_column(r), "type": ROOT_TYPE if r == 0 else CELL_TYPE}
        for r in (resolution, *parent_resolutions(resolution))
    ]
    columns.append({"name": STRING_COLUMN, "type": STRING_TYPE})
    columns += [{"name": index.name, "type": VALUE_TYPE} for index in INDICES]
    return columns


def column_dictionary(resolutions: Sequence[int]) -> List[Dict[str, str]]:
    """The collection's table:columns: every column of every table once, described."""
    words = wording()
    levels = {r for n in resolutions for r in (n, *parent_resolutions(n))}
    columns = [
        {
            "name": cell_column(r),
            "type": ROOT_TYPE if r == 0 else CELL_TYPE,
            "description": cell_description(r),
        }
        for r in sorted(levels, reverse=True)
    ]
    columns.append(
        {
            "name": STRING_COLUMN,
            "type": STRING_TYPE,
            "description": one_line(words["string_column"]),
        }
    )
    columns += [
        {
            "name": index.name,
            "type": VALUE_TYPE,
            "description": one_line(words["value_column"], title=index.title),
        }
        for index in INDICES
    ]
    return columns


def average_area(resolution: int) -> str:
    """A resolution's average cell area, for people: "44 m²", "1.5 ha", "0.74 km²"."""
    m2 = h3.average_hexagon_area(resolution, unit="m^2")
    if m2 < 1e4:
        return f"{m2:,.0f} m²"
    return f"{m2 / 1e4:.3g} ha" if m2 < 1e6 else f"{m2 / 1e6:.2g} km²"


def asset_fields(resolution: int) -> Dict[str, Any]:
    """What the collection hook sets on a table's asset."""
    words = wording()
    parents = parent_resolutions(resolution)
    return {
        "title": one_line(words["asset_title"], resolution=resolution),
        "description": one_line(
            words["asset_description"],
            resolution=resolution,
            area=average_area(resolution),
            parents=", ".join(cell_column(p) for p in parents),
        ),
        "h3:native_resolution": resolution,
        "h3:parent_resolutions": parents,
    }


def asset_resolution(key: str) -> Optional[int]:
    """The resolution of a table's asset key, or None for any other asset."""
    prefix = "hex-r"
    if key.startswith(prefix) and key[len(prefix) :].isdigit():
        return int(key[len(prefix) :])
    return None


def shared_geobox(cogs: Mapping[str, Path]) -> GeoBox:
    """The grid every index COG shares.

    Raises:
        ValueError: if the COGs are not on one grid.
    """
    geoboxes = []
    for path in cogs.values():
        with rasterio.open(path) as src:
            geoboxes.append(GeoBox.from_rio(src))
    if any(other != geoboxes[0] for other in geoboxes[1:]):
        raise ValueError("The index COGs are not on one grid")
    return geoboxes[0]


def grid_crs(geobox: GeoBox) -> CRS:
    """The grid's CRS.

    Raises:
        ValueError: if the COGs have none.
    """
    if geobox.crs is None:
        raise ValueError("The index COGs have no CRS")
    return geobox.crs


def grid_cells(geobox: GeoBox, resolution: int) -> np.ndarray:
    """Every cell whose centre is in the grid's footprint, sorted.

    The footprint is the grid's outline in degrees, densified so it follows
    the projected edges.
    """
    outline = geobox.footprint("EPSG:4326").geom
    cells = np.asarray(geometry_to_cells(outline, resolution))
    return np.unique(cells.astype(np.uint64))


def as_numpy(array: Any) -> np.ndarray:
    """An Arrow array from h3ronpy as numpy."""
    return np.asarray(pa.array(array))


def pixel_of(
    geobox: GeoBox, cells: np.ndarray
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Row and column of the pixel under each cell's centre, and which are inside."""
    coordinates = cells_to_coordinates(pa.array(cells, pa.uint64()))
    lat = as_numpy(coordinates.column("lat"))
    lng = as_numpy(coordinates.column("lng"))
    to_grid = Transformer.from_crs(
        "EPSG:4326", grid_crs(geobox).to_wkt(), always_xy=True
    )
    x, y = to_grid.transform(lng, lat)
    rows, cols = rowcol(geobox.transform, x, y)
    height, width = geobox.shape
    inside = (rows >= 0) & (rows < height) & (cols >= 0) & (cols < width)
    return np.where(inside, rows, 0), np.where(inside, cols, 0), inside


def sample(
    cogs: Mapping[str, Path], rows: np.ndarray, cols: np.ndarray, inside: np.ndarray
) -> Dict[str, np.ndarray]:
    """Each index's value at the pixels, NaN at nodata or outside the grid.

    Reads one band whole at a time: the H3 cell cap keeps a grid with many
    cells small, and a large grid has few.
    """
    values = {}
    for name, path in cogs.items():
        with rasterio.open(path) as src:
            band = src.read(1, masked=True)
        picked = np.ma.filled(band[rows, cols].astype(np.float32), np.nan)
        picked[~inside] = np.nan
        values[name] = picked
    return values


def table_blocks(
    cogs: Mapping[str, Path], resolution: int, block_cells: int = BLOCK_CELLS
) -> Iterator[pa.Table]:
    """The table for one resolution, block by block in cell order."""
    geobox = shared_geobox(cogs)
    cells = grid_cells(geobox, resolution)
    ordered = {index.name: cogs[index.name] for index in INDICES if index.name in cogs}
    for start in range(0, cells.size, block_cells):
        block = cells[start : start + block_cells]
        values = sample(ordered, *pixel_of(geobox, block))
        keep = np.logical_or.reduce([~np.isnan(v) for v in values.values()])
        if not keep.any():
            continue
        kept = {name: value[keep] for name, value in values.items()}
        yield block_table(block[keep], kept, resolution)


def block_table(
    cells: np.ndarray, values: Mapping[str, np.ndarray], resolution: int
) -> pa.Table:
    arrow_cells = pa.array(cells, pa.uint64())
    columns: Dict[str, pa.Array] = {cell_column(resolution): arrow_cells}
    for parent in parent_resolutions(resolution):
        parents = as_numpy(h3ronpy.change_resolution(arrow_cells, parent))
        columns[cell_column(parent)] = (
            pa.array(parents.astype(np.int64), pa.int64())
            if parent == 0
            else pa.array(parents, pa.uint64())
        )
    columns[STRING_COLUMN] = pa.array(h3ronpy.cells_to_string(arrow_cells), pa.string())
    for name, data in values.items():
        columns[name] = pa.array(data, pa.float32(), mask=np.isnan(data))
    return pa.table(columns)


def write_hex_table(
    cogs: Mapping[str, Path],
    path: Path,
    resolution: int,
    block_cells: int = BLOCK_CELLS,
) -> int:
    """Write one resolution's table to ``path``; return its row count."""
    rows = 0
    writer = None
    try:
        for table in table_blocks(cogs, resolution, block_cells):
            if writer is None:
                writer = pq.ParquetWriter(path, table.schema, compression="zstd")
            writer.write_table(table, row_group_size=ROW_GROUP_ROWS)
            rows += table.num_rows
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError(f"No valid pixel under any H3 r{resolution} cell centre")
    return rows


def write_hex_collection(
    catalog_root: Path,
    fire_id: str,
    cogs: Mapping[str, Path],
    tables: HexTables,
    lineage: str = "",
) -> Path:
    """Write each resolution's table and the collection's metadata; return its folder.

    ``lineage`` is the analysis's processing notes, which the collection's
    own notes extend with the sampling.
    """
    words = wording()
    folder = catalog_root / collection_id(fire_id)
    folder.mkdir(parents=True)
    for resolution in tables.resolutions:
        write_hex_table(cogs, folder / hex_file(resolution), resolution)
    name = tables.fire_event_name
    levels = ", ".join(f"r{r}" for r in tables.resolutions)
    metadata = {
        "title": one_line(words["title"], fire_event_name=name),
        "description": one_line(
            words["description"], fire_event_name=name, levels=levels
        ),
        "processing_notes": one_line(
            words["processing_notes"],
            lineage=lineage,
            levels=levels,
            crs=f"EPSG:{grid_crs(shared_geobox(cogs)).epsg}",
        ),
    }
    (folder / ".portolan").mkdir()
    (folder / ".portolan" / "metadata.yaml").write_text(
        yaml.safe_dump(metadata, sort_keys=False)
    )
    return folder
