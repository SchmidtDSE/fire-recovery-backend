"""Each index's H3 hex table: the mean of its valid pixels per resolution-10 cell.

GLEN joins layers on H3 cells, and its SQL reads Parquet, not COGs. One table
per index, one row per h10 cell, with the parents GLEN's catalog joins on (h8)
and partitions by (h0), following data-workflows' raster convention
(native resolution from the source pixel; h10 is the finest the catalog uses).

Each valid pixel counts toward the one cell that holds its centre, so every
valid pixel is counted exactly once: the tables' total pixel count and the
pixel sum match the COG's. Native-resolution statistics in a zone, with
partial pixels weighted, are geo-query-wasm's ``zonal_stats`` on the COGs.

The rasters are read in row strips, so memory stays bounded at the largest
analysis the pipeline accepts.
"""

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Mapping, Tuple

import h3.api.numpy_int as h3n
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
import yaml
from pyproj import Transformer
from rasterio.transform import xy
from rasterio.windows import Window

HEX_RESOLUTION = 10
HEX_PARENTS = (9, 8, 0)
STRIP_ROWS = 256
ROW_GROUP_ROWS = 100_000
COUNT_COLUMN = "n_valid"
FIRE_ID_COLUMN = "fire_id"
# The titles, descriptions and column dictionary the collection carries.
WORDING_FILE = Path(__file__).resolve().parents[3] / "config" / "hex.yaml"
# table:columns types, as portolan writes them from the Parquet schema.
VALUE_TYPE = "float"
COLUMN_TYPES = {
    COUNT_COLUMN: "uint32",
    **{cell_name: "uint64" for cell_name in ("h10", "h9", "h8")},
    "h0": "int64",
    FIRE_ID_COLUMN: "string",
}


def hex_file(index: str) -> str:
    return f"{index}-hex.parquet"


def hex_asset_key(index: str) -> str:
    """The key portolan gives the table's collection asset: the file's stem."""
    return f"{index}-hex"


def cell_column(resolution: int) -> str:
    return f"h{resolution}"


@lru_cache(maxsize=1)
def wording() -> Dict[str, Any]:
    """config/hex.yaml: what the collection says about the hex tables."""
    loaded: Dict[str, Any] = yaml.safe_load(WORDING_FILE.read_text())
    return loaded


def column_dictionary(indices: Mapping[str, str]) -> List[Dict[str, str]]:
    """The collection's table:columns: every hex column once, described.

    ``indices`` maps each index's name to its title.
    """
    words = wording()
    columns = [
        {
            "name": name,
            "type": VALUE_TYPE,
            "description": " ".join(
                words["value_column"].format(title=title, index=name).split()
            ),
        }
        for name, title in indices.items()
    ]
    columns += [
        {
            "name": name,
            "type": kind,
            "description": " ".join(words["columns"][name].split()),
        }
        for name, kind in COLUMN_TYPES.items()
    ]
    return columns


def strip_aggregates(
    cogs: Mapping[str, Path], strip_rows: int = STRIP_ROWS
) -> Tuple[np.ndarray, Dict[str, np.ndarray], Dict[str, np.ndarray]]:
    """Per-cell pixel sums and counts for every index, read strip by strip.

    Returns the sorted h10 cells and, per index, the sum and count of its
    valid pixels in each cell (a cell an index has no valid pixel in has a
    count of 0).
    """
    names = list(cogs)
    sources = [rasterio.open(cogs[name]) for name in names]
    try:
        first = sources[0]
        for src in sources[1:]:
            if (src.transform, src.shape, src.crs) != (
                first.transform,
                first.shape,
                first.crs,
            ):
                raise ValueError("The index COGs are not on one grid")
        to_lonlat = Transformer.from_crs(first.crs, "EPSG:4326", always_xy=True)
        parts_cells: List[np.ndarray] = []
        parts_sums: Dict[str, List[np.ndarray]] = {name: [] for name in names}
        parts_counts: Dict[str, List[np.ndarray]] = {name: [] for name in names}
        for row0 in range(0, first.height, strip_rows):
            window = Window(0, row0, first.width, min(strip_rows, first.height - row0))
            strips = {
                name: src.read(1, window=window, masked=True)
                for name, src in zip(names, sources)
            }
            valid = {name: ~np.ma.getmaskarray(a) for name, a in strips.items()}
            rows, cols = np.nonzero(np.logical_or.reduce(list(valid.values())))
            if rows.size == 0:
                continue
            xs, ys = xy(first.transform, rows + row0, cols, offset="center")
            lon, lat = to_lonlat.transform(np.asarray(xs), np.asarray(ys))
            cells = np.fromiter(
                (
                    h3n.latlng_to_cell(la, lo, HEX_RESOLUTION)
                    for la, lo in zip(lat, lon)
                ),
                dtype=np.uint64,
                count=lat.size,
            )
            strip_cells, inverse = np.unique(cells, return_inverse=True)
            parts_cells.append(strip_cells)
            for name, data in strips.items():
                ok = valid[name][rows, cols]
                values = np.where(ok, data.data[rows, cols], 0.0).astype(np.float64)
                parts_sums[name].append(
                    np.bincount(inverse, weights=values, minlength=strip_cells.size)
                )
                parts_counts[name].append(
                    np.bincount(inverse, weights=ok, minlength=strip_cells.size)
                )
    finally:
        for src in sources:
            src.close()

    if not parts_cells:
        empty = np.array([], dtype=np.uint64)
        return empty, {n: np.array([]) for n in names}, {n: np.array([]) for n in names}
    # A cell can span strips: merge the strips' partial sums.
    all_cells = np.concatenate(parts_cells)
    cells, inverse = np.unique(all_cells, return_inverse=True)
    sums = {
        name: np.bincount(inverse, weights=np.concatenate(parts), minlength=cells.size)
        for name, parts in parts_sums.items()
    }
    counts = {
        name: np.bincount(
            inverse, weights=np.concatenate(parts), minlength=cells.size
        ).astype(np.int64)
        for name, parts in parts_counts.items()
    }
    return cells, sums, counts


def write_hex_tables(
    cogs: Mapping[str, Path],
    directory: Path,
    fire_id: str,
    strip_rows: int = STRIP_ROWS,
) -> Dict[str, Path]:
    """One Parquet table per index in ``directory``; returns their paths.

    Columns: the index's mean (float32), ``n_valid``, ``h10``, ``h9``, ``h8``
    (uint64), ``h0`` (int64) and ``fire_id``, sorted by ``h10``. A table holds
    only the cells where its index has at least one valid pixel.
    """
    cells, sums, counts = strip_aggregates(cogs, strip_rows)
    parents = {
        p: np.fromiter(
            (h3n.cell_to_parent(int(c), p) for c in cells),
            dtype=np.uint64,
            count=cells.size,
        )
        for p in HEX_PARENTS
    }
    written = {}
    for name in cogs:
        keep = counts[name] > 0
        n = counts[name][keep]
        columns = {
            name: pa.array((sums[name][keep] / n).astype(np.float32), pa.float32()),
            COUNT_COLUMN: pa.array(n.astype(np.uint32), pa.uint32()),
            cell_column(HEX_RESOLUTION): pa.array(cells[keep], pa.uint64()),
        }
        for p in HEX_PARENTS:
            # h0 is int64 by convention (the partition key); finer levels uint64.
            kind = pa.int64() if p == 0 else pa.uint64()
            columns[cell_column(p)] = pa.array(
                parents[p][keep].astype(np.int64 if p == 0 else np.uint64), kind
            )
        columns[FIRE_ID_COLUMN] = pa.array([fire_id] * int(keep.sum()), pa.string())
        path = directory / hex_file(name)
        pq.write_table(
            pa.table(columns), path, compression="zstd", row_group_size=ROW_GROUP_ROWS
        )
        written[name] = path
    return written
