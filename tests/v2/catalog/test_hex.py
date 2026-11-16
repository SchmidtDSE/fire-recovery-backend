"""The H3 tables of an analysis's indices (options.h3_resolutions)."""

import json
from pathlib import Path
from typing import Dict

import h3
import numpy as np
import pyarrow.parquet as pq
import pytest
import rasterio
from pyproj import Transformer

from src.models.provenance import Provenance
from src.v2.catalog import hex as hexmod
from src.v2.catalog.build import CatalogResult, build_catalog
from src.v2.catalog.hex import (
    HexTables,
    collection_id,
    column_dictionary,
    hex_file,
    write_hex_table,
)
from src.v2.catalog.sidecar import build_sidecar
from src.v2.identity.grid import Grid, h3_cell_count
from src.v2.identity.job_id import CanonicalRequest, fire_id
from src.v2.raster.cog import write_cog

NODATA = -9999.0
# 60 x 50 pixels of 20 m near the Geology fire: 1.2 km².
SMALL = Grid(epsg=32611, resolution=20, bounds=(580000, 3750000, 581200, 3751000))
INDEX_COLUMNS = [("dnbr", "float"), ("rdnbr", "float"), ("rbr", "float")]


def small_cogs(directory: Path) -> Dict[str, Path]:
    """Distinct values in every pixel. Rows 0-4 nodata in every index; rows
    10-14 nodata in dNBR only."""
    directory.mkdir(parents=True, exist_ok=True)
    cogs = {}
    for offset, name in enumerate(("dnbr", "rdnbr", "rbr")):
        data = np.arange(SMALL.height * SMALL.width, dtype=np.float32).reshape(
            SMALL.height, SMALL.width
        ) + np.float32(offset * 10_000)
        data[:5] = NODATA
        if name == "dnbr":
            data[10:15] = NODATA
        raw = directory / f"{name}_raw.tif"
        with rasterio.open(
            raw, "w", driver="GTiff", width=SMALL.width, height=SMALL.height,
            count=1, dtype="float32", nodata=NODATA, crs=f"EPSG:{SMALL.epsg}",
            transform=SMALL.transform(),
        ) as dst:  # fmt: skip
            dst.write(data, 1)
        cogs[name] = write_cog(raw, directory / f"{name}.tif")
        raw.unlink()
    return cogs


@pytest.fixture
def cogs(tmp_path: Path) -> Dict[str, Path]:
    return small_cogs(tmp_path / "cogs")


@pytest.fixture
def r13(tmp_path: Path, cogs: Dict[str, Path]) -> Path:
    path = tmp_path / hex_file(13)
    write_hex_table(cogs, path, 13)
    return path


@pytest.mark.parametrize(
    "resolution, cells",
    [
        (13, [("h13", "uint64"), ("h10", "uint64"), ("h8", "uint64")]),
        (11, [("h11", "uint64"), ("h10", "uint64"), ("h8", "uint64")]),
        (10, [("h10", "uint64"), ("h8", "uint64")]),
        (9, [("h9", "uint64"), ("h8", "uint64")]),
        (8, [("h8", "uint64")]),
    ],
)
def test_columns_follow_the_contract(
    tmp_path: Path, cogs: Dict[str, Path], resolution: int, cells: list
) -> None:
    path = tmp_path / hex_file(resolution)
    write_hex_table(cogs, path, resolution)
    schema = pq.read_schema(path)
    assert [(f.name, str(f.type)) for f in schema] == [
        *cells, ("h0", "int64"), ("h3", "string"), *INDEX_COLUMNS,
    ]  # fmt: skip
    # The collection's dictionary describes every column, with its type.
    described = {c["name"]: c for c in column_dictionary([resolution])}
    for field in schema:
        assert described[field.name]["description"]
        assert described[field.name]["type"] == str(field.type)


def test_each_cell_holds_the_pixel_under_its_centre(
    r13: Path, cogs: Dict[str, Path]
) -> None:
    table = pq.read_table(r13).to_pandas()
    to_utm = Transformer.from_crs("EPSG:4326", f"EPSG:{SMALL.epsg}", always_xy=True)
    rasters = {name: rasterio.open(path) for name, path in cogs.items()}
    try:
        for row in table.sample(300, random_state=0).itertuples():
            lat, lng = h3.cell_to_latlng(h3.int_to_str(int(row.h13)))
            x, y = to_utm.transform(lng, lat)
            for name, src in rasters.items():
                r, c = src.index(x, y)
                pixel = float(src.read(1)[r, c])
                value = getattr(row, name)
                if pixel == NODATA:
                    assert np.isnan(value)
                else:
                    assert value == pixel
    finally:
        for src in rasters.values():
            src.close()


def test_nodata_is_null_and_cells_with_no_valid_index_are_dropped(r13: Path) -> None:
    table = pq.read_table(r13)
    frame = table.to_pandas()
    # Pixel values are row * 60 + col (+ 10,000 per index): rows 0-4 never
    # appear, and dNBR alone is missing for rows 10-14.
    rdnbr_rows = (frame["rdnbr"] - 10_000) // SMALL.width
    assert rdnbr_rows.min() == 5
    assert frame["rdnbr"].notna().all() and frame["rbr"].notna().all()
    missing = frame["dnbr"].isna()
    assert set(rdnbr_rows[missing]) == set(range(10, 15))
    assert table.column("dnbr").null_count == int(missing.sum())


def test_cells_are_sorted_unique_and_their_parents_are_h3s(r13: Path) -> None:
    frame = pq.read_table(r13).to_pandas()
    cells = frame["h13"].to_numpy()
    assert (np.diff(cells.astype(np.int64)) > 0).all()
    for row in frame.sample(100, random_state=1).itertuples():
        cell = h3.int_to_str(int(row.h13))
        assert row.h3 == cell
        for parent in (10, 8, 0):
            expected = h3.str_to_int(h3.cell_to_parent(cell, parent))
            assert int(getattr(row, f"h{parent}")) == expected


def test_r13_covers_the_valid_area_at_its_cell_size(r13: Path) -> None:
    frame = pq.read_table(r13).to_pandas()
    valid_m2 = (SMALL.height - 5) * SMALL.width * SMALL.resolution**2
    cell_m2 = h3.cell_area(frame["h3"].iloc[0], unit="m^2")
    assert len(frame) == pytest.approx(valid_m2 / cell_m2, rel=0.02)
    # The request limit's estimate, over the whole grid.
    estimate = h3_cell_count(SMALL, 13)
    full = SMALL.height * SMALL.width * SMALL.resolution**2 / cell_m2
    assert estimate == pytest.approx(full, rel=0.01)


def test_blocks_and_strips_make_the_same_table(
    tmp_path: Path, cogs: Dict[str, Path], r13: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(hexmod, "STRIP_ROWS", 7)
    small = tmp_path / "small.parquet"
    write_hex_table(cogs, small, 13, block_cells=997)
    assert pq.read_table(small).equals(pq.read_table(r13))


@pytest.fixture
def built(
    tmp_path: Path,
    canonical: CanonicalRequest,
    grid: Grid,
    provenance: Provenance,
    index_cogs: Dict[str, Path],
) -> CatalogResult:
    job_id = canonical.job_id()
    fid = fire_id(canonical.fire_event_name, job_id)
    return build_catalog(
        tmp_path / "work",
        fid,
        index_cogs,
        build_sidecar(canonical, job_id, grid, provenance, index_cogs),
        title="Geology_Fire burn severity",
        description="dNBR, RdNBR and RBR for Geology_Fire.",
        item_datetime=canonical.postfire_window[1],
        source_url="https://earth-search.aws.element84.com/v1/collections/sentinel-2-l2a",
        hex_tables=HexTables(fire_event_name="Geology_Fire", resolutions=(9, 12)),
    )


def test_the_tables_are_a_collection_and_rashid_passes(built: CatalogResult) -> None:
    assert built.warnings == 0
    fid = built.root.name
    catalog = json.loads((built.root / "catalog.json").read_text())
    children = {link["href"] for link in catalog["links"] if link["rel"] == "child"}
    assert children == {f"./{fid}-hex/collection.json", f"./{fid}/collection.json"}

    collection = json.loads(
        (built.root / collection_id(fid) / "collection.json").read_text()
    )
    assert collection["title"] == "Geology_Fire burn severity on H3 cells"
    assert "r9, r12" in collection["description"]
    assert "renders" not in collection
    assert collection["extent"]["temporal"]["interval"] == [
        ["2023-06-01T00:00:00Z", "2023-06-22T23:59:59Z"]
    ]
    dictionary = {c["name"]: c for c in collection["table:columns"]}
    assert all(c["description"] for c in dictionary.values())
    for resolution, parents in ((9, [8, 0]), (12, [10, 8, 0])):
        asset = collection["assets"][f"hex-r{resolution}"]
        assert asset["href"] == f"./{hex_file(resolution)}"
        assert asset["roles"] == ["data"]
        assert asset["h3:native_resolution"] == resolution
        assert asset["h3:parent_resolutions"] == parents
        assert f"resolution {resolution}" in asset["title"]
        # Each table carries its own row count and columns; the collection
        # only the dictionary of all of them.
        path = built.root / collection_id(fid) / hex_file(resolution)
        assert asset["table:row_count"] == pq.read_metadata(path).num_rows
        schema = [(f.name, str(f.type)) for f in pq.read_schema(path)]
        assert [(c["name"], c["type"]) for c in asset["table:columns"]] == schema
        assert {name for name, _ in schema} <= set(dictionary)
    assert "table:row_count" not in collection
    # Its README states the analysis's method, then the sampling.
    readme = (built.root / collection_id(fid) / "README.md").read_text()
    assert "newest processing baseline" in readme
    assert "pixel under its centre, found by projecting the centre to EPSG:" in readme

    # The analysis's own collection is as it was.
    analysis = json.loads((built.root / fid / "collection.json").read_text())
    assert not [key for key in analysis["assets"] if key.startswith("hex-")]
    assert set(analysis["renders"]) == {"dnbr", "rdnbr", "rbr"}
