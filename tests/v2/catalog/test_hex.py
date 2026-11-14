"""The per-index H3 hex tables."""

from pathlib import Path
from typing import Dict

import h3
import numpy as np
import pyarrow.parquet as pq
import pytest
import rasterio

from src.v2.catalog.hex import (
    COLUMN_TYPES,
    HEX_PARENTS,
    column_dictionary,
    hex_file,
    write_hex_tables,
)

FIRE_ID = "geology-fire-c74d4092"


@pytest.fixture
def tables(tmp_path: Path, index_cogs: Dict[str, Path]) -> Dict[str, Path]:
    directory = tmp_path / "hex"
    directory.mkdir()
    return write_hex_tables(index_cogs, directory, FIRE_ID)


def test_schema_follows_the_catalog_convention(tables: Dict[str, Path]) -> None:
    assert sorted(path.name for path in tables.values()) == sorted(
        hex_file(name) for name in ("dnbr", "rdnbr", "rbr")
    )
    schema = pq.read_schema(tables["rbr"])
    assert [(f.name, str(f.type)) for f in schema] == [
        ("rbr", "float"),
        ("n_valid", "uint32"),
        ("h10", "uint64"),
        ("h9", "uint64"),
        ("h8", "uint64"),
        ("h0", "int64"),
        ("fire_id", "string"),
    ]


def test_every_valid_pixel_counts_once(
    tables: Dict[str, Path], index_cogs: Dict[str, Path]
) -> None:
    for name, path in tables.items():
        table = pq.read_table(path).to_pandas()
        with rasterio.open(index_cogs[name]) as src:
            values = src.read(1, masked=True).compressed().astype(np.float64)
        assert table["n_valid"].sum() == values.size
        # The pixel sum survives: mean x count over the cells.
        total = (table[name].astype(np.float64) * table["n_valid"]).sum()
        assert np.isclose(total, values.sum(), rtol=1e-6)
        assert table["h10"].is_unique
        assert (table["fire_id"] == FIRE_ID).all()


def test_parents_are_the_cells_parents(tables: Dict[str, Path]) -> None:
    table = pq.read_table(tables["dnbr"]).to_pandas().head(50)
    for row in table.itertuples():
        cell = h3.int_to_str(int(row.h10))
        assert h3.get_resolution(cell) == 10
        for p in HEX_PARENTS:
            assert int(getattr(row, f"h{p}")) == h3.str_to_int(
                h3.cell_to_parent(cell, p)
            )


def test_strips_merge_to_the_same_table(
    tmp_path: Path, index_cogs: Dict[str, Path], tables: Dict[str, Path]
) -> None:
    (tmp_path / "small").mkdir()
    small = write_hex_tables(index_cogs, tmp_path / "small", FIRE_ID, strip_rows=7)
    for name in tables:
        assert pq.read_table(small[name]).equals(pq.read_table(tables[name]))


def test_dictionary_covers_every_column(tables: Dict[str, Path]) -> None:
    dictionary = column_dictionary(
        {
            "dnbr": "Differenced NBR (dNBR)",
            "rdnbr": "Relativized dNBR (RdNBR)",
            "rbr": "RBR",
        }
    )
    described = {c["name"]: c for c in dictionary}
    for path in tables.values():
        for field in pq.read_schema(path):
            assert described[field.name]["description"]
    assert {name: described[name]["type"] for name in COLUMN_TYPES} == COLUMN_TYPES
