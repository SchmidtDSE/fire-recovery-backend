"""portolan enrich plugin: copy the analysis's sidecar into the STAC it builds.

portolan calls these hooks every time ``add`` or a fix writes an item or a
collection. Each write is a plain assignment, so running them again changes
nothing. With no sidecar named in the environment the hooks do nothing, so the
plugin is inert for any other catalog. A submitted perimeter's collection gets
only its column descriptions (perimeter.py); the hex collection, its tables'
metadata and the windows (hex.py).
"""

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

from portolan_cli.hookspecs import hookimpl as portolan_hookimpl

from src.v2.raster.indices import EXPRESSION_FORMAT, INDICES, Index
from src.v2.catalog.hex import (
    asset_fields,
    asset_resolution,
    column_dictionary,
    is_hex_collection,
    table_columns,
)
from src.v2.catalog.perimeter import column_descriptions, is_perimeter_collection
from src.v2.catalog.sidecar import SIDECAR_ENV

# The index whose thumbnail stands for the whole collection, and the copy of
# it in the collection folder. portolan adopts a collection's own
# ``thumbnail.*`` over any item's thumbnail.
COLLECTION_THUMBNAIL_INDEX = "rbr"
COLLECTION_THUMBNAIL_FILE = "thumbnail.jpg"
BAND_UNIT = "dimensionless"

# How a map client draws each index: the render extension's collection-level
# ``renders``, one entry per index, keyed to the index's asset. Render v2 has
# no asset-level fields; portolan-cli writes undeclared asset-level
# ``render:*`` fields from one catalog-wide setting, which the build points
# at the same colormap so the two never disagree.
RENDER_EXTENSION = "https://stac-extensions.github.io/render/v2.0.0/schema.json"
# TiTiler's name for matplotlib's YlOrRd, the thumbnails' ramp.
RENDER_COLORMAP = "ylorrd"

INDEX_BY_NAME = {index.name: index for index in INDICES}

Hook = TypeVar("Hook", bound=Callable[..., Any])


def hookimpl(function: Hook) -> Hook:
    """portolan's hook marker, typed (portolan_cli ships no type information)."""
    marked: Hook = portolan_hookimpl(function)
    return marked


def thumbnail_file(index: str) -> str:
    """The thumbnail drawn beside an index's COG, named as portolan names one."""
    return f"{index}.thumb.jpg"


def thumbnail_title(index: Index) -> str:
    low, high = index.display_range
    return f"{index.title}, fixed {low:g} to {high:g} ramp"


def renders() -> Dict[str, Dict[str, Any]]:
    """One render per index: its asset, a fixed rescale, the shared colormap.

    The rescale is the index's display_range, the thumbnail's stretch: a
    display choice, not class breaks.
    """
    return {
        index.name: {
            "title": index.title,
            "assets": [index.name],
            "rescale": [list(index.display_range)],
            "colormap_name": RENDER_COLORMAP,
        }
        for index in INDICES
    }


def add_hex_metadata(collection: Dict[str, Any]) -> None:
    """Title, describe and declare the H3 resolutions of each table.

    portolan registers the tables as collection assets (``tabular.enabled``)
    before this hook runs; this adds what portolan can't know, and the one
    column dictionary every table's columns are in.

    portolan writes the first table's columns and row count on the collection
    and each later table's on its asset, so the collection's row count would
    be one table's beside a dictionary of all of them. The hook gives every
    table its own on its asset and leaves the collection the dictionary
    alone. The table extension allows either place (rashid PTL-DAT-015).
    """
    assets = collection.get("assets", {})
    tables = {
        key: resolution
        for key in assets
        if (resolution := asset_resolution(key)) is not None
    }
    if not tables:
        return
    collection_rows = collection.pop("table:row_count", None)
    for key, resolution in tables.items():
        asset = assets[key]
        asset.update(asset_fields(resolution))
        asset["table:columns"] = table_columns(resolution)
        if "table:row_count" not in asset and collection_rows is not None:
            asset["table:row_count"] = collection_rows
    collection["table:columns"] = column_dictionary(sorted(tables.values()))


def load_sidecar() -> Optional[Dict[str, Any]]:
    path = os.environ.get(SIDECAR_ENV)
    if not path:
        return None
    sidecar: Dict[str, Any] = json.loads(Path(path).read_text())
    return sidecar


def add_fire_properties(item: Dict[str, Any], sidecar: Dict[str, Any]) -> None:
    """The analysis's properties (windows, scenes, grid, method) and extensions."""
    item["properties"].update(sidecar["properties"])
    extensions = item.setdefault("stac_extensions", [])
    for uri in sidecar.get("stac_extensions", []):
        if uri not in extensions:
            extensions.append(uri)


def title_item(item: Dict[str, Any], index: Index) -> None:
    item["properties"]["title"] = index.title
    item["properties"]["description"] = index.description


def key_cog_by_index(assets: Dict[str, Any], index: Index) -> None:
    """portolan keys an item's primary file "data"; key it by the index instead."""
    if "data" in assets:
        assets[index.name] = assets.pop("data")


def describe_cog(asset: Dict[str, Any], index: Index, sidecar: Dict[str, Any]) -> None:
    """The COG asset's title, description, formula and band table."""
    asset["title"] = index.title
    asset["description"] = index.description
    asset["processing:expression"] = {
        "format": EXPRESSION_FORMAT,
        "expression": index.expression,
    }
    asset["bands"] = [
        dict(band, name=index.name, unit=BAND_UNIT)
        for band in sidecar["bands"][index.name]
    ]


def title_thumbnail(asset: Dict[str, Any], index: Index) -> None:
    asset["title"] = thumbnail_title(index)


def describe_perimeter_columns(collection: Dict[str, Any]) -> None:
    """The perimeter GeoParquet's column descriptions (config/perimeter.yaml)."""
    descriptions = column_descriptions()
    for column in collection.get("table:columns", []):
        if column.get("name") in descriptions:
            column["description"] = descriptions[column["name"]]


def set_temporal_extent(collection: Dict[str, Any], sidecar: Dict[str, Any]) -> None:
    """The collection's temporal extent: the prefire start to the postfire end."""
    properties = sidecar["properties"]
    collection["extent"]["temporal"]["interval"] = [
        [properties["start_datetime"], properties["end_datetime"]]
    ]


def add_renders(collection: Dict[str, Any]) -> None:
    """Each index's render (render extension v2)."""
    collection["renders"] = renders()
    extensions = collection.setdefault("stac_extensions", [])
    if RENDER_EXTENSION not in extensions:
        extensions.append(RENDER_EXTENSION)


@hookimpl
def portolan_enrich_item(item: Dict[str, Any], context: Any) -> None:
    """Title the item after its index; add the fire properties and the method.

    Each item holds one index's COG and thumbnail, in a folder named after
    the index, so the item id is the index name.
    """
    sidecar = load_sidecar()
    if sidecar is None:
        return
    index = INDEX_BY_NAME.get(item.get("id", ""))
    if index is None:
        return
    add_fire_properties(item, sidecar)
    title_item(item, index)
    assets = item.setdefault("assets", {})
    key_cog_by_index(assets, index)
    if index.name in assets:
        describe_cog(assets[index.name], index, sidecar)
    if "thumbnail" in assets:
        title_thumbnail(assets["thumbnail"], index)


@hookimpl
def portolan_enrich_collection(collection: Dict[str, Any], context: Any) -> None:
    """Set the temporal extent to the windows; add each index's render.

    The perimeter's collection gets its column descriptions and nothing else;
    the hex collection, the windows and its tables' metadata.
    portolan registers the collection thumbnail after this hook runs, by
    adopting COLLECTION_THUMBNAIL_FILE, so the hook never sees it.
    """
    sidecar = load_sidecar()
    if sidecar is None:
        return
    if is_perimeter_collection(collection.get("id", "")):
        describe_perimeter_columns(collection)
        return
    set_temporal_extent(collection, sidecar)
    if is_hex_collection(collection.get("id", "")):
        add_hex_metadata(collection)
        return
    add_renders(collection)
