"""portolan enrich plugin: copy the analysis's sidecar into the STAC it builds.

portolan calls these hooks every time ``add`` or a fix writes an item or a
collection. Each write is a plain assignment, so running them again changes
nothing. With no sidecar named in the environment the hooks do nothing, so the
plugin is inert for any other catalog.
"""

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar

from portolan_cli.hookspecs import hookimpl as portolan_hookimpl

from src.v2.raster.indices import EXPRESSION_FORMAT, INDICES, Index
from src.v2.catalog.sidecar import SIDECAR_ENV

# The index whose thumbnail stands for the whole collection, and the copy of
# it in the collection folder. portolan adopts a collection's own
# ``thumbnail.*`` over any item's thumbnail.
COLLECTION_THUMBNAIL_INDEX = "rbr"
COLLECTION_THUMBNAIL_FILE = "thumbnail.jpg"
BAND_UNIT = "dimensionless"

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


def load_sidecar() -> Optional[Dict[str, Any]]:
    path = os.environ.get(SIDECAR_ENV)
    if not path:
        return None
    sidecar: Dict[str, Any] = json.loads(Path(path).read_text())
    return sidecar


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
    properties = item["properties"]
    properties.update(sidecar["properties"])
    properties["title"] = index.title
    properties["description"] = index.description
    extensions = item.setdefault("stac_extensions", [])
    for uri in sidecar.get("stac_extensions", []):
        if uri not in extensions:
            extensions.append(uri)
    assets = item.setdefault("assets", {})
    # portolan keys an item's primary file "data"; key it by the index instead.
    if "data" in assets:
        assets[index.name] = assets.pop("data")
    asset = assets.get(index.name)
    if asset is not None:
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
    thumbnail = assets.get("thumbnail")
    if thumbnail is not None:
        thumbnail["title"] = thumbnail_title(index)


@hookimpl
def portolan_enrich_collection(collection: Dict[str, Any], context: Any) -> None:
    """Set the temporal extent to the prefire and postfire windows.

    portolan registers the collection thumbnail after this hook runs, by
    adopting COLLECTION_THUMBNAIL_FILE, so the hook never sees it.
    """
    sidecar = load_sidecar()
    if sidecar is None:
        return
    properties = sidecar["properties"]
    collection["extent"]["temporal"]["interval"] = [
        [properties["start_datetime"], properties["end_datetime"]]
    ]
