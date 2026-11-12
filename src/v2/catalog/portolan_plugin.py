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

from src.v2.raster.indices import EXPRESSION_FORMAT, INDICES
from src.v2.catalog.sidecar import SIDECAR_ENV

ITEM_ID = "severity"
THUMBNAIL_FILE = "rbr.thumb.jpg"
THUMBNAIL_TITLE = "RBR, fixed 0 to 0.5 ramp"
BAND_UNIT = "dimensionless"

INDEX_BY_NAME = {index.name: index for index in INDICES}

Hook = TypeVar("Hook", bound=Callable[..., Any])


def hookimpl(function: Hook) -> Hook:
    """portolan's hook marker, typed (portolan_cli ships no type information)."""
    marked: Hook = portolan_hookimpl(function)
    return marked


def load_sidecar() -> Optional[Dict[str, Any]]:
    path = os.environ.get(SIDECAR_ENV)
    if not path:
        return None
    sidecar: Dict[str, Any] = json.loads(Path(path).read_text())
    return sidecar


@hookimpl
def portolan_enrich_item(item: Dict[str, Any], context: Any) -> None:
    """Add the fire properties, name each index asset, point at the RBR thumbnail."""
    sidecar = load_sidecar()
    if sidecar is None:
        return
    item["properties"].update(sidecar["properties"])
    extensions = item.setdefault("stac_extensions", [])
    for uri in sidecar.get("stac_extensions", []):
        if uri not in extensions:
            extensions.append(uri)
    assets = item.setdefault("assets", {})
    # portolan keys an item's primary file "data"; give every index its own key.
    primary = assets.get("data")
    if primary is not None:
        stem = Path(primary["href"]).stem
        if stem in INDEX_BY_NAME:
            assets[stem] = assets.pop("data")
    for name, index in INDEX_BY_NAME.items():
        asset = assets.get(name)
        if asset is None:
            continue
        asset["title"] = index.title
        asset["description"] = index.description
        asset["processing:expression"] = {
            "format": EXPRESSION_FORMAT,
            "expression": index.expression,
        }
        asset["bands"] = [
            dict(band, name=name, unit=BAND_UNIT) for band in sidecar["bands"][name]
        ]
    thumbnail = assets.get("thumbnail")
    if thumbnail is not None:
        thumbnail["href"] = f"./{THUMBNAIL_FILE}"
        thumbnail["title"] = THUMBNAIL_TITLE
    # portolan also tracks the pre-drawn thumbnail file as an asset of its own
    # ("rbr.thumb"); the item needs it once, as "thumbnail".
    for key in [
        key
        for key, asset in assets.items()
        if key != "thumbnail" and "thumbnail" in asset.get("roles", [])
    ]:
        del assets[key]


@hookimpl
def portolan_enrich_collection(collection: Dict[str, Any], context: Any) -> None:
    """Set the temporal extent to the windows and point at the RBR thumbnail."""
    sidecar = load_sidecar()
    if sidecar is None:
        return
    properties = sidecar["properties"]
    collection["extent"]["temporal"]["interval"] = [
        [properties["start_datetime"], properties["end_datetime"]]
    ]
    thumbnail = collection.get("assets", {}).get("thumbnail")
    if thumbnail is not None:
        thumbnail["href"] = f"./{ITEM_ID}/{THUMBNAIL_FILE}"
        thumbnail["title"] = THUMBNAIL_TITLE
