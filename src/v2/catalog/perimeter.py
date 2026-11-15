"""The submitted perimeter as a collection in its analysis's catalog.

Only a request whose geometry_kind is "perimeter" has one, and the perimeter is
part of that request's job id, so the catalog stays a function of its key.

Layout, beside the analysis's own collection::

    {fire_id}/
      {fire_id}-perimeter/       collection.json, README.md, AGENTS.md,
                                 perimeter.parquet, perimeter.pmtiles,
                                 perimeter.thumb.jpg, styles/default.json

A single-file collection: the GeoParquet is a collection asset (``data``), and
``portolan add --pmtiles`` adds its vector tiles (``visual``, through
tippecanoe), a ``pmtiles`` link, a thumbnail and a default style. Its own
.portolan/metadata.yaml overrides the catalog's providers, keywords and notes;
portolan merges metadata from the catalog down, the collection's winning.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import geopandas as gpd
import yaml
from shapely.geometry import shape

PERIMETER_FILE = "perimeter.parquet"
# What portolan add --pmtiles names the tiles it makes from PERIMETER_FILE.
PMTILES_FILE = "perimeter.pmtiles"
# A fire_id ends in eight hex digits, so no analysis collection has this suffix.
COLLECTION_SUFFIX = "-perimeter"
FIRE_EVENT_NAME_COLUMN = "fire_event_name"
FIRE_ID_COLUMN = "fire_id"
WORDING_FILE = Path(__file__).resolve().parents[3] / "config" / "perimeter.yaml"


@dataclass(frozen=True)
class Perimeter:
    """A perimeter to publish: the request's name and its geometry in degrees."""

    fire_event_name: str
    geometry: Dict[str, Any]


def collection_id(fire_id: str) -> str:
    return f"{fire_id}{COLLECTION_SUFFIX}"


def is_perimeter_collection(collection_id: str) -> bool:
    return collection_id.endswith(COLLECTION_SUFFIX)


@lru_cache(maxsize=1)
def wording() -> Dict[str, Any]:
    """config/perimeter.yaml: what the catalog says about the perimeter."""
    loaded: Dict[str, Any] = yaml.safe_load(WORDING_FILE.read_text())
    return loaded


def one_line(text: str, **values: str) -> str:
    return " ".join(text.format(**values).split())


def column_descriptions() -> Dict[str, str]:
    return {name: one_line(text) for name, text in wording()["columns"].items()}


def write_perimeter_collection(
    catalog_root: Path, fire_id: str, perimeter: Perimeter
) -> Path:
    """Write the collection's GeoParquet and metadata; return its folder.

    The GeoParquet is one row in EPSG:4326, with the bbox covering column
    GeoParquet 1.1 describes, which portolan would otherwise add by rewriting
    the file.
    """
    words = wording()
    name = perimeter.fire_event_name
    folder = catalog_root / collection_id(fire_id)
    folder.mkdir(parents=True)
    frame = gpd.GeoDataFrame(
        {FIRE_ID_COLUMN: [fire_id], FIRE_EVENT_NAME_COLUMN: [name]},
        geometry=[shape(perimeter.geometry)],
        crs="EPSG:4326",
    )
    frame.to_parquet(
        folder / PERIMETER_FILE, compression="zstd", write_covering_bbox=True
    )
    metadata = {
        "title": one_line(words["title"], fire_event_name=name),
        "description": one_line(words["description"], fire_event_name=name),
        "providers": words["providers"],
        "keywords": words["keywords"],
        "source_url": "",
        "processing_notes": one_line(words["processing_notes"]),
    }
    (folder / ".portolan").mkdir()
    (folder / ".portolan" / "metadata.yaml").write_text(
        yaml.safe_dump(metadata, sort_keys=False)
    )
    return folder
