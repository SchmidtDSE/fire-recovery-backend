#!/usr/bin/env python3
"""
Generate the committed v2 job id vectors.

Each vector is a raw POST /v2/severity body with the canonical form and job id
it must produce. ``same_as`` and ``differs_from`` name another vector whose id
must match or differ; the generator refuses to write a file that breaks them.

Usage:
    pixi run python -m scripts.generate_v2_job_id_vectors

Regenerating changes the committed ids only when validation, normalization,
buffering, snapping or canonicalization changed. That re-keys every analysis,
so it needs a PIPELINE_VERSION bump or a very good reason.
"""

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.v2.constants import PIPELINE_VERSION
from src.v2.identity.job_id import canonical_request
from src.v2.identity.request import SeverityRequest

OUT = Path(__file__).parent.parent / "tests" / "fixtures" / "v2_job_id_vectors.json"

# A ~7 km box in Joshua Tree, away from any z15 tile edge.
BASE_RING = [
    [-116.098276, 33.929925],
    [-116.098276, 33.880794],
    [-116.019318, 33.880794],
    [-116.019318, 33.929925],
    [-116.098276, 33.929925],
]
BASE: Dict[str, Any] = {
    "fire_event_name": "Geology_Fire",
    "geometry": {"type": "Polygon", "coordinates": [BASE_RING]},
    "prefire_window": ["2023-06-01", "2023-06-09"],
    "postfire_window": ["2023-06-17", "2023-06-22"],
}


def box(west: float, south: float, east: float, north: float) -> Dict[str, Any]:
    ring = [[west, south], [east, south], [east, north], [west, north], [west, south]]
    return {"type": "Polygon", "coordinates": [ring]}


def shifted(ring: List[List[float]], dlon: float, dlat: float) -> List[List[float]]:
    return [[lon + dlon, lat + dlat] for lon, lat in ring]


def variant(**changes: Any) -> Dict[str, Any]:
    body = copy.deepcopy(BASE)
    body.update(changes)
    return body


def vectors() -> List[Dict[str, Any]]:
    """(name, body, relation) triples, as dicts."""
    out: List[Dict[str, Any]] = []

    def add(
        name: str,
        body: Dict[str, Any],
        same_as: Optional[str] = None,
        differs_from: Optional[str] = None,
    ) -> None:
        entry: Dict[str, Any] = {"name": name, "request_body": body}
        if same_as:
            entry["same_as"] = same_as
        if differs_from:
            entry["differs_from"] = differs_from
        out.append(entry)

    add("base_polygon", copy.deepcopy(BASE))
    add(
        "feature_with_properties_id_bbox",
        variant(
            geometry={
                "type": "Feature",
                "id": "perimeter-1",
                "bbox": [-116.1, 33.88, -116.01, 33.93],
                "properties": {"source": "drawn"},
                "geometry": {"type": "Polygon", "coordinates": [BASE_RING]},
            }
        ),
        same_as="base_polygon",
    )
    add(
        "multipolygon_of_same_ring",
        variant(geometry={"type": "MultiPolygon", "coordinates": [[BASE_RING]]}),
        same_as="base_polygon",
    )
    add(
        "clockwise_ring",
        variant(geometry={"type": "Polygon", "coordinates": [BASE_RING[::-1]]}),
        same_as="base_polygon",
    )
    add(
        "z_coordinates",
        variant(
            geometry={
                "type": "Polygon",
                "coordinates": [[[lon, lat, 912.5] for lon, lat in BASE_RING]],
            }
        ),
        same_as="base_polygon",
    )
    add("sensor_explicit_default", variant(sensor="sentinel-2"), same_as="base_polygon")
    add(
        "numeric_string_coordinates",
        variant(
            geometry={
                "type": "Polygon",
                "coordinates": [[[str(lon), str(lat)] for lon, lat in BASE_RING]],
            }
        ),
        same_as="base_polygon",
    )
    add("unknown_extra_key", variant(note="ignored"), same_as="base_polygon")
    add(
        "below_microdegree_difference",
        variant(
            geometry={"type": "Polygon", "coordinates": [shifted(BASE_RING, 4e-7, 0)]}
        ),
        same_as="base_polygon",
    )
    add(
        "small_shift_same_tiles",
        variant(
            geometry={
                "type": "Polygon",
                "coordinates": [shifted(BASE_RING, 0.00005, -0.00004)],
            }
        ),
        same_as="base_polygon",
    )
    add("landsat", variant(sensor="landsat"), differs_from="base_polygon")
    add(
        "other_fire_event_name",
        variant(fire_event_name="Geology_Fire_2"),
        differs_from="base_polygon",
    )
    add(
        "other_prefire_window",
        variant(prefire_window=["2023-05-25", "2023-06-09"]),
        differs_from="base_polygon",
    )
    add(
        "datetime_string_for_same_date",
        variant(prefire_window=["2023-06-01T00:00:00", "2023-06-09"]),
        differs_from="base_polygon",
    )
    add(
        "shift_across_tile_edges",
        variant(
            geometry={"type": "Polygon", "coordinates": [shifted(BASE_RING, 0.03, 0)]}
        ),
        differs_from="base_polygon",
    )
    add(
        "near_zero_coordinates",
        variant(geometry=box(-0.004, -0.003, 0.002, 0.004)),
    )
    add(
        "southern_hemisphere",
        variant(geometry=box(148.95, -35.42, 149.08, -35.31)),
    )
    add(
        "straddles_utm_zones_10_and_11",
        variant(geometry=box(-120.06, 36.95, -119.94, 37.04)),
    )
    add(
        "about_100_km_box",
        variant(geometry=box(-121.2, 39.4, -120.05, 40.3)),
    )
    return out


def generate() -> List[Dict[str, Any]]:
    entries = []
    ids: Dict[str, str] = {}
    for entry in vectors():
        request = SeverityRequest.model_validate(entry["request_body"])
        canonical = canonical_request(request)
        ids[entry["name"]] = canonical.job_id()
        entries.append(
            {
                **entry,
                "canonical": json.loads(canonical.to_json()),
                "job_id": canonical.job_id(),
            }
        )
    for entry in entries:
        if "same_as" in entry and ids[entry["name"]] != ids[entry["same_as"]]:
            raise SystemExit(f"{entry['name']} must share {entry['same_as']}'s id")
        if "differs_from" in entry and ids[entry["name"]] == ids[entry["differs_from"]]:
            raise SystemExit(
                f"{entry['name']} must differ from {entry['differs_from']}"
            )
    return entries


def main() -> None:
    entries = generate()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(
            {"pipeline_version": PIPELINE_VERSION, "vectors": entries}, f, indent=1
        )
        f.write("\n")
    print(f"Wrote {len(entries)} vectors to {OUT}")


if __name__ == "__main__":
    main()
