"""
Provenance for the satellite scenes an analysis was actually computed from.

Which STAC provider served an analysis is decided at run time: the provider
chain is tried in order and the first one with scenes in every window wins
(see ``StacEndpointHandler.search_windows``). Two runs of the same request can
therefore resolve to different providers and scene lists, so the choice is
recorded with the results rather than inferred from the request.

This module is the single definition of that record. The pipeline writes it
into each published item's properties as ``fire:source``,
``fire:prefire_scenes`` and ``fire:postfire_scenes`` (severity v2 design §5),
and the result record reports scene counts from it.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from pystac import Item

from src.stac.stac_endpoint_handler import StacMapping

PROPERTY_SOURCE = "fire:source"
PROPERTY_PREFIRE_SCENES = "fire:prefire_scenes"
PROPERTY_POSTFIRE_SCENES = "fire:postfire_scenes"


class Scene(BaseModel):
    """One source scene: enough to find it again."""

    id: str = Field(..., description="STAC item id")
    datetime: str = Field(..., description="Acquisition time, ISO 8601 in UTC")
    href: Optional[str] = Field(None, description="The STAC item's own URL")
    processing_baseline: Optional[str] = Field(
        None, description="Sentinel-2 processing baseline (s2:processing_baseline)"
    )
    offset: Optional[float] = Field(
        None,
        description="The reflectance offset this scene was converted with "
        "(reflectance = DN * scale + offset)",
    )

    @classmethod
    def from_item(cls, item: Item, offset: Optional[float] = None) -> "Scene":
        if item.datetime is None:
            raise ValueError(f"Item {item.id} has no datetime")
        baseline = item.properties.get("s2:processing_baseline")
        return cls(
            id=item.id,
            datetime=item.datetime.isoformat().replace("+00:00", "Z"),
            href=item.get_self_href(),
            processing_baseline=str(baseline) if baseline is not None else None,
            offset=offset,
        )


class SourceData(BaseModel):
    """Which provider, API, collection and bands served an analysis."""

    provider: str = Field(..., description="Human-readable provider name")
    provider_id: str = Field(
        ..., description="Stable provider identifier (StacProvider enum member name)"
    )
    api: str = Field(..., description="STAC API root URL")
    collection: str = Field(..., description="STAC collection the scenes came from")
    sensor: str = Field(
        ..., description="Sensor key used to resolve the provider chain"
    )
    bands: Dict[str, str] = Field(
        ...,
        description="Logical band name (nir, swir, mask) to the provider's asset key",
    )

    @classmethod
    def from_mapping(cls, provider: StacMapping, sensor: str) -> "SourceData":
        return cls(
            provider=provider.name,
            provider_id=provider.id.name,
            api=provider.url,
            collection=provider.collection,
            sensor=sensor,
            bands=provider.bands(),
        )


class Provenance(BaseModel):
    """The provider and every scene each window's composite was computed from."""

    source: SourceData
    prefire_scenes: List[Scene]
    postfire_scenes: List[Scene]

    def to_stac_properties(self) -> Dict[str, Any]:
        """Render as the STAC item properties fragment to merge in."""
        return {
            PROPERTY_SOURCE: self.source.model_dump(),
            PROPERTY_PREFIRE_SCENES: [s.model_dump() for s in self.prefire_scenes],
            PROPERTY_POSTFIRE_SCENES: [s.model_dump() for s in self.postfire_scenes],
        }
