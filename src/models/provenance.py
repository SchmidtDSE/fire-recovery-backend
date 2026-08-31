"""
Provenance for the satellite scenes an analysis was actually computed from.

Which STAC provider served an analysis is decided at run time: the provider
chain is tried in order and the first one holding scenes in every required
window wins (see ``StacEndpointHandler.search_items``). Two runs of the same
request can therefore legitimately resolve to different providers and different
scene counts, so the choice has to be recorded alongside the results rather than
inferred from the request.

This module is the single source of truth for that record. The same model is
written into STAC item properties and returned from the API, so the field names
are defined exactly once.
"""

from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Key under which the provenance object is nested in STAC item properties.
STAC_PROPERTY_KEY = "source_data"


class SourceDataProvenance(BaseModel):
    """Which provider and how many scenes an analysis was computed from."""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "provider": "Microsoft Planetary Computer",
                "provider_id": "MICROSOFT_PLANETARY_COMPUTER",
                "collection": "sentinel-2-l2a",
                "sensor": "sentinel-2",
                "prefire_scene_count": 2,
                "postfire_scene_count": 7,
            }
        }
    )

    provider: str = Field(
        ..., description="Human-readable name of the STAC provider that served the data"
    )
    provider_id: str = Field(
        ..., description="Stable provider identifier (StacProvider enum member name)"
    )
    collection: str = Field(..., description="STAC collection the scenes came from")
    sensor: str = Field(
        ..., description="Sensor key used to resolve the provider chain"
    )
    prefire_scene_count: int = Field(
        ..., description="Scenes contributing to the prefire composite"
    )
    postfire_scene_count: int = Field(
        ..., description="Scenes contributing to the postfire composite"
    )

    def to_stac_properties(self) -> Dict[str, Any]:
        """Render as the STAC item properties fragment to merge in."""
        return {STAC_PROPERTY_KEY: self.model_dump()}

    @classmethod
    def from_stac_properties(
        cls, properties: Optional[Dict[str, Any]]
    ) -> Optional["SourceDataProvenance"]:
        """
        Recover provenance from STAC item properties, or None if absent.

        Items written before provenance was recorded simply lack the key, and
        the API keeps serving them, so a missing or malformed fragment is a
        normal outcome rather than an error.
        """
        raw = (properties or {}).get(STAC_PROPERTY_KEY)
        if not isinstance(raw, dict):
            return None

        try:
            return cls.model_validate(raw)
        except ValidationError:
            return None
