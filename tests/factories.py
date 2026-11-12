"""Shared constructors for test doubles that need to be structurally real."""

from src.stac.stac_endpoint_handler import StacMapping, StacProvider


def make_stac_mapping(
    provider: StacProvider = StacProvider.ELEMENT_84,
    collection: str = "sentinel-2-l2a",
) -> StacMapping:
    """
    A real provider mapping for tests that stub out ``search_items``.

    Prefer this over ``Mock()``: the fire severity command reads ``.name`` and
    ``.id.name`` off the mapping to record provenance, and ``name`` is reserved
    by Mock's constructor, so a bare Mock silently yields a non-string there.
    """
    return StacMapping(
        id=provider,
        name=provider.value,
        url="https://example.com/stac/v1",
        collection=collection,
        swir_band="B12",
        nir_band="B08",
        epsg_code=4326,
    )
