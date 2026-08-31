"""Tests for source-data provenance, the record of which scenes were used."""

from src.models.provenance import STAC_PROPERTY_KEY, SourceDataProvenance


def make_provenance(**overrides) -> SourceDataProvenance:
    defaults = dict(
        provider="Microsoft Planetary Computer",
        provider_id="MICROSOFT_PLANETARY_COMPUTER",
        collection="sentinel-2-l2a",
        sensor="sentinel-2",
        prefire_scene_count=2,
        postfire_scene_count=7,
    )
    defaults.update(overrides)
    return SourceDataProvenance(**defaults)


class TestStacPropertyRoundTrip:
    """Provenance survives the trip through STAC item properties.

    The API rebuilds its response from the stored STAC item rather than from
    the command result, so this round trip is the whole delivery path.
    """

    def test_round_trip_preserves_every_field(self) -> None:
        original = make_provenance()

        recovered = SourceDataProvenance.from_stac_properties(
            original.to_stac_properties()
        )

        assert recovered == original

    def test_nests_under_a_single_property_key(self) -> None:
        properties = make_provenance().to_stac_properties()

        assert list(properties) == [STAC_PROPERTY_KEY]

    def test_merges_alongside_other_item_properties(self) -> None:
        provenance = make_provenance()
        properties = {
            "fire_event_name": "Geology_Fire",
            "product_type": "fire_severity",
            **provenance.to_stac_properties(),
        }

        assert SourceDataProvenance.from_stac_properties(properties) == provenance
        assert properties["fire_event_name"] == "Geology_Fire"


class TestMissingOrUnusableProvenance:
    """Items predating provenance stay readable rather than erroring."""

    def test_properties_without_the_key_give_none(self) -> None:
        assert (
            SourceDataProvenance.from_stac_properties({"fire_event_name": "Geology"})
            is None
        )

    def test_empty_properties_give_none(self) -> None:
        assert SourceDataProvenance.from_stac_properties({}) is None

    def test_none_properties_give_none(self) -> None:
        assert SourceDataProvenance.from_stac_properties(None) is None

    def test_incomplete_fragment_gives_none(self) -> None:
        assert (
            SourceDataProvenance.from_stac_properties(
                {STAC_PROPERTY_KEY: {"provider": "Element 84"}}
            )
            is None
        )

    def test_non_object_fragment_gives_none(self) -> None:
        assert (
            SourceDataProvenance.from_stac_properties({STAC_PROPERTY_KEY: "Element 84"})
            is None
        )
