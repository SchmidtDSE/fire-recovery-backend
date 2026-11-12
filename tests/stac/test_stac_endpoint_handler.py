"""Tests for the sensor-aware STAC endpoint configuration and handler.

These tests exercise configuration parsing and the per-provider accessors only
(no network calls): the shipped config is loaded and inspected, the model is
validated directly, and the unknown-sensor / unknown-provider error paths are
covered with synthetic configs.

Searches run against a fake pystac-client, so no network is needed.
Reflectance scaling is tested with the v2 loader (tests/v2/test_load.py).
"""

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import pytest

from src.stac.stac_endpoint_handler import (
    DEFAULT_SENSOR,
    SensorConfig,
    StacEndpointHandler,
    StacMapping,
    StacProvider,
    StacProviderConfig,
    in_window,
)


class TestStacProviderConfigLoading:
    """Loading and parsing of the sensor-keyed provider configuration."""

    def test_shipped_config_exposes_both_sensors(self) -> None:
        config = StacProviderConfig.load_from_file()
        sensors = config.get_sensors()
        assert "sentinel-2" in sensors
        assert "landsat" in sensors

    def test_default_sensor_is_sentinel_2(self) -> None:
        assert DEFAULT_SENSOR == "sentinel-2"
        config = StacProviderConfig.load_from_file()
        # get_providers() defaults to the module default sensor.
        assert config.get_providers() == config.get_providers("sentinel-2")

    def test_sentinel2_providers_use_sentinel_collection(self) -> None:
        config = StacProviderConfig.load_from_file()
        providers = config.get_providers("sentinel-2")
        assert len(providers) == 2
        for provider in providers:
            assert provider.collection == "sentinel-2-l2a"

    def test_landsat_providers_use_landsat_collection_and_bands(self) -> None:
        config = StacProviderConfig.load_from_file()
        providers = config.get_providers("landsat")
        assert len(providers) >= 1
        for provider in providers:
            assert provider.collection == "landsat-c2-l2"
            # NBR uses NIR (B5, ~0.86um) and SWIR (B7, ~2.2um).
            assert provider.nir_band == "nir08"
            assert provider.swir_band == "swir22"
            assert provider.mask_band == "qa_pixel"

    def test_sentinel2_mask_bands_are_scl(self) -> None:
        config = StacProviderConfig.load_from_file()
        masks = {p.mask_band.lower() for p in config.get_providers("sentinel-2")}
        assert masks == {"scl"}

    def test_provider_id_parsed_to_enum(self) -> None:
        config = StacProviderConfig.load_from_file()
        ids = {p.id for p in config.get_providers("sentinel-2")}
        assert StacProvider.ELEMENT_84 in ids
        assert StacProvider.MICROSOFT_PLANETARY_COMPUTER in ids

    def test_unknown_sensor_raises_value_error(self) -> None:
        config = StacProviderConfig.load_from_file()
        with pytest.raises(ValueError, match="Unknown sensor"):
            config.get_providers("modis")

    def test_unknown_provider_id_raises_value_error(self, tmp_path) -> None:
        bad_config = {
            "sensors": {
                "sentinel-2": {
                    "providers": [
                        {
                            "id": "NOT_A_REAL_PROVIDER",
                            "name": "Bogus",
                            "url": "https://example.com",
                            "collection": "sentinel-2-l2a",
                            "swir_band": "B12",
                            "nir_band": "B8A",
                            "mask_band": "SCL",
                        }
                    ]
                }
            }
        }
        path = tmp_path / "bad_providers.json"
        path.write_text(json.dumps(bad_config))
        with pytest.raises(ValueError, match="Unknown provider"):
            StacProviderConfig.load_from_file(str(path))

    def test_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            StacProviderConfig.load_from_file("config/does_not_exist.json")


class TestStacEndpointHandler:
    """The handler wraps the config and exposes per-provider accessors."""

    def test_handler_exposes_both_sensors(self) -> None:
        handler = StacEndpointHandler()
        sensors = handler.get_sensors()
        assert "sentinel-2" in sensors
        assert "landsat" in sensors

    def test_bands_mapping(self) -> None:
        handler = StacEndpointHandler()
        landsat_provider = handler.config.get_providers("landsat")[0]
        assert landsat_provider.bands() == {
            "nir": "nir08",
            "swir": "swir22",
            "mask": "qa_pixel",
        }


class TestSensorConfigModel:
    """Direct validation of the pydantic models."""

    def test_stac_mapping_fields(self) -> None:
        mapping = StacMapping(
            id=StacProvider.ELEMENT_84,
            name="Element 84",
            url="https://example.com",
            collection="sentinel-2-l2a",
            swir_band="swir22",
            nir_band="nir08",
            mask_band="scl",
        )
        assert mapping.collection == "sentinel-2-l2a"
        assert mapping.id is StacProvider.ELEMENT_84

    def test_sensor_config_holds_providers(self) -> None:
        sensor_config = SensorConfig(
            providers=[
                StacMapping(
                    id=StacProvider.ELEMENT_84,
                    name="Element 84",
                    url="https://example.com",
                    collection="landsat-c2-l2",
                    swir_band="swir22",
                    nir_band="nir08",
                    mask_band="qa_pixel",
                )
            ]
        )
        assert len(sensor_config.providers) == 1
        assert sensor_config.providers[0].collection == "landsat-c2-l2"


def _items(*timestamps: str) -> List[SimpleNamespace]:
    return [
        SimpleNamespace(
            id=f"item-{ts}",
            datetime=datetime.fromisoformat(ts).replace(tzinfo=timezone.utc),
            properties={},
        )
        for ts in timestamps
    ]


class TestInWindow:
    """Window membership follows util.date_windows, which the loader shares."""

    def test_keeps_items_inside_and_sorts_them(self) -> None:
        items = _items(
            "2016-11-05T18:35:42", "2016-10-16T18:33:42", "2016-11-20T00:00:00"
        )
        inside = in_window(items, ["2016-10-15", "2016-11-05"])
        assert [i.id for i in inside] == [
            "item-2016-10-16T18:33:42",
            "item-2016-11-05T18:35:42",
        ]

    def test_end_date_covers_the_whole_final_day(self) -> None:
        """Mid-morning overpasses on the end date count as inside the window."""
        assert in_window(_items("2017-01-04T18:41:16"), ["2016-11-05", "2017-01-04"])

    def test_items_without_datetime_are_ignored(self) -> None:
        items = [SimpleNamespace(id="x", datetime=None)]
        assert in_window(items, ["2016-11-05", "2017-01-04"]) == []

    def test_window_offset_is_honoured_not_relabelled(self) -> None:
        """A window start carrying an offset means that instant, not UTC."""
        # The window opens at 07:00Z; this scene lands four hours earlier.
        items = _items("2023-06-15T03:00:00")
        assert in_window(items, ["2023-06-15T00:00:00-07:00", "2023-06-20"]) == []


class FakeClient:
    """A pystac-client stand-in: items per (API url, window start date)."""

    def __init__(self, url: str, catalog: Dict[str, Dict[str, List[str]]]):
        self.url = url
        self.catalog = catalog
        self.searches: List[Dict[str, Any]] = []

    def search(self, **kwargs: Any) -> SimpleNamespace:
        self.searches.append(kwargs)
        start = kwargs["datetime"][0].date().isoformat()
        stamps = self.catalog.get(self.url, {}).get(start, [])
        return SimpleNamespace(items=lambda: iter(_items(*stamps)))


PRE = ["2023-06-01", "2023-06-09"]
POST = ["2023-06-17", "2023-06-22"]
E84 = "https://earth-search.aws.element84.com/v1/"
MPC = "https://planetarycomputer.microsoft.com/api/stac/v1"


class TestSearchWindows:
    """Provider choice: the first in chain order with scenes in every window."""

    @staticmethod
    def _search(
        monkeypatch: pytest.MonkeyPatch,
        catalog: Dict[str, Dict[str, List[str]]],
        failing: Optional[str] = None,
    ) -> Any:
        clients: Dict[str, FakeClient] = {}

        def open_client(url: str) -> FakeClient:
            if url == failing:
                raise ConnectionError("provider down")
            clients[url] = FakeClient(url, catalog)
            return clients[url]

        monkeypatch.setattr(
            "src.stac.stac_endpoint_handler.PystacClient.open", open_client
        )
        result = StacEndpointHandler().search_windows(
            (-116.1, 33.8, -116.0, 33.9), [PRE, POST], "sentinel-2"
        )
        return result, clients

    def test_first_provider_covering_both_windows_wins(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        catalog = {
            E84: {
                "2023-06-01": ["2023-06-04T18:40:00"],
                "2023-06-17": ["2023-06-19T18:40:00"],
            },
            MPC: {
                "2023-06-01": ["2023-06-04T18:40:00"],
                "2023-06-17": ["2023-06-19T18:40:00"],
            },
        }
        result, clients = self._search(monkeypatch, catalog)
        assert result.provider.id is StacProvider.ELEMENT_84
        assert [len(w) for w in result.windows] == [1, 1]
        assert MPC not in clients

    def test_each_window_is_searched_separately(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        catalog = {
            E84: {
                "2023-06-01": ["2023-06-04T18:40:00"],
                "2023-06-17": ["2023-06-19T18:40:00"],
            }
        }
        _, clients = self._search(monkeypatch, catalog)
        searches = clients[E84].searches
        assert len(searches) == 2
        starts = [s["datetime"][0].isoformat() for s in searches]
        ends = [s["datetime"][1].isoformat() for s in searches]
        assert starts == ["2023-06-01T00:00:00+00:00", "2023-06-17T00:00:00+00:00"]
        assert ends == ["2023-06-10T00:00:00+00:00", "2023-06-23T00:00:00+00:00"]
        assert searches[0]["collections"] == ["sentinel-2-l2a"]

    def test_falls_back_when_a_window_is_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        catalog = {
            E84: {"2023-06-01": ["2023-06-04T18:40:00"]},
            MPC: {
                "2023-06-01": ["2023-06-04T18:40:00"],
                "2023-06-17": ["2023-06-19T18:40:00"],
            },
        }
        result, _ = self._search(monkeypatch, catalog)
        assert result.provider.id is StacProvider.MICROSOFT_PLANETARY_COMPUTER

    def test_falls_back_when_a_provider_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        catalog = {
            MPC: {
                "2023-06-01": ["2023-06-04T18:40:00"],
                "2023-06-17": ["2023-06-19T18:40:00"],
            }
        }
        result, _ = self._search(monkeypatch, catalog, failing=E84)
        assert result.provider.id is StacProvider.MICROSOFT_PLANETARY_COMPUTER

    def test_raises_when_no_provider_covers_every_window(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        catalog = {E84: {"2023-06-01": ["2023-06-04T18:40:00"]}}
        with pytest.raises(RuntimeError, match="every window"):
            self._search(monkeypatch, catalog)


class TestSignForRead:
    def test_only_planetary_computer_items_are_signed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "src.stac.stac_endpoint_handler.planetary_computer.sign",
            lambda item: ("signed", item),
        )
        config = StacProviderConfig.load_from_file()
        e84, mpc = config.get_providers("sentinel-2")
        items: List[Any] = ["a", "b"]
        assert StacEndpointHandler.sign_for_read(e84, items) == ["a", "b"]
        assert StacEndpointHandler.sign_for_read(mpc, items) == [
            ("signed", "a"),
            ("signed", "b"),
        ]
