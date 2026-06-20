"""Tests for the sensor-aware STAC endpoint configuration and handler.

These tests exercise configuration parsing and the per-provider accessors only
(no network calls): the shipped config is loaded and inspected, the model is
validated directly, and the unknown-sensor / unknown-provider error paths are
covered with synthetic configs.
"""

import json

import pytest

from src.stac.stac_endpoint_handler import (
    DEFAULT_SENSOR,
    SensorConfig,
    StacEndpointHandler,
    StacMapping,
    StacProvider,
    StacProviderConfig,
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
            # Sentinel-2 L2A is already surface reflectance: no rescaling.
            assert provider.scale == 1.0
            assert provider.offset == 0.0

    def test_landsat_providers_use_landsat_collection_and_scaling(self) -> None:
        config = StacProviderConfig.load_from_file()
        providers = config.get_providers("landsat")
        assert len(providers) >= 1
        for provider in providers:
            assert provider.collection == "landsat-c2-l2"
            # USGS Collection-2 Level-2 surface-reflectance scaling.
            assert provider.scale == pytest.approx(0.0000275)
            assert provider.offset == pytest.approx(-0.2)
            # NBR uses NIR (B5, ~0.86um) and SWIR (B7, ~2.2um).
            assert provider.nir_band == "nir08"
            assert provider.swir_band == "swir22"

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
                            "epsg_code": 4326,
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

    def test_band_names_accessor(self) -> None:
        handler = StacEndpointHandler()
        landsat_provider = handler.config.get_providers("landsat")[0]
        nir, swir = handler.get_band_names(landsat_provider)
        assert nir == "nir08"
        assert swir == "swir22"

    def test_reflectance_scaling_sentinel2_is_noop(self) -> None:
        handler = StacEndpointHandler()
        provider = handler.config.get_providers("sentinel-2")[0]
        assert handler.get_reflectance_scaling(provider) == (1.0, 0.0)

    def test_reflectance_scaling_landsat(self) -> None:
        handler = StacEndpointHandler()
        provider = handler.config.get_providers("landsat")[0]
        scale, offset = handler.get_reflectance_scaling(provider)
        assert scale == pytest.approx(0.0000275)
        assert offset == pytest.approx(-0.2)

    def test_epsg_accessor(self) -> None:
        handler = StacEndpointHandler()
        provider = handler.config.get_providers("sentinel-2")[0]
        assert handler.get_epsg_code(provider) == 4326


class TestSensorConfigModel:
    """Direct validation of the pydantic models."""

    def test_stac_mapping_defaults_scale_offset(self) -> None:
        mapping = StacMapping(
            id=StacProvider.ELEMENT_84,
            name="Element 84",
            url="https://example.com",
            collection="sentinel-2-l2a",
            swir_band="swir22",
            nir_band="nir08",
            epsg_code=4326,
        )
        assert mapping.scale == 1.0
        assert mapping.offset == 0.0

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
                    epsg_code=4326,
                    scale=0.0000275,
                    offset=-0.2,
                )
            ]
        )
        assert len(sensor_config.providers) == 1
        assert sensor_config.providers[0].collection == "landsat-c2-l2"
