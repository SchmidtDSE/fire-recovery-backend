from typing import Any, Dict, List, Optional, Tuple
from pystac import ItemCollection
from pystac_client import Client as PystacClient
import planetary_computer
from enum import Enum
from pydantic import BaseModel
import json
import logging
from geojson_pydantic import Polygon, MultiPolygon, Feature


# Default sensor used when a caller does not specify one. Kept as a module
# constant so the request model, router, and command can share a single source
# of truth for the fallback.
DEFAULT_SENSOR = "sentinel-2"


class StacProvider(Enum):
    """Enum for STAC providers."""

    MICROSOFT_PLANETARY_COMPUTER = "Microsoft Planetary Computer"
    ELEMENT_84 = "Element 84"


class StacMapping(BaseModel):
    """
    Model for a single (sensor, provider) STAC mapping.

    The collection ID and band names are properties of the *(sensor, provider)*
    pair, not of the provider alone, so they live together on this model. The
    reflectance ``scale``/``offset`` default to a no-op; sensors whose surface
    reflectance is stored as scaled integers (e.g. Landsat Collection-2 Level-2)
    override them so values become comparable across sensors.
    """

    id: StacProvider
    name: str
    url: str
    collection: str
    swir_band: str
    nir_band: str
    epsg_code: int
    scale: float = 1.0
    offset: float = 0.0


class SensorConfig(BaseModel):
    """Ordered fallback chain of provider mappings for a single sensor."""

    providers: List[StacMapping]


class StacProviderConfig(BaseModel):
    """Top-level configuration: a set of sensors, each with its own providers."""

    sensors: Dict[str, SensorConfig]

    @classmethod
    def load_from_file(
        cls, filepath: str = "config/stac_providers.json"
    ) -> "StacProviderConfig":
        """Load configuration from a JSON file."""
        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            # Convert the string representation of the provider enum to the
            # actual enum for every provider under every sensor.
            for sensor_cfg in data.get("sensors", {}).values():
                for provider in sensor_cfg.get("providers", []):
                    if isinstance(provider.get("id"), str):
                        try:
                            provider["id"] = StacProvider[provider["id"]]
                        except KeyError:
                            raise ValueError(f"Unknown provider: {provider['id']}")

            return cls.model_validate(data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {filepath} not found.")

    def get_sensors(self) -> List[str]:
        """
        Get the configured sensor keys.

        Returns:
            List of sensor names (e.g. ["sentinel-2", "landsat"]).
        """
        return list(self.sensors.keys())

    def get_providers(self, sensor: str = DEFAULT_SENSOR) -> List[StacMapping]:
        """
        Get the ordered providers for a given sensor.

        Args:
            sensor: Sensor key selecting which provider chain to use.

        Returns:
            Ordered list of provider configurations for the sensor.

        Raises:
            ValueError: If the sensor is not configured.
        """
        if sensor not in self.sensors:
            raise ValueError(
                f"Unknown sensor '{sensor}'. Available sensors: "
                f"{sorted(self.sensors.keys())}"
            )
        return self.sensors[sensor].providers


class StacEndpointHandler:
    """
    Handles interactions with STAC endpoints with fallback support.

    Tries multiple STAC endpoints in order of priority when data is not
    available. Providers, their collection IDs, band names, and reflectance
    scaling are all resolved per sensor from configuration.
    """

    def __init__(
        self,
        stac_provider_json_path: str = "config/stac_providers.json",
    ):
        """
        Initialize the STAC endpoint handler.

        Args:
            stac_provider_json_path: Path to the STAC provider configuration JSON file.
        """
        self.config = StacProviderConfig.load_from_file(stac_provider_json_path)
        self.logger = logging.getLogger(__name__)

    def get_sensors(self) -> List[str]:
        """Get the configured sensor keys."""
        return self.config.get_sensors()

    async def get_client(
        self, provider: StacMapping
    ) -> Tuple[PystacClient, StacMapping]:
        """
        Get a STAC client with authentication for the given provider.

        Args:
            provider: The StacMapping provider configuration

        Returns:
            Tuple of (STAC client, provider mapping)
        """
        self.logger.info(f"Using STAC endpoint: {provider.name}")

        if provider.id == StacProvider.MICROSOFT_PLANETARY_COMPUTER:
            client = PystacClient.open(
                provider.url, modifier=planetary_computer.sign_inplace
            )
        else:
            client = PystacClient.open(provider.url)

        return client, provider

    async def search_items(
        self,
        geometry: Polygon | MultiPolygon | Feature | Dict[str, Any],
        date_range: List[str],
        sensor: str = DEFAULT_SENSOR,
        provider_index: Optional[int] = None,
    ) -> Tuple[ItemCollection, StacMapping]:
        """
        Search for items using the STAC providers configured for a sensor.

        Args:
            geometry: GeoJSON geometry to search within (Polygon, MultiPolygon, Feature, or dict)
            date_range: List of [start_date, end_date] as strings
            sensor: Sensor key selecting the provider chain and collection to query
            provider_index: Index of provider to use. If None, tries all in order.

        Returns:
            Tuple of (items, provider)

        Raises:
            ValueError: If the sensor is unknown or provider_index is out of range
            RuntimeError: If no items found at any provider
        """
        providers = self.config.get_providers(sensor)

        base_params: Dict[str, Any] = {
            "intersects": geometry,
            "datetime": "/".join(date_range),
        }

        if provider_index is not None:
            # Try specific provider
            if provider_index < 0 or provider_index >= len(providers):
                raise ValueError(f"Provider index {provider_index} out of range")

            provider = providers[provider_index]
            client, provider = await self.get_client(provider)

            items = client.search(
                **base_params, collections=[provider.collection]
            ).item_collection()
            if len(items) > 0:
                return items, provider
            else:
                raise RuntimeError(f"No items found using provider {provider.name}")
        else:
            # Try all providers in order (per-sensor fallback chain)
            for i, provider in enumerate(providers):
                try:
                    self.logger.info(
                        f"Trying STAC provider {i}: {provider.name} "
                        f"(sensor: {sensor}, collection: {provider.collection})"
                    )
                    client, provider = await self.get_client(provider)

                    items = client.search(
                        **base_params, collections=[provider.collection]
                    ).item_collection()
                    if len(items) > 0:
                        return items, provider
                except Exception as e:
                    self.logger.warning(
                        f"Failed to get items with provider {provider.name}: {str(e)}"
                    )
                    continue

            raise RuntimeError(
                f"No items found with any available STAC provider for sensor '{sensor}'"
            )

    def get_band_names(self, provider: StacMapping) -> Tuple[str, str]:
        """
        Get the NIR and SWIR band names for the given provider.

        Args:
            provider: The provider configuration

        Returns:
            Tuple of (nir_band, swir_band)
        """
        return provider.nir_band, provider.swir_band

    def get_epsg_code(self, provider: StacMapping) -> int:
        """
        Get the EPSG code for the given provider.

        Args:
            provider: The provider configuration

        Returns:
            EPSG code as integer
        """
        return provider.epsg_code

    def get_reflectance_scaling(self, provider: StacMapping) -> Tuple[float, float]:
        """
        Get the (scale, offset) needed to convert stored values to reflectance.

        Surface-reflectance products are sometimes stored as scaled integers
        (``reflectance = value * scale + offset``). Sentinel-2 L2A is already
        reflectance, so its providers use the default no-op ``(1.0, 0.0)``.
        Landsat Collection-2 Level-2 uses ``scale=0.0000275``, ``offset=-0.2``.

        Args:
            provider: The provider configuration

        Returns:
            Tuple of (scale, offset)
        """
        return provider.scale, provider.offset
