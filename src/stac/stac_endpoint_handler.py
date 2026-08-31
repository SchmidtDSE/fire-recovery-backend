from typing import Any, Dict, List, Optional, Tuple
from pystac import ItemCollection
from pystac_client import Client as PystacClient
import planetary_computer
from enum import Enum
from pydantic import BaseModel
import json
import logging
from geojson_pydantic import Polygon, MultiPolygon, Feature

from src.util.date_windows import window_bounds


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
    pair, not of the provider alone, so they live together on this model.

    Reflectance scaling is intentionally NOT modeled here. stackstac applies the
    per-asset ``scale``/``offset`` from each item's ``raster:bands`` metadata at
    stack time (``rescale=True``), so reflectance correction — including
    Landsat C2 L2's additive offset — comes from the providers' published
    metadata rather than hardcoded per-sensor constants.
    """

    id: StacProvider
    name: str
    url: str
    collection: str
    swir_band: str
    nir_band: str
    epsg_code: int


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
    available. Providers, their collection IDs, and band names are resolved
    per sensor from configuration. Reflectance scaling is left to stackstac
    (via each item's ``raster:bands`` metadata at stack time).
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
        required_windows: Optional[List[List[str]]] = None,
    ) -> Tuple[ItemCollection, StacMapping]:
        """
        Search for items using the STAC providers configured for a sensor.

        Args:
            geometry: GeoJSON geometry to search within (Polygon, MultiPolygon, Feature, or dict)
            date_range: List of [start_date, end_date] as strings
            sensor: Sensor key selecting the provider chain and collection to query
            provider_index: Index of provider to use. If None, tries all in order.
            required_windows: Optional list of [start_date, end_date] sub-ranges
                that must *each* contain at least one item for a provider to be
                accepted. Without it, the chain stops at the first provider
                returning any item at all, which on small AOIs can be a lone
                scene sitting outside the caller's windows while a later
                provider covers them all. Ignored when provider_index is given.

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
            # Best non-covering result seen so far, kept so a provider chain in
            # which nobody covers every window still returns its richest result
            # (and a meaningful error downstream) rather than nothing at all.
            fallback: Optional[Tuple[ItemCollection, StacMapping]] = None

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
                    if len(items) == 0:
                        continue

                    if required_windows and not self._items_cover_windows(
                        items, required_windows
                    ):
                        self.logger.info(
                            f"Provider {provider.name} returned {len(items)} "
                            f"item(s) but left at least one required window "
                            f"empty; trying next provider"
                        )
                        if fallback is None or len(items) > len(fallback[0]):
                            fallback = (items, provider)
                        continue

                    return items, provider
                except Exception as e:
                    self.logger.warning(
                        f"Failed to get items with provider {provider.name}: {str(e)}"
                    )
                    continue

            if fallback is not None:
                self.logger.warning(
                    f"No STAC provider covered every required window for sensor "
                    f"'{sensor}'; falling back to {fallback[1].name} with "
                    f"{len(fallback[0])} item(s)"
                )
                return fallback

            raise RuntimeError(
                f"No items found with any available STAC provider for sensor '{sensor}'"
            )

    @staticmethod
    def _items_cover_windows(items: ItemCollection, windows: List[List[str]]) -> bool:
        """
        Check that every window contains at least one item.

        Window semantics come from util.date_windows, which the fire severity
        command also slices its time axis with -- the two must agree, or a
        provider could be accepted here and then yield an empty window.
        """
        for start_date, end_date in windows:
            start, end = window_bounds(start_date, end_date)

            if not any(
                item.datetime is not None and start <= item.datetime < end
                for item in items
            ):
                return False

        return True

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
