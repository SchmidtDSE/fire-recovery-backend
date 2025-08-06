from typing import Dict, List, Optional, Tuple
from pystac import ItemCollection
from pystac_client import Client as PystacClient
import planetary_computer
from enum import Enum
from pydantic import BaseModel
import json
import logging


class StacProvider(Enum):
    """Enum for STAC providers."""

    MICROSOFT_PLANETARY_COMPUTER = "Microsoft Planetary Computer"
    ELEMENT_84 = "Element 84"


class StacMapping(BaseModel):
    """Model for STAC mapping configuration."""

    id: StacProvider
    name: str
    url: str
    swir_band: str
    nir_band: str
    epsg_code: int


class StacProviderConfig(BaseModel):
    providers: List[StacMapping]

    @classmethod
    def load_from_file(
        cls, filepath: str = "config/stac_providers.json"
    ) -> "StacProviderConfig":
        """Load configuration from a JSON file."""
        try:
            with open(filepath, "r") as f:
                data = json.load(f)

            # Convert string representation of enum to actual enum
            for provider in data.get("providers", []):
                if isinstance(provider.get("id"), str):
                    try:
                        provider["id"] = StacProvider[provider["id"]]
                    except KeyError:
                        raise ValueError(f"Unknown provider: {provider['id']}")

            return cls.model_validate(data)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {filepath} not found.")

    def get_providers(self) -> List[StacMapping]:
        """
        Get the providers as a list.

        Returns:
            List of provider configurations.
        """
        return self.providers


class StacEndpointHandler:
    """
    Handles interactions with STAC endpoints with fallback support.
    Tries multiple STAC endpoints in order of priority when data is not available.
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
        self.providers = StacProviderConfig.load_from_file(
            stac_provider_json_path
        ).get_providers()
        self.logger = logging.getLogger(__name__)

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
        geometry: Dict,
        date_range: List[str],
        collections: List[str] = None,
        provider_index: Optional[int] = None,
    ) -> Tuple[ItemCollection, StacMapping]:
        """
        Search for items using STAC providers.

        Args:
            geometry: GeoJSON geometry to search within
            date_range: List of [start_date, end_date] as strings
            collections: List of collection IDs to search
            provider_index: Index of provider to use. If None, tries all in order.

        Returns:
            Tuple of (items, provider)

        Raises:
            RuntimeError: If no items found at any provider
        """
        search_params = {
            "intersects": geometry,
            "datetime": "/".join(date_range),
        }

        if collections:
            search_params["collections"] = collections

        if provider_index is not None:
            # Try specific provider
            if provider_index < 0 or provider_index >= len(self.providers):
                raise ValueError(f"Provider index {provider_index} out of range")

            provider = self.providers[provider_index]
            client, provider = await self.get_client(provider)

            items = client.search(**search_params).item_collection()
            if len(items) > 0:
                return items, provider
            else:
                raise RuntimeError(f"No items found using provider {provider.name}")
        else:
            # Try all providers in order
            for i, provider in enumerate(self.providers):
                try:
                    self.logger.info(f"Trying STAC provider {i}: {provider.name}")
                    client, provider = await self.get_client(provider)

                    items = client.search(**search_params).item_collection()
                    if len(items) > 0:
                        return items, provider
                except Exception as e:
                    self.logger.warning(
                        f"Failed to get items with provider {provider.name}: {str(e)}"
                    )
                    continue

            raise RuntimeError("No items found with any available STAC provider")

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
