"""The STAC provider chain: which APIs to search for a sensor, and in what order.

Providers are tried in configured order, and the first one with scenes in
every requested window wins. Two runs of the same request can therefore
resolve to different providers, which is why the choice is recorded with the
results rather than inferred from the request.
"""

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Sequence, Tuple

import planetary_computer
from pydantic import BaseModel
from pystac import Item
from pystac_client import Client as PystacClient

from src.util.date_windows import window_bounds
from src.util.geo_ops import Bounds

# Default sensor used when a caller does not specify one. Changing it changes
# job ids for requests that omit the sensor (D13), so it needs a
# PIPELINE_VERSION bump.
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
    """

    id: StacProvider
    name: str
    url: str
    collection: str
    swir_band: str
    nir_band: str
    mask_band: str

    def bands(self) -> Dict[str, str]:
        """Logical band name ("nir", "swir", "mask") to this provider's asset key."""
        return {"nir": self.nir_band, "swir": self.swir_band, "mask": self.mask_band}


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
        """The configured sensor keys (e.g. ["sentinel-2", "landsat"])."""
        return list(self.sensors.keys())

    def get_providers(self, sensor: str = DEFAULT_SENSOR) -> List[StacMapping]:
        """
        Get the ordered providers for a given sensor.

        Raises:
            ValueError: If the sensor is not configured.
        """
        if sensor not in self.sensors:
            raise ValueError(
                f"Unknown sensor '{sensor}'. Available sensors: "
                f"{sorted(self.sensors.keys())}"
            )
        return self.sensors[sensor].providers


@dataclass(frozen=True)
class WindowSearch:
    """The provider that covered every window, and each window's scenes."""

    provider: StacMapping
    windows: Tuple[List[Item], ...]


def in_window(items: Sequence[Item], window: Sequence[str]) -> List[Item]:
    """
    The items inside a ``[start, end]`` window, sorted by datetime then id.

    Window semantics come from util.date_windows: the end date includes its
    whole day. Items without a datetime are dropped.
    """
    start, end = window_bounds(window[0], window[1])
    inside = [
        item
        for item in items
        if item.datetime is not None and start <= item.datetime < end
    ]
    return sorted(inside, key=lambda item: (item.datetime, item.id))


def acquisition_key(item: Item) -> Optional[Tuple[str, str, str, str]]:
    """What one Sentinel-2 acquisition of one tile shares across its processings.

    The product URI names the mission, sensing start, relative orbit and tile,
    e.g. ``S2A_MSIL2A_20200803T181931_N0214_R127_T11SPV_20200803T224210.SAFE``.
    Two processings of one acquisition differ only in the baseline (``N0214``)
    and the processing time; their timestamps can differ by a millisecond, so
    the datetime can't identify them. None for items without a product URI.
    """
    uri = item.properties.get("s2:product_uri")
    if not isinstance(uri, str):
        return None
    parts = uri.split("_")
    if len(parts) < 7:
        return None
    return (parts[0], parts[2], parts[4], parts[5])


def processing_baseline(item: Item) -> Tuple[int, ...]:
    """``s2:processing_baseline`` as numbers ("05.00" -> (5, 0)); (-1,) if absent."""
    value = item.properties.get("s2:processing_baseline")
    try:
        return tuple(int(part) for part in str(value).split("."))
    except ValueError:
        return (-1,)


def latest_processing(items: Sequence[Item]) -> List[Item]:
    """One item per acquisition: the one with the newest processing baseline.

    Element 84 lists Sentinel-2 acquisitions from before 2022 twice: ESA's
    original processing (offset 0) and its Collection 1 reprocessing (baseline
    05.00, offset -0.1). Compositing both would read every scene twice and
    merge two processings of one acquisition. The reprocessing is ESA's
    consistent archive and matches current products, so it wins. Items that
    aren't Sentinel-2 products pass through. Sorted by datetime then id.
    """
    newest: Dict[Tuple[str, str, str, str], Item] = {}
    kept: List[Item] = []
    for item in items:
        key = acquisition_key(item)
        if key is None:
            kept.append(item)
            continue
        current = newest.get(key)
        if current is None or (processing_baseline(item), item.id) > (
            processing_baseline(current),
            current.id,
        ):
            newest[key] = item
    kept.extend(newest.values())
    return sorted(kept, key=lambda item: (item.datetime, item.id))


class StacEndpointHandler:
    """Searches the configured providers for a sensor, in order."""

    def __init__(
        self,
        stac_provider_json_path: str = "config/stac_providers.json",
    ):
        self.config = StacProviderConfig.load_from_file(stac_provider_json_path)
        self.logger = logging.getLogger(__name__)

    def get_sensors(self) -> List[str]:
        """Get the configured sensor keys."""
        return self.config.get_sensors()

    def search_windows(
        self,
        bbox: Bounds,
        windows: Sequence[Sequence[str]],
        sensor: str = DEFAULT_SENSOR,
    ) -> WindowSearch:
        """
        Search each window separately and return the first provider covering all.

        Searching per window, rather than one range from the first start to
        the last end, never returns the scenes between the windows. Searches
        are unsigned: Planetary Computer hrefs are signed just before they are
        read (see ``sign_for_read``), so a slow job never reads with an
        expired token.

        Args:
            bbox: West, south, east, north in EPSG:4326.
            windows: ``[start, end]`` date windows.
            sensor: Sensor key selecting the provider chain.

        Raises:
            ValueError: If the sensor is unknown.
            RuntimeError: If no provider has scenes in every window.
        """
        for provider in self.config.get_providers(sensor):
            try:
                client = PystacClient.open(provider.url)
                found = tuple(
                    latest_processing(
                        in_window(
                            list(
                                client.search(
                                    collections=[provider.collection],
                                    bbox=list(bbox),
                                    datetime=list(window_bounds(window[0], window[1])),
                                ).items()
                            ),
                            window,
                        )
                    )
                    for window in windows
                )
            except Exception as e:
                self.logger.warning(f"Search failed at {provider.name}: {e}")
                continue
            if all(found):
                self.logger.info(
                    f"Using {provider.name} for {sensor}: "
                    f"{[len(items) for items in found]} scenes per window"
                )
                return WindowSearch(provider=provider, windows=found)
            self.logger.info(
                f"{provider.name} has no {sensor} scenes in at least one window; "
                f"trying the next provider"
            )
        raise RuntimeError(
            f"No STAC provider for sensor '{sensor}' has scenes in every window"
        )

    @staticmethod
    def sign_for_read(provider: StacMapping, items: Sequence[Item]) -> List[Item]:
        """Copies of the items with hrefs a reader can open now.

        Planetary Computer hrefs need a short-lived SAS token; others are public.
        """
        if provider.id == StacProvider.MICROSOFT_PLANETARY_COMPUTER:
            return [planetary_computer.sign(item) for item in items]
        return list(items)
