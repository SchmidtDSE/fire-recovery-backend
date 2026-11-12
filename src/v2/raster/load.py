"""Reading a window's scenes onto (part of) the output grid with odc-stac.

odc-stac reads projection v2 (``proj:code``) directly, groups the tiles of one
overpass into one time step (``groupby="solar_day"``), and reads native uint16
(design §2). It doesn't apply ``raster:bands`` scale and offset, so
``window_scaling`` finds them and the compute step applies them.
"""

from typing import Any, Dict, List, Optional, Sequence

import odc.stac
import planetary_computer
import xarray as xr
from odc.geo.geobox import GeoBox
from pystac import Item
from shapely.geometry import box, shape

from src.stac.stac_endpoint_handler import StacMapping, StacProvider
from src.util.geo_ops import Bounds
from src.v2.identity.grid import Grid
from src.v2.sensors import SENSOR_BAND_TYPES, fallback_scaling


def asset_scaling(item: Item, asset_key: str, sensor: str) -> tuple[float, float]:
    """(scale, offset) for one asset: from raster:bands, else the product's rules.

    Raises:
        ValueError: if the asset is missing or its scaling can't be determined.
    """
    asset = item.assets.get(asset_key)
    if asset is None:
        raise ValueError(f"Item {item.id} has no {asset_key!r} asset")
    bands = asset.extra_fields.get("raster:bands") or []
    if bands and "scale" in bands[0]:
        return float(bands[0]["scale"]), float(bands[0].get("offset", 0.0))
    return fallback_scaling(sensor, item.properties)


def window_scaling(
    items: Sequence[Item], provider: StacMapping, sensor: str
) -> tuple[float, float]:
    """The single (scale, offset) every reflectance asset in a window shares.

    Raises:
        ValueError: if the window mixes scalings. A Sentinel-2 window that
            straddles the January 2022 processing-baseline change does, and is
            rejected for now (decision 20).
    """
    pairs = {
        asset_scaling(item, key, sensor)
        for item in items
        for key in (provider.nir_band, provider.swir_band)
    }
    if len(pairs) != 1:
        raise ValueError(
            f"The window mixes reflectance scalings {sorted(pairs)}. A Sentinel-2 "
            f"window can't straddle the January 2022 processing-baseline change; "
            f"move it to one side."
        )
    return pairs.pop()


def stac_cfg(provider: StacMapping, sensor: str) -> Dict[str, Any]:
    """odc-stac band configuration: each band's native type and nodata.

    It overrides what items publish, so loads are uniform across providers,
    including Planetary Computer's Sentinel-2 items, which publish none.
    """
    keys = provider.bands()
    return {
        provider.collection: {
            "assets": {
                keys[logical]: {"data_type": dtype, "nodata": nodata}
                for logical, (dtype, nodata) in SENSOR_BAND_TYPES[sensor].items()
            }
        }
    }


def geobox(grid: Grid) -> GeoBox:
    """The odc-geo GeoBox of the output grid, exactly as derived (no anchoring)."""
    return GeoBox((grid.height, grid.width), grid.transform(), f"EPSG:{grid.epsg}")


def intersecting(items: Sequence[Item], footprint: Bounds) -> List[Item]:
    """The items whose geometry touches an EPSG:4326 box."""
    target = box(*footprint)
    return [
        item
        for item in items
        if item.geometry is not None and shape(item.geometry).intersects(target)
    ]


def load_block(
    items: Sequence[Item],
    provider: StacMapping,
    sensor: str,
    block: GeoBox,
    threads: int,
) -> Optional[xr.Dataset]:
    """Read every band a window needs over one block, eagerly.

    Planetary Computer hrefs are signed here, per block, so tokens are fresh
    however long the job runs. Returns None when no item touches the block.
    """
    footprint = block.geographic_extent.boundingbox
    touching = intersecting(
        items, (footprint.left, footprint.bottom, footprint.right, footprint.top)
    )
    if not touching:
        return None
    sign = (
        planetary_computer.sign
        if provider.id == StacProvider.MICROSOFT_PLANETARY_COMPUTER
        else None
    )
    return odc.stac.load(
        touching,
        bands=list(provider.bands().values()),
        geobox=block,
        groupby="solar_day",
        resampling="nearest",
        stac_cfg=stac_cfg(provider, sensor),
        patch_url=sign,
        pool=threads,
        fail_on_error=True,
    )
