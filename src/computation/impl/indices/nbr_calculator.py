import xarray as xr
from typing import Dict, Any, Optional, List
from src.computation.interfaces.index_calculator import IndexCalculator


class NBRCalculator(IndexCalculator):
    """Calculator for Normalized Burn Ratio (NBR)"""

    @property
    def index_name(self) -> str:
        return "nbr"

    def requires_pre_and_post(self) -> bool:
        return False

    def get_dependencies(self) -> List[str]:
        return []

    async def calculate(
        self,
        prefire_data: Optional[xr.DataArray],
        postfire_data: Optional[xr.DataArray],
        context: Dict[str, Any],
    ) -> xr.DataArray:
        """Calculate NBR for the provided data period"""
        if prefire_data is None and postfire_data is None:
            raise ValueError(
                "NBR calculation requires either prefire_data or postfire_data"
            )

        if prefire_data is not None and postfire_data is not None:
            raise ValueError(
                "NBR calculation should receive only one period of data, not both"
            )

        # Use the provided data period
        data = prefire_data if prefire_data is not None else postfire_data
        assert data is not None, "Data cannot be None after validation"

        band_mapping = context.get("band_mapping")
        if not band_mapping:
            raise ValueError("Context must include 'band_mapping'")

        nir_band = band_mapping.get("nir")
        swir_band = band_mapping.get("swir")

        if not nir_band:
            raise ValueError("Band mapping must include 'nir' band")
        if not swir_band:
            raise ValueError("Band mapping must include 'swir' band")

        nir = data.sel(band=nir_band).median(dim="time")
        swir = data.sel(band=swir_band).median(dim="time")

        return (nir - swir) / (nir + swir)
