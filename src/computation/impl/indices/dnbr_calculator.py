import xarray as xr
from typing import Dict, Any, Optional, List
from src.computation.interfaces.index_calculator import IndexCalculator
from src.computation.impl.indices.nbr_calculator import NBRCalculator


class DNBRCalculator(IndexCalculator):
    """Calculator for Delta NBR (dNBR)"""

    def __init__(self, nbr_calculator: NBRCalculator):
        self.nbr_calculator = nbr_calculator

    @property
    def index_name(self) -> str:
        return "dnbr"

    def requires_pre_and_post(self) -> bool:
        return True

    def get_dependencies(self) -> List[str]:
        return ["nbr"]

    async def calculate(
        self,
        prefire_data: Optional[xr.DataArray],
        postfire_data: Optional[xr.DataArray],
        context: Dict[str, Any],
    ) -> xr.DataArray:
        """Calculate dNBR (difference between pre and post NBR)"""
        if prefire_data is None:
            raise ValueError("dNBR calculation requires prefire_data")
        if postfire_data is None:
            raise ValueError("dNBR calculation requires postfire_data")

        prefire_nbr = await self.nbr_calculator.calculate(prefire_data, None, context)
        postfire_nbr = await self.nbr_calculator.calculate(None, postfire_data, context)

        prefire_nbr, postfire_nbr = xr.align(prefire_nbr, postfire_nbr)
        return prefire_nbr - postfire_nbr
