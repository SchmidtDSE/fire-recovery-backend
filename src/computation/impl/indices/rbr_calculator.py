import xarray as xr
from typing import Dict, Any, Optional, List
from src.computation.interfaces.index_calculator import IndexCalculator
from src.computation.impl.indices.nbr_calculator import NBRCalculator
from src.computation.impl.indices.dnbr_calculator import DNBRCalculator


class RBRCalculator(IndexCalculator):
    """Calculator for Relativized Burn Ratio (RBR)"""

    def __init__(self, nbr_calculator: NBRCalculator, dnbr_calculator: DNBRCalculator):
        self.nbr_calculator = nbr_calculator
        self.dnbr_calculator = dnbr_calculator

    @property
    def index_name(self) -> str:
        return "rbr"

    def requires_pre_and_post(self) -> bool:
        return True

    def get_dependencies(self) -> List[str]:
        return ["nbr", "dnbr"]

    async def calculate(
        self,
        prefire_data: Optional[xr.DataArray],
        postfire_data: Optional[xr.DataArray],
        context: Dict[str, Any],
    ) -> xr.DataArray:
        """Calculate RBR (relativized burn ratio)"""
        if prefire_data is None:
            raise ValueError("RBR calculation requires prefire_data")
        if postfire_data is None:
            raise ValueError("RBR calculation requires postfire_data")

        prefire_nbr = await self.nbr_calculator.calculate(prefire_data, None, context)
        dnbr = await self.dnbr_calculator.calculate(
            prefire_data, postfire_data, context
        )

        # RBR = dNBR / (prefire_NBR + 1.001)
        # Adding 1.001 to avoid division by zero and normalize the range
        denominator = prefire_nbr + 1.001

        return dnbr / denominator
