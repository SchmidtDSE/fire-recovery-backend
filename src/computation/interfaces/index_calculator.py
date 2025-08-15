from abc import ABC, abstractmethod
import xarray as xr
from typing import Dict, Any, Optional, List


class IndexCalculator(ABC):
    """Base interface for calculating a single spectral index"""
    
    @property
    @abstractmethod
    def index_name(self) -> str:
        """Return the name of this index (e.g., 'nbr', 'dnbr')"""
        pass
    
    @abstractmethod
    async def calculate(
        self, 
        prefire_data: Optional[xr.DataArray], 
        postfire_data: Optional[xr.DataArray],
        context: Dict[str, Any]
    ) -> xr.DataArray:
        """Calculate this specific index"""
        pass
    
    @abstractmethod
    def requires_pre_and_post(self) -> bool:
        """Return True if this index needs both pre and post fire data"""
        pass
    
    @abstractmethod
    def get_dependencies(self) -> List[str]:
        """Return list of other indices this one depends on"""
        pass