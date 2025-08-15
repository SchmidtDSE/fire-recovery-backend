# Fire Recovery Backend Refactoring Plan (Updated)

## Strategy Pattern Refinement

You're absolutely correct - having a single strategy that calculates ALL indices violates the Single Responsibility Principle. Each index should be its own strategy for maximum flexibility.

## Updated Architecture: Individual Index Strategies

### Strategy Pattern for Individual Indices

**Each fire severity index becomes its own strategy:**

```
src/computation/implementations/indices/
├── nbr_calculator.py           # Normalized Burn Ratio
├── dnbr_calculator.py          # Delta NBR (prefire - postfire)  
├── rdnbr_calculator.py         # Relativized dNBR
├── rbr_calculator.py           # Relativized Burn Ratio
├── bai_calculator.py           # Future: Burned Area Index
├── nbr2_calculator.py          # Future: NBR2 variant
└── custom_severity_calculator.py  # Future: Custom algorithms
```

### Base Interface

**File: `src/computation/interfaces/index_calculator.py`**
```python
from abc import ABC, abstractmethod
import xarray as xr
from typing import Dict, Any, Optional

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
    def requires_both_periods(self) -> bool:
        """Return True if this index needs both pre and post fire data"""
        pass
    
    @abstractmethod
    def get_dependencies(self) -> List[str]:
        """Return list of other indices this one depends on"""
        pass
```

## Individual Strategy Implementations

### Base NBR Calculator
```python
# src/computation/implementations/indices/nbr_calculator.py
class NBRCalculator(IndexCalculator):
    @property
    def index_name(self) -> str:
        return "nbr"
    
    def requires_both_periods(self) -> bool:
        return False  # Can calculate for single time period
    
    def get_dependencies(self) -> List[str]:
        return []  # No dependencies
    
    async def calculate(
        self, 
        prefire_data: Optional[xr.DataArray], 
        postfire_data: Optional[xr.DataArray],
        context: Dict[str, Any]
    ) -> xr.DataArray:
        """Calculate NBR for the provided data period"""
        data = prefire_data if prefire_data is not None else postfire_data
        if data is None:
            raise ValueError("NBR calculation requires either prefire or postfire data")
        
        nir_band = context['band_mapping']['nir']
        swir_band = context['band_mapping']['swir']
        
        nir = data.sel(band=nir_band).median(dim="time")
        swir = data.sel(band=swir_band).median(dim="time")
        
        return (nir - swir) / (nir + swir)
```

### Delta NBR Calculator
```python
# src/computation/implementations/indices/dnbr_calculator.py
class DNBRCalculator(IndexCalculator):
    def __init__(self, nbr_calculator: NBRCalculator):
        self.nbr_calculator = nbr_calculator
    
    @property
    def index_name(self) -> str:
        return "dnbr"
    
    def requires_both_periods(self) -> bool:
        return True  # Needs both pre and post fire data
    
    def get_dependencies(self) -> List[str]:
        return ["nbr"]  # Depends on NBR calculation
    
    async def calculate(
        self, 
        prefire_data: Optional[xr.DataArray], 
        postfire_data: Optional[xr.DataArray],
        context: Dict[str, Any]
    ) -> xr.DataArray:
        """Calculate dNBR (difference between pre and post NBR)"""
        if prefire_data is None or postfire_data is None:
            raise ValueError("dNBR calculation requires both prefire and postfire data")
        
        prefire_nbr = await self.nbr_calculator.calculate(
            prefire_data, None, context
        )
        postfire_nbr = await self.nbr_calculator.calculate(
            postfire_data, None, context
        )
        
        prefire_nbr, postfire_nbr = xr.align(prefire_nbr, postfire_nbr)
        return prefire_nbr - postfire_nbr
```

### RdNBR Calculator
```python
# src/computation/implementations/indices/rdnbr_calculator.py
class RdNBRCalculator(IndexCalculator):
    def __init__(self, nbr_calculator: NBRCalculator, dnbr_calculator: DNBRCalculator):
        self.nbr_calculator = nbr_calculator
        self.dnbr_calculator = dnbr_calculator
    
    @property
    def index_name(self) -> str:
        return "rdnbr"
    
    def requires_both_periods(self) -> bool:
        return True
    
    def get_dependencies(self) -> List[str]:
        return ["nbr", "dnbr"]
    
    async def calculate(
        self, 
        prefire_data: Optional[xr.DataArray], 
        postfire_data: Optional[xr.DataArray],
        context: Dict[str, Any]
    ) -> xr.DataArray:
        """Calculate RdNBR (relativized dNBR)"""
        prefire_nbr = await self.nbr_calculator.calculate(
            prefire_data, None, context
        )
        dnbr = await self.dnbr_calculator.calculate(
            prefire_data, postfire_data, context
        )
        
        abs_sqrt_prefire_nbr = abs(prefire_nbr) ** 0.5
        # Avoid division by zero
        abs_sqrt_prefire_nbr = abs_sqrt_prefire_nbr.where(
            abs_sqrt_prefire_nbr != 0, 0.001
        )
        
        return dnbr / abs_sqrt_prefire_nbr
```

## Command Pattern with Index Selection

### Updated Fire Severity Command
```python
# src/services/commands/analyze_fire_severity_command.py
class AnalyzeFireSeverityCommand(BaseCommand):
    def __init__(self, repositories, index_registry: IndexRegistry):
        super().__init__(repositories, {})
        self.index_registry = index_registry
    
    async def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        # 1. Get requested indices (default to all available)
        requested_indices = context.get('indices', ['nbr', 'dnbr', 'rdnbr', 'rbr'])
        
        # 2. Fetch data via repositories
        prefire_data = await self._fetch_prefire_data(context)
        postfire_data = await self._fetch_postfire_data(context)
        
        # 3. Calculate each requested index
        results = {}
        for index_name in requested_indices:
            calculator = self.index_registry.get_calculator(index_name)
            if calculator:
                results[index_name] = await calculator.calculate(
                    prefire_data, postfire_data, context
                )
        
        # 4. Store results
        urls = await self.repositories['result'].store_results(results, context)
        
        return {'status': 'completed', 'urls': urls}
```

### Index Registry for Dependency Management
```python
# src/computation/registry/index_registry.py
class IndexRegistry:
    def __init__(self):
        self._calculators = {}
        self._setup_calculators()
    
    def _setup_calculators(self):
        # Create instances with proper dependencies
        nbr_calc = NBRCalculator()
        dnbr_calc = DNBRCalculator(nbr_calc)
        rdnbr_calc = RdNBRCalculator(nbr_calc, dnbr_calc)
        rbr_calc = RBRCalculator(nbr_calc, dnbr_calc)
        
        self._calculators = {
            'nbr': nbr_calc,
            'dnbr': dnbr_calc,
            'rdnbr': rdnbr_calc,
            'rbr': rbr_calc,
        }
    
    def get_calculator(self, index_name: str) -> Optional[IndexCalculator]:
        return self._calculators.get(index_name)
    
    def get_available_indices(self) -> List[str]:
        return list(self._calculators.keys())
    
    def add_calculator(self, calculator: IndexCalculator):
        """Add new index calculator at runtime"""
        self._calculators[calculator.index_name] = calculator
```

## Benefits of Individual Index Strategies

### 1. Selective Processing
```python
# Only calculate what's needed
context = {
    'indices': ['nbr', 'dnbr'],  # Skip expensive RdNBR and RBR
    # ... other context
}
```

### 2. Easy Extension
```python
# Add new index without touching existing code
class BAICalculator(IndexCalculator):
    @property
    def index_name(self) -> str:
        return "bai"  # Burned Area Index
    
    # Implement required methods...

# Register new calculator
registry.add_calculator(BAICalculator())
```

### 3. Performance Optimization
```python
# Different strategies for different data sizes
class FastNBRCalculator(IndexCalculator):
    """Optimized NBR for large datasets"""
    
class PreciseNBRCalculator(IndexCalculator):
    """High-precision NBR for small datasets"""

# Choose strategy based on data size
if data_size > LARGE_DATASET_THRESHOLD:
    registry.replace_calculator('nbr', FastNBRCalculator())
```

### 4. Independent Testing
```python
# Test each index calculation independently
def test_nbr_calculation():
    calculator = NBRCalculator()
    result = await calculator.calculate(mock_data, None, mock_context)
    assert result.shape == expected_shape

def test_dnbr_calculation():
    nbr_calc = Mock()
    dnbr_calc = DNBRCalculator(nbr_calc)
    # Test dnbr logic without depending on actual NBR calculation
```

## API Integration

### Request with Index Selection
```python
@router.post("/process/analyze_fire_severity")
async def analyze_fire_severity(request: ProcessingRequest):
    command = AnalyzeFireSeverityCommand(repositories, index_registry)
    
    context = {
        'job_id': str(uuid.uuid4()),
        'fire_event_name': request.fire_event_name,
        'geometry': request.geometry,
        'prefire_date_range': request.prefire_date_range,
        'postfire_date_range': request.postfire_date_range,
        'indices': request.indices or ['nbr', 'dnbr', 'rdnbr', 'rbr']  # Default all
    }
    
    background_tasks.add_task(command.execute, context)
```

### Request Model
```python
class ProcessingRequest(BaseModel):
    fire_event_name: str
    geometry: Polygon
    prefire_date_range: List[str]
    postfire_date_range: List[str]
    indices: Optional[List[str]] = None  # Allow index selection
```

This approach gives you:
- **Maximum flexibility**: Add/remove indices independently
- **Performance control**: Calculate only what's needed
- **Easy testing**: Each index is independently testable
- **Future-proof**: New algorithms are just new strategy classes
- **Resource optimization**: Skip expensive calculations when not needed

The individual strategy pattern is definitely the better approach for your use case!