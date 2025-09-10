"""
Fixtures and test data for comprehensive fire severity integration tests.
Provides realistic satellite data simulation and strategic mocking for external services.
"""

import numpy as np
import xarray as xr
import pytest
from typing import Dict, Any, List, Tuple
from unittest.mock import Mock, AsyncMock

from src.core.storage.memory import MemoryStorage
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry
from src.commands.interfaces.command_context import CommandContext


# Test geometry definitions with realistic sizes
TEST_GEOMETRIES = {
    "small": {
        "type": "Polygon",
        "coordinates": [[
            [-120.1, 35.1],
            [-120.08, 35.1], 
            [-120.08, 35.12],
            [-120.1, 35.12],
            [-120.1, 35.1]
        ]]
        # Approximately 2x2 km polygon
    },
    "medium": {
        "type": "Polygon", 
        "coordinates": [[
            [-120.1, 35.1],
            [-120.0, 35.1],
            [-120.0, 35.2], 
            [-120.1, 35.2],
            [-120.1, 35.1]
        ]]
        # Approximately 10x10 km polygon
    },
    "large": {
        "type": "Polygon",
        "coordinates": [[
            [-120.2, 35.0],
            [-119.95, 35.0],
            [-119.95, 35.25],
            [-120.2, 35.25], 
            [-120.2, 35.0]
        ]]
        # Approximately 25x25 km polygon
    }
}


def create_realistic_satellite_data(
    geometry_bounds: Tuple[float, float, float, float],
    time_coords: List[str],
    burn_pattern: str = "center_burn"
) -> xr.DataArray:
    """
    Create realistic synthetic satellite data with fire severity patterns.
    
    Args:
        geometry_bounds: (minx, miny, maxx, maxy) bounding box
        time_coords: List of datetime strings
        burn_pattern: Type of burn pattern ("center_burn", "edge_burn", "healthy")
        
    Returns:
        xarray.DataArray with realistic NIR and SWIR values
    """
    minx, miny, maxx, maxy = geometry_bounds
    
    # Calculate appropriate resolution for the geometry size
    width_deg = maxx - minx
    height_deg = maxy - miny
    
    # Aim for roughly 30m pixels (Sentinel-2 resolution)
    # 1 degree ≈ 111 km, so 30m ≈ 0.00027 degrees
    pixel_size = 0.0003  # Slightly larger for faster processing
    
    x_size = max(10, int(width_deg / pixel_size))
    y_size = max(10, int(height_deg / pixel_size))
    
    # Create coordinate arrays
    x_coords = np.linspace(minx, maxx, x_size)
    y_coords = np.linspace(miny, maxy, y_size)
    time_coords_dt = [np.datetime64(t) for t in time_coords]
    
    # Create realistic spectral values based on burn pattern
    if burn_pattern == "center_burn":
        # Burned area in center, healthy vegetation on edges
        nir_base, swir_base = create_burn_gradient(x_size, y_size, center_burned=True)
    elif burn_pattern == "edge_burn":
        # Burned area on edges, healthy center
        nir_base, swir_base = create_burn_gradient(x_size, y_size, center_burned=False)
    else:  # healthy
        # Uniform healthy vegetation
        nir_base = np.full((y_size, x_size), 0.45)  # Healthy NIR
        swir_base = np.full((y_size, x_size), 0.20)  # Healthy SWIR
    
    # Add temporal and band dimensions
    n_times = len(time_coords_dt)
    
    # Create full data array with shape (time, band, y, x)
    data = np.zeros((n_times, 2, y_size, x_size), dtype=np.float32)
    
    for t_idx in range(n_times):
        # Add slight temporal noise
        temporal_noise = np.random.normal(0, 0.02, (y_size, x_size))
        
        # NIR band (B08)
        data[t_idx, 0, :, :] = np.clip(nir_base + temporal_noise, 0, 1)
        
        # SWIR band (B12) 
        data[t_idx, 1, :, :] = np.clip(swir_base + temporal_noise * 0.5, 0, 1)
    
    # Create xarray DataArray
    da = xr.DataArray(
        data,
        dims=["time", "band", "y", "x"],
        coords={
            "time": time_coords_dt,
            "band": ["B08", "B12"],  # NIR, SWIR
            "y": y_coords,
            "x": x_coords
        },
        attrs={
            "crs": "EPSG:4326",
            "nodata": -9999
        }
    )
    
    # Set spatial reference information
    da.rio.write_crs("EPSG:4326", inplace=True)
    da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    
    return da


def create_burn_gradient(
    x_size: int, 
    y_size: int, 
    center_burned: bool = True
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Create realistic spectral gradients representing fire damage.
    
    Args:
        x_size: Width in pixels
        y_size: Height in pixels  
        center_burned: If True, burn in center; if False, burn on edges
        
    Returns:
        Tuple of (NIR, SWIR) arrays representing realistic spectral values
    """
    # Create coordinate grids
    x = np.linspace(0, 1, x_size)
    y = np.linspace(0, 1, y_size)
    X, Y = np.meshgrid(x, y)
    
    # Calculate distance from center
    center_dist = np.sqrt((X - 0.5)**2 + (Y - 0.5)**2)
    max_dist = np.sqrt(0.5)  # Maximum distance from center to corner
    normalized_dist = center_dist / max_dist
    
    if center_burned:
        # Burned in center, healthy on edges
        burn_intensity = 1.0 - normalized_dist
    else:
        # Healthy in center, burned on edges
        burn_intensity = normalized_dist
    
    # Apply smooth transition
    burn_intensity = np.clip(burn_intensity, 0, 1)
    
    # Realistic spectral values for different burn severities:
    # Healthy vegetation: NIR ~0.4-0.6, SWIR ~0.15-0.25
    # Burned areas: NIR ~0.2-0.3, SWIR ~0.3-0.4
    
    # NIR decreases with burn intensity
    nir_healthy = 0.5
    nir_burned = 0.25
    nir = nir_healthy - (nir_healthy - nir_burned) * burn_intensity
    
    # SWIR increases with burn intensity
    swir_healthy = 0.20
    swir_burned = 0.35
    swir = swir_healthy + (swir_burned - swir_healthy) * burn_intensity
    
    # Add spatial noise for realism
    spatial_noise = np.random.normal(0, 0.05, (y_size, x_size))
    nir = np.clip(nir + spatial_noise, 0.1, 0.8)
    swir = np.clip(swir + spatial_noise, 0.1, 0.5)
    
    return nir.astype(np.float32), swir.astype(np.float32)


@pytest.fixture 
def real_memory_storage() -> MemoryStorage:
    """Create real memory storage for integration tests"""
    return MemoryStorage(base_url="memory://integration-test")


@pytest.fixture
def real_index_registry() -> IndexRegistry:
    """Create real index registry with all calculators"""
    return IndexRegistry()


@pytest.fixture 
def mock_stac_manager() -> Mock:
    """Create mock STAC manager for integration tests"""
    manager = Mock(spec=STACJSONManager)
    manager.create_fire_severity_item = AsyncMock(
        return_value="memory://integration-test/stac/fire_severity_item.json"
    )
    return manager


@pytest.fixture
def realistic_stac_endpoint_mock():
    """
    Create realistic STAC endpoint mock that returns synthetic satellite data.
    This mocks the external STAC API but provides realistic data for computation.
    """
    def _create_mock(geometry_bounds, prefire_dates, postfire_dates, burn_pattern="center_burn"):
        mock_handler = Mock()
        
        # Mock search_items to return realistic STAC items
        async def mock_search_items(geometry, date_range, collections):
            # Create synthetic STAC items
            items = [
                {"id": f"sentinel-2-l2a-{i}", "datetime": date}
                for i, date in enumerate([
                    *prefire_dates,
                    *postfire_dates
                ])
            ]
            
            endpoint_config = {
                "nir_band": "B08",
                "swir_band": "B12", 
                "epsg": 4326
            }
            
            return items, endpoint_config
        
        mock_handler.search_items = AsyncMock(side_effect=mock_search_items)
        mock_handler.get_band_names.return_value = ("B08", "B12")
        mock_handler.get_epsg_code.return_value = 4326
        
        # Create combined date range for synthetic data
        all_dates = prefire_dates + postfire_dates
        
        # Mock stackstac.stack to return realistic synthetic data
        def mock_stackstac_stack(items, epsg, assets, bounds, chunksize):
            return create_realistic_satellite_data(
                geometry_bounds, all_dates, burn_pattern
            )
        
        return mock_handler, mock_stackstac_stack
    
    return _create_mock


def create_integration_context(
    geometry_name: str,
    real_memory_storage: MemoryStorage,
    mock_stac_manager: Mock,
    real_index_registry: IndexRegistry,
    prefire_dates: List[str] = None,
    postfire_dates: List[str] = None
) -> CommandContext:
    """
    Create a realistic command context for integration testing.
    
    Args:
        geometry_name: One of "small", "medium", "large"
        real_memory_storage: Real memory storage instance
        mock_stac_manager: Mocked STAC manager
        real_index_registry: Real index registry with calculators
        prefire_dates: Prefire date range
        postfire_dates: Postfire date range
        
    Returns:
        CommandContext configured for integration testing
    """
    if prefire_dates is None:
        prefire_dates = ["2023-06-01", "2023-06-15"]
    if postfire_dates is None:
        postfire_dates = ["2023-07-01", "2023-07-15"]
        
    geometry = TEST_GEOMETRIES[geometry_name]
    
    return CommandContext(
        job_id=f"integration-test-{geometry_name}",
        fire_event_name=f"test-fire-{geometry_name}",
        geometry=geometry,
        storage=real_memory_storage,
        stac_manager=mock_stac_manager,
        index_registry=real_index_registry,
        computation_config={
            "prefire_date_range": prefire_dates,
            "postfire_date_range": postfire_dates,
            "collection": "sentinel-2-l2a",
            "buffer_meters": 100,
            "indices": ["dnbr", "rdnbr", "rbr"],
        },
        metadata={"test_type": "integration", "geometry_size": geometry_name}
    )


def get_geometry_bounds(geometry: Dict[str, Any]) -> Tuple[float, float, float, float]:
    """Extract bounding box from GeoJSON geometry"""
    coords = geometry["coordinates"][0]
    lons = [pt[0] for pt in coords]
    lats = [pt[1] for pt in coords] 
    return min(lons), min(lats), max(lons), max(lats)


# Expected performance benchmarks for different geometry sizes
PERFORMANCE_EXPECTATIONS = {
    "small": {"max_time_seconds": 10, "expected_pixels": "< 1000"},
    "medium": {"max_time_seconds": 30, "expected_pixels": "1000-10000"}, 
    "large": {"max_time_seconds": 90, "expected_pixels": "> 10000"}
}

# Known spectral index value ranges for validation
EXPECTED_INDEX_RANGES = {
    "nbr": {"min": -1.0, "max": 1.0, "healthy": 0.3, "burned": -0.2},
    "dnbr": {"min": -2.0, "max": 2.0, "moderate_burn": 0.27, "high_burn": 0.66},
    "rdnbr": {"min": -2000, "max": 2000, "moderate_burn": 269, "high_burn": 659},
    "rbr": {"min": -1.0, "max": 1.0, "moderate_burn": 0.5, "high_burn": 0.8}
}