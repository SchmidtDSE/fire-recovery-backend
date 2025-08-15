import pytest
import xarray as xr
import numpy as np
import pandas as pd
from typing import Any


@pytest.fixture
def mock_data() -> xr.DataArray:
    """Create mock xarray DataArray for testing"""
    # Create sample data with NIR and SWIR bands
    data = xr.DataArray(
        np.random.rand(2, 10, 10, 3),  # bands, height, width, time
        dims=["band", "y", "x", "time"],
        coords={
            "band": ["nir", "swir"],
            "y": range(10),
            "x": range(10),
            "time": pd.date_range("2023-01-01", periods=3, freq="D"),
        },
    )
    # Ensure NIR > SWIR for positive NBR values in most cases
    data.loc[dict(band="nir")] = data.loc[dict(band="nir")] * 0.8 + 0.2
    data.loc[dict(band="swir")] = data.loc[dict(band="swir")] * 0.3 + 0.1
    return data


@pytest.fixture
def simple_test_data() -> xr.DataArray:
    """Create simple test data with known values for formula verification"""
    nir_values = np.array([[0.8, 0.6], [0.7, 0.9]])
    swir_values = np.array([[0.2, 0.3], [0.1, 0.4]])
    
    data = xr.DataArray(
        np.stack([nir_values, swir_values])[..., np.newaxis],
        dims=["band", "y", "x", "time"],
        coords={
            "band": ["nir", "swir"],
            "y": [0, 1],
            "x": [0, 1],
            "time": [np.datetime64("2023-01-01")],
        },
    )
    return data


@pytest.fixture
def band_context() -> dict[str, Any]:
    """Create mock context with band mapping"""
    return {
        "band_mapping": {
            "nir": "nir",
            "swir": "swir"
        }
    }