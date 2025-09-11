import pytest
import xarray as xr
import numpy as np
from typing import Any
from src.computation.impl.indices.nbr_calculator import NBRCalculator


class TestNBRCalculator:
    """Test suite for NBRCalculator"""

    @pytest.fixture
    def calculator(self) -> NBRCalculator:
        """Create NBRCalculator instance"""
        return NBRCalculator()

    def test_index_name(self, calculator: NBRCalculator) -> None:
        """Test that index name is correct"""
        assert calculator.index_name == "nbr"

    def test_requires_pre_and_post(self, calculator: NBRCalculator) -> None:
        """Test that NBR does not require both periods"""
        assert calculator.requires_pre_and_post() is False

    def test_get_dependencies(self, calculator: NBRCalculator) -> None:
        """Test that NBR has no dependencies"""
        assert calculator.get_dependencies() == []

    @pytest.mark.asyncio
    async def test_calculate_with_prefire_data(
        self,
        calculator: NBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test NBR calculation with prefire data only"""
        result = await calculator.calculate(mock_data, None, band_context)

        # Check result shape and type
        assert isinstance(result, xr.DataArray)
        assert result.shape == (10, 10)  # Should have spatial dimensions only

        # Check that values are in expected range [-1, 1]
        assert result.min() >= -1.0
        assert result.max() <= 1.0

    @pytest.mark.asyncio
    async def test_calculate_with_postfire_data(
        self,
        calculator: NBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test NBR calculation with postfire data only"""
        result = await calculator.calculate(None, mock_data, band_context)

        # Check result shape and type
        assert isinstance(result, xr.DataArray)
        assert result.shape == (10, 10)

        # Check that values are in expected range
        assert result.min() >= -1.0
        assert result.max() <= 1.0

    @pytest.mark.asyncio
    async def test_calculate_raises_with_no_data(
        self, calculator: NBRCalculator, band_context: dict[str, Any]
    ) -> None:
        """Test that calculation raises error when no data provided"""
        with pytest.raises(
            ValueError,
            match="NBR calculation requires either prefire_data or postfire_data",
        ):
            await calculator.calculate(None, None, band_context)

    @pytest.mark.asyncio
    async def test_calculate_raises_with_both_data(
        self,
        calculator: NBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test that calculation raises error when both data periods provided"""
        with pytest.raises(
            ValueError,
            match="NBR calculation should receive only one period of data, not both",
        ):
            await calculator.calculate(mock_data, mock_data, band_context)

    @pytest.mark.asyncio
    async def test_calculate_raises_without_band_mapping(
        self, calculator: NBRCalculator, mock_data: xr.DataArray
    ) -> None:
        """Test that calculation raises error when band mapping missing"""
        context: dict[str, Any] = {}
        with pytest.raises(ValueError, match="Context must include 'band_mapping'"):
            await calculator.calculate(mock_data, None, context)

    @pytest.mark.asyncio
    async def test_calculate_raises_without_nir_band(
        self, calculator: NBRCalculator, mock_data: xr.DataArray
    ) -> None:
        """Test that calculation raises error when NIR band missing from mapping"""
        context = {"band_mapping": {"swir": "swir"}}
        with pytest.raises(ValueError, match="Band mapping must include 'nir' band"):
            await calculator.calculate(mock_data, None, context)

    @pytest.mark.asyncio
    async def test_calculate_raises_without_swir_band(
        self, calculator: NBRCalculator, mock_data: xr.DataArray
    ) -> None:
        """Test that calculation raises error when SWIR band missing from mapping"""
        context = {"band_mapping": {"nir": "nir"}}
        with pytest.raises(ValueError, match="Band mapping must include 'swir' band"):
            await calculator.calculate(mock_data, None, context)

    @pytest.mark.asyncio
    async def test_nbr_formula_correctness(
        self,
        calculator: NBRCalculator,
        simple_test_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test that NBR formula is calculated correctly"""
        result = await calculator.calculate(simple_test_data, None, band_context)

        # Calculate expected NBR manually: (NIR - SWIR) / (NIR + SWIR)
        nir_values = np.array([[0.8, 0.6], [0.7, 0.9]])
        swir_values = np.array([[0.2, 0.3], [0.1, 0.4]])
        expected = (nir_values - swir_values) / (nir_values + swir_values)

        np.testing.assert_array_almost_equal(result.values, expected, decimal=6)
