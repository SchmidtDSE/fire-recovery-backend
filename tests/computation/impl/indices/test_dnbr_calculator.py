import pytest
import xarray as xr
import numpy as np
from typing import Any
from unittest.mock import AsyncMock, Mock
from src.computation.impl.indices.dnbr_calculator import DNBRCalculator
from src.computation.impl.indices.nbr_calculator import NBRCalculator


class TestDNBRCalculator:
    """Test suite for DNBRCalculator"""

    @pytest.fixture
    def mock_nbr_calculator(self) -> Mock:
        """Create mock NBRCalculator"""
        mock = Mock(spec=NBRCalculator)
        mock.calculate = AsyncMock()
        return mock

    @pytest.fixture
    def calculator(self, mock_nbr_calculator: Mock) -> DNBRCalculator:
        """Create DNBRCalculator instance with mock NBR calculator"""
        return DNBRCalculator(mock_nbr_calculator)

    def test_index_name(self, calculator: DNBRCalculator) -> None:
        """Test that index name is correct"""
        assert calculator.index_name == "dnbr"

    def test_requires_pre_and_post(self, calculator: DNBRCalculator) -> None:
        """Test that DNBR requires both periods"""
        assert calculator.requires_pre_and_post() is True

    def test_get_dependencies(self, calculator: DNBRCalculator) -> None:
        """Test that DNBR depends on NBR"""
        assert calculator.get_dependencies() == ["nbr"]

    @pytest.mark.asyncio
    async def test_calculate_with_valid_data(
        self,
        calculator: DNBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
        mock_nbr_calculator: Mock,
    ) -> None:
        """Test DNBR calculation with valid pre and post fire data"""
        # Mock NBR results
        prefire_nbr = xr.DataArray(np.ones((10, 10)) * 0.8, dims=["y", "x"])
        postfire_nbr = xr.DataArray(np.ones((10, 10)) * 0.3, dims=["y", "x"])

        mock_nbr_calculator.calculate.side_effect = [prefire_nbr, postfire_nbr]

        result = await calculator.calculate(mock_data, mock_data, band_context)

        # Check that NBR calculator was called correctly
        assert mock_nbr_calculator.calculate.call_count == 2

        # Check the calls manually since xarray comparison in mock is problematic
        calls = mock_nbr_calculator.calculate.call_args_list

        # First call should be (mock_data, None, band_context)
        first_call = calls[0]
        assert first_call[0][0] is mock_data
        assert first_call[0][1] is None
        assert first_call[0][2] == band_context

        # Second call should be (None, mock_data, band_context)
        second_call = calls[1]
        assert second_call[0][0] is None
        assert second_call[0][1] is mock_data
        assert second_call[0][2] == band_context

        # Check result
        assert isinstance(result, xr.DataArray)
        assert result.shape == (10, 10)

        # dNBR should be prefire - postfire = 0.8 - 0.3 = 0.5
        expected_dnbr = 0.5
        np.testing.assert_array_almost_equal(result.values, expected_dnbr, decimal=6)

    @pytest.mark.asyncio
    async def test_calculate_raises_without_prefire_data(
        self,
        calculator: DNBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test that calculation raises error when prefire data missing"""
        with pytest.raises(ValueError, match="dNBR calculation requires prefire_data"):
            await calculator.calculate(None, mock_data, band_context)

    @pytest.mark.asyncio
    async def test_calculate_raises_without_postfire_data(
        self,
        calculator: DNBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test that calculation raises error when postfire data missing"""
        with pytest.raises(ValueError, match="dNBR calculation requires postfire_data"):
            await calculator.calculate(mock_data, None, band_context)

    @pytest.mark.asyncio
    async def test_calculate_with_misaligned_data(
        self,
        calculator: DNBRCalculator,
        band_context: dict[str, Any],
        mock_nbr_calculator: Mock,
    ) -> None:
        """Test DNBR calculation with misaligned spatial data"""
        # Create NBR results with different coordinates
        prefire_nbr = xr.DataArray(
            np.ones((5, 5)) * 0.8,
            dims=["y", "x"],
            coords={"y": range(5), "x": range(5)},
        )
        postfire_nbr = xr.DataArray(
            np.ones((5, 5)) * 0.3,
            dims=["y", "x"],
            coords={"y": range(2, 7), "x": range(2, 7)},
        )

        mock_nbr_calculator.calculate.side_effect = [prefire_nbr, postfire_nbr]

        # Create mock data (content doesn't matter since NBR calculator is mocked)
        mock_data = xr.DataArray(
            np.zeros((2, 5, 5, 1)), dims=["band", "y", "x", "time"]
        )

        result = await calculator.calculate(mock_data, mock_data, band_context)

        # xr.align should handle misalignment, result should have intersection of coordinates
        assert isinstance(result, xr.DataArray)
        # The intersection should be coordinates [2, 3, 4] for both y and x
        assert result.shape == (3, 3)
