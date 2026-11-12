import pytest
import xarray as xr
import numpy as np
from typing import Any
from unittest.mock import AsyncMock, Mock
from src.computation.impl.indices.rdnbr_calculator import RdNBRCalculator
from src.computation.impl.indices.nbr_calculator import NBRCalculator
from src.computation.impl.indices.dnbr_calculator import DNBRCalculator


class TestRdNBRCalculator:
    """Test suite for RdNBRCalculator"""

    @pytest.fixture
    def mock_nbr_calculator(self) -> Mock:
        """Create mock NBRCalculator"""
        mock = Mock(spec=NBRCalculator)
        mock.calculate = AsyncMock()
        return mock

    @pytest.fixture
    def mock_dnbr_calculator(self) -> Mock:
        """Create mock DNBRCalculator"""
        mock = Mock(spec=DNBRCalculator)
        mock.calculate = AsyncMock()
        return mock

    @pytest.fixture
    def calculator(
        self, mock_nbr_calculator: Mock, mock_dnbr_calculator: Mock
    ) -> RdNBRCalculator:
        """Create RdNBRCalculator instance with mock calculators"""
        return RdNBRCalculator(mock_nbr_calculator, mock_dnbr_calculator)

    def test_index_name(self, calculator: RdNBRCalculator) -> None:
        """Test that index name is correct"""
        assert calculator.index_name == "rdnbr"

    def test_requires_pre_and_post(self, calculator: RdNBRCalculator) -> None:
        """Test that RdNBR requires both periods"""
        assert calculator.requires_pre_and_post() is True

    def test_get_dependencies(self, calculator: RdNBRCalculator) -> None:
        """Test that RdNBR depends on NBR and dNBR"""
        assert calculator.get_dependencies() == ["nbr", "dnbr"]

    @pytest.mark.asyncio
    async def test_calculate_with_valid_data(
        self,
        calculator: RdNBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
        mock_nbr_calculator: Mock,
        mock_dnbr_calculator: Mock,
    ) -> None:
        """Test RdNBR calculation with valid data"""
        # Mock results
        prefire_nbr = xr.DataArray(
            np.ones((10, 10)) * 0.64, dims=["y", "x"]
        )  # sqrt will be 0.8
        dnbr = xr.DataArray(np.ones((10, 10)) * 0.4, dims=["y", "x"])

        mock_nbr_calculator.calculate.return_value = prefire_nbr
        mock_dnbr_calculator.calculate.return_value = dnbr

        result = await calculator.calculate(mock_data, mock_data, band_context)

        # Check that calculators were called correctly
        mock_nbr_calculator.calculate.assert_called_once_with(
            mock_data, None, band_context
        )
        mock_dnbr_calculator.calculate.assert_called_once_with(
            mock_data, mock_data, band_context
        )

        # Check result
        assert isinstance(result, xr.DataArray)
        assert result.shape == (10, 10)

        # RdNBR = dNBR / sqrt(abs(prefire_NBR)) = 0.4 / sqrt(0.64) = 0.4 / 0.8 = 0.5
        expected_rdnbr = 0.5
        np.testing.assert_array_almost_equal(result.values, expected_rdnbr, decimal=6)

    @pytest.mark.asyncio
    async def test_calculate_with_zero_prefire_nbr(
        self,
        calculator: RdNBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
        mock_nbr_calculator: Mock,
        mock_dnbr_calculator: Mock,
    ) -> None:
        """Test RdNBR calculation handles zero prefire NBR values"""
        # Mock results with some zero NBR values
        prefire_nbr_data = np.ones((10, 10)) * 0.64
        prefire_nbr_data[0, 0] = 0.0  # Add a zero value
        prefire_nbr = xr.DataArray(prefire_nbr_data, dims=["y", "x"])
        dnbr = xr.DataArray(np.ones((10, 10)) * 0.4, dims=["y", "x"])

        mock_nbr_calculator.calculate.return_value = prefire_nbr
        mock_dnbr_calculator.calculate.return_value = dnbr

        result = await calculator.calculate(mock_data, mock_data, band_context)

        # Check that zero NBR was replaced with 0.001 to avoid division by zero
        # At [0,0]: RdNBR = 0.4 / sqrt(0.001) ≈ 12.65
        assert not np.isinf(result.values).any()
        assert not np.isnan(result.values).any()

        # The zero location should have a large positive value
        assert result.values[0, 0] > 10

    @pytest.mark.asyncio
    async def test_calculate_with_negative_prefire_nbr(
        self,
        calculator: RdNBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
        mock_nbr_calculator: Mock,
        mock_dnbr_calculator: Mock,
    ) -> None:
        """Test RdNBR calculation handles negative prefire NBR values"""
        # Mock results with negative NBR values
        prefire_nbr = xr.DataArray(
            np.ones((10, 10)) * -0.36, dims=["y", "x"]
        )  # abs sqrt will be 0.6
        dnbr = xr.DataArray(np.ones((10, 10)) * 0.3, dims=["y", "x"])

        mock_nbr_calculator.calculate.return_value = prefire_nbr
        mock_dnbr_calculator.calculate.return_value = dnbr

        result = await calculator.calculate(mock_data, mock_data, band_context)

        # RdNBR = dNBR / sqrt(abs(prefire_NBR)) = 0.3 / sqrt(0.36) = 0.3 / 0.6 = 0.5
        expected_rdnbr = 0.5
        np.testing.assert_array_almost_equal(result.values, expected_rdnbr, decimal=6)

    @pytest.mark.asyncio
    async def test_calculate_raises_without_prefire_data(
        self,
        calculator: RdNBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test that calculation raises error when prefire data missing"""
        with pytest.raises(ValueError, match="RdNBR calculation requires prefire_data"):
            await calculator.calculate(None, mock_data, band_context)

    @pytest.mark.asyncio
    async def test_calculate_raises_without_postfire_data(
        self,
        calculator: RdNBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test that calculation raises error when postfire data missing"""
        with pytest.raises(
            ValueError, match="RdNBR calculation requires postfire_data"
        ):
            await calculator.calculate(mock_data, None, band_context)
