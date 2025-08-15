import pytest
import xarray as xr
import numpy as np
from typing import Any
from unittest.mock import AsyncMock, Mock
from src.computation.impl.indices.rbr_calculator import RBRCalculator
from src.computation.impl.indices.nbr_calculator import NBRCalculator
from src.computation.impl.indices.dnbr_calculator import DNBRCalculator


class TestRBRCalculator:
    """Test suite for RBRCalculator"""

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
    ) -> RBRCalculator:
        """Create RBRCalculator instance with mock calculators"""
        return RBRCalculator(mock_nbr_calculator, mock_dnbr_calculator)

    def test_index_name(self, calculator: RBRCalculator) -> None:
        """Test that index name is correct"""
        assert calculator.index_name == "rbr"

    def test_requires_pre_and_post(self, calculator: RBRCalculator) -> None:
        """Test that RBR requires both periods"""
        assert calculator.requires_pre_and_post() is True

    def test_get_dependencies(self, calculator: RBRCalculator) -> None:
        """Test that RBR depends on NBR and dNBR"""
        assert calculator.get_dependencies() == ["nbr", "dnbr"]

    @pytest.mark.asyncio
    async def test_calculate_with_valid_data(
        self,
        calculator: RBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
        mock_nbr_calculator: Mock,
        mock_dnbr_calculator: Mock,
    ) -> None:
        """Test RBR calculation with valid data"""
        # Mock results
        prefire_nbr = xr.DataArray(np.ones((10, 10)) * 0.6, dims=["y", "x"])
        dnbr = xr.DataArray(np.ones((10, 10)) * 0.3, dims=["y", "x"])

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

        # RBR = dNBR / (prefire_NBR + 1.001) = 0.3 / (0.6 + 1.001) = 0.3 / 1.601 ≈ 0.1874
        expected_rbr = 0.3 / 1.601
        np.testing.assert_array_almost_equal(result.values, expected_rbr, decimal=4)

    @pytest.mark.asyncio
    async def test_calculate_with_negative_prefire_nbr(
        self,
        calculator: RBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
        mock_nbr_calculator: Mock,
        mock_dnbr_calculator: Mock,
    ) -> None:
        """Test RBR calculation with negative prefire NBR values"""
        # Mock results with negative NBR values
        prefire_nbr = xr.DataArray(np.ones((10, 10)) * -0.5, dims=["y", "x"])
        dnbr = xr.DataArray(np.ones((10, 10)) * 0.2, dims=["y", "x"])

        mock_nbr_calculator.calculate.return_value = prefire_nbr
        mock_dnbr_calculator.calculate.return_value = dnbr

        result = await calculator.calculate(mock_data, mock_data, band_context)

        # RBR = dNBR / (prefire_NBR + 1.001) = 0.2 / (-0.5 + 1.001) = 0.2 / 0.501 ≈ 0.399
        expected_rbr = 0.2 / 0.501
        np.testing.assert_array_almost_equal(result.values, expected_rbr, decimal=3)

    @pytest.mark.asyncio
    async def test_calculate_normalizes_range(
        self,
        calculator: RBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
        mock_nbr_calculator: Mock,
        mock_dnbr_calculator: Mock,
    ) -> None:
        """Test that RBR normalizes the range appropriately"""
        # Test with extreme NBR values
        prefire_nbr = xr.DataArray(np.array([[1.0, -1.0], [0.0, 0.5]]), dims=["y", "x"])
        dnbr = xr.DataArray(np.ones((2, 2)) * 0.4, dims=["y", "x"])

        mock_nbr_calculator.calculate.return_value = prefire_nbr
        mock_dnbr_calculator.calculate.return_value = dnbr

        result = await calculator.calculate(mock_data, mock_data, band_context)

        # Check that result values are reasonable
        assert result.shape == (2, 2)

        # All values should be finite (no inf or nan)
        assert np.isfinite(result.values).all()

        # With the +1.001 offset, even negative NBR values should give reasonable results
        assert (
            result.values > 0
        ).all()  # All should be positive since dNBR is positive

    @pytest.mark.asyncio
    async def test_calculate_raises_without_prefire_data(
        self,
        calculator: RBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test that calculation raises error when prefire data missing"""
        with pytest.raises(ValueError, match="RBR calculation requires prefire_data"):
            await calculator.calculate(None, mock_data, band_context)

    @pytest.mark.asyncio
    async def test_calculate_raises_without_postfire_data(
        self,
        calculator: RBRCalculator,
        mock_data: xr.DataArray,
        band_context: dict[str, Any],
    ) -> None:
        """Test that calculation raises error when postfire data missing"""
        with pytest.raises(ValueError, match="RBR calculation requires postfire_data"):
            await calculator.calculate(mock_data, None, band_context)
