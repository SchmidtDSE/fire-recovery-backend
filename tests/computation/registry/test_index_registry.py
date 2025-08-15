import pytest
from unittest.mock import Mock, patch
from typing import List, Dict, Any, Optional
import xarray as xr
from src.computation.registry.index_registry import IndexRegistry
from src.computation.interfaces.index_calculator import IndexCalculator
from src.computation.impl.indices.nbr_calculator import NBRCalculator
from src.computation.impl.indices.dnbr_calculator import DNBRCalculator
from src.computation.impl.indices.rdnbr_calculator import RdNBRCalculator
from src.computation.impl.indices.rbr_calculator import RBRCalculator


class MockCalculator(IndexCalculator):
    """Mock calculator for testing"""
    
    def __init__(self, name: str, requires_both: bool = False, dependencies: Optional[List[str]] = None):
        self._name = name
        self._requires_both = requires_both
        self._dependencies = dependencies or []
    
    @property
    def index_name(self) -> str:
        return self._name
    
    def requires_pre_and_post(self) -> bool:
        return self._requires_both
    
    def get_dependencies(self) -> List[str]:
        return self._dependencies
    
    async def calculate(
        self, 
        prefire_data: Optional[xr.DataArray], 
        postfire_data: Optional[xr.DataArray],
        context: Dict[str, Any]
    ) -> xr.DataArray:
        return xr.DataArray([1.0])


class TestIndexRegistry:
    """Test suite for IndexRegistry"""

    @pytest.fixture
    def registry(self) -> IndexRegistry:
        """Create IndexRegistry instance"""
        return IndexRegistry()

    def test_registry_initialization(self, registry: IndexRegistry) -> None:
        """Test that registry initializes with expected calculators"""
        available = registry.get_available_indices()
        
        # Should have all four default calculators
        expected_indices = {"nbr", "dnbr", "rdnbr", "rbr"}
        assert set(available) == expected_indices
        assert len(available) == 4

    def test_get_calculator_existing(self, registry: IndexRegistry) -> None:
        """Test getting existing calculator"""
        nbr_calc = registry.get_calculator("nbr")
        assert nbr_calc is not None
        assert isinstance(nbr_calc, NBRCalculator)
        assert nbr_calc.index_name == "nbr"

    def test_get_calculator_nonexistent(self, registry: IndexRegistry) -> None:
        """Test getting non-existent calculator returns None"""
        calc = registry.get_calculator("nonexistent")
        assert calc is None

    def test_get_available_indices(self, registry: IndexRegistry) -> None:
        """Test getting list of available indices"""
        indices = registry.get_available_indices()
        assert isinstance(indices, list)
        assert "nbr" in indices
        assert "dnbr" in indices
        assert "rdnbr" in indices
        assert "rbr" in indices

    def test_add_calculator_new(self, registry: IndexRegistry) -> None:
        """Test adding new calculator"""
        mock_calc = MockCalculator("test_index")
        
        registry.add_calculator(mock_calc)
        
        assert "test_index" in registry.get_available_indices()
        retrieved = registry.get_calculator("test_index")
        assert retrieved is mock_calc

    def test_add_calculator_duplicate_raises_error(self, registry: IndexRegistry) -> None:
        """Test that adding duplicate calculator raises ValueError"""
        mock_calc = MockCalculator("nbr")  # NBR already exists
        
        with pytest.raises(ValueError, match="Calculator for index 'nbr' already exists"):
            registry.add_calculator(mock_calc)

    def test_replace_calculator_existing(self, registry: IndexRegistry) -> None:
        """Test replacing existing calculator"""
        mock_calc = MockCalculator("nbr")
        original_calc = registry.get_calculator("nbr")
        
        registry.replace_calculator("nbr", mock_calc)
        
        replaced_calc = registry.get_calculator("nbr")
        assert replaced_calc is mock_calc
        assert replaced_calc is not original_calc

    def test_replace_calculator_nonexistent_raises_error(self, registry: IndexRegistry) -> None:
        """Test that replacing non-existent calculator raises ValueError"""
        mock_calc = MockCalculator("nonexistent")
        
        with pytest.raises(ValueError, match="Calculator for index 'nonexistent' doesn't exist"):
            registry.replace_calculator("nonexistent", mock_calc)

    def test_replace_calculator_name_mismatch_raises_error(self, registry: IndexRegistry) -> None:
        """Test that replacing with mismatched name raises ValueError"""
        mock_calc = MockCalculator("different_name")
        
        with pytest.raises(ValueError, match="Calculator index name 'different_name' doesn't match requested 'nbr'"):
            registry.replace_calculator("nbr", mock_calc)

    def test_remove_calculator_existing(self, registry: IndexRegistry) -> None:
        """Test removing existing calculator"""
        assert "nbr" in registry.get_available_indices()
        
        result = registry.remove_calculator("nbr")
        
        assert result is True
        assert "nbr" not in registry.get_available_indices()
        assert registry.get_calculator("nbr") is None

    def test_remove_calculator_nonexistent(self, registry: IndexRegistry) -> None:
        """Test removing non-existent calculator"""
        result = registry.remove_calculator("nonexistent")
        assert result is False

    def test_get_calculators_requiring_both_periods(self, registry: IndexRegistry) -> None:
        """Test getting calculators that require both periods"""
        requiring_both = registry.get_calculators_requiring_both_periods()
        
        # NBR should not require both periods
        assert "nbr" not in requiring_both
        
        # dNBR, RdNBR, and RBR should require both periods
        expected_both = {"dnbr", "rdnbr", "rbr"}
        assert set(requiring_both) == expected_both

    def test_get_calculators_with_dependencies(self, registry: IndexRegistry) -> None:
        """Test getting calculator dependencies mapping"""
        dependencies = registry.get_calculators_with_dependencies()
        
        assert isinstance(dependencies, dict)
        assert dependencies["nbr"] == []  # No dependencies
        assert dependencies["dnbr"] == ["nbr"]  # Depends on NBR
        assert set(dependencies["rdnbr"]) == {"nbr", "dnbr"}  # Depends on both
        assert set(dependencies["rbr"]) == {"nbr", "dnbr"}  # Depends on both

    def test_validate_dependencies_success(self, registry: IndexRegistry) -> None:
        """Test dependency validation with valid registry"""
        assert registry.validate_dependencies() is True

    def test_validate_dependencies_failure(self, registry: IndexRegistry) -> None:
        """Test dependency validation with missing dependencies"""
        # Remove NBR calculator to break dependencies
        registry.remove_calculator("nbr")
        
        assert registry.validate_dependencies() is False

    def test_validate_dependencies_with_custom_calculator(self, registry: IndexRegistry) -> None:
        """Test dependency validation with custom calculator having missing deps"""
        # Add calculator with non-existent dependency
        mock_calc = MockCalculator("custom", dependencies=["nonexistent"])
        registry.add_calculator(mock_calc)
        
        assert registry.validate_dependencies() is False

    def test_dependency_injection_in_setup(self, registry: IndexRegistry) -> None:
        """Test that calculators are properly injected with dependencies"""
        dnbr_calc = registry.get_calculator("dnbr")
        rdnbr_calc = registry.get_calculator("rdnbr")
        rbr_calc = registry.get_calculator("rbr")
        
        # Check that DNBRCalculator has NBRCalculator injected
        assert isinstance(dnbr_calc, DNBRCalculator)
        assert hasattr(dnbr_calc, 'nbr_calculator')
        assert isinstance(dnbr_calc.nbr_calculator, NBRCalculator)
        
        # Check that RdNBRCalculator has both calculators injected
        assert isinstance(rdnbr_calc, RdNBRCalculator)
        assert hasattr(rdnbr_calc, 'nbr_calculator')
        assert hasattr(rdnbr_calc, 'dnbr_calculator')
        assert isinstance(rdnbr_calc.nbr_calculator, NBRCalculator)
        assert isinstance(rdnbr_calc.dnbr_calculator, DNBRCalculator)
        
        # Check that RBRCalculator has both calculators injected
        assert isinstance(rbr_calc, RBRCalculator)
        assert hasattr(rbr_calc, 'nbr_calculator')
        assert hasattr(rbr_calc, 'dnbr_calculator')
        assert isinstance(rbr_calc.nbr_calculator, NBRCalculator)
        assert isinstance(rbr_calc.dnbr_calculator, DNBRCalculator)

    @patch('src.computation.registry.index_registry.logger')
    def test_logging_on_setup(self, mock_logger, registry: IndexRegistry) -> None:
        """Test that appropriate log messages are generated during setup"""
        # Registry is already created in fixture, so create a new one to trigger setup
        new_registry = IndexRegistry()
        
        # Check that info logs were called
        mock_logger.info.assert_called()
        
        # Verify log messages contain expected content
        call_args = [call[0][0] for call in mock_logger.info.call_args_list]
        setup_log = next((msg for msg in call_args if "Setting up index calculators" in msg), None)
        assert setup_log is not None
        
        registered_log = next((msg for msg in call_args if "Registered" in msg and "index calculators" in msg), None)
        assert registered_log is not None

    @patch('src.computation.registry.index_registry.logger')
    def test_logging_on_missing_calculator(self, mock_logger, registry: IndexRegistry) -> None:
        """Test that warning is logged when requesting missing calculator"""
        registry.get_calculator("nonexistent")
        
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "Calculator for index 'nonexistent' not found" in warning_msg

    def test_registry_is_independent(self) -> None:
        """Test that multiple registry instances are independent"""
        registry1 = IndexRegistry()
        registry2 = IndexRegistry()
        
        # Remove calculator from first registry
        registry1.remove_calculator("nbr")
        
        # Second registry should still have NBR
        assert registry1.get_calculator("nbr") is None
        assert registry2.get_calculator("nbr") is not None
        assert "nbr" not in registry1.get_available_indices()
        assert "nbr" in registry2.get_available_indices()

    def test_calculator_instances_are_shared_correctly(self, registry: IndexRegistry) -> None:
        """Test that dependent calculators share the same NBR instance"""
        dnbr_calc = registry.get_calculator("dnbr")
        rdnbr_calc = registry.get_calculator("rdnbr")
        rbr_calc = registry.get_calculator("rbr")
        
        # All should share the same NBR calculator instance
        assert dnbr_calc.nbr_calculator is rdnbr_calc.nbr_calculator
        assert dnbr_calc.nbr_calculator is rbr_calc.nbr_calculator
        
        # RdNBR and RBR should share the same dNBR calculator instance
        assert rdnbr_calc.dnbr_calculator is rbr_calc.dnbr_calculator