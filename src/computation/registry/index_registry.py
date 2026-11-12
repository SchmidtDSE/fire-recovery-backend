from typing import Dict, List, Optional
import logging
from src.computation.interfaces.index_calculator import IndexCalculator
from src.computation.impl.indices.nbr_calculator import NBRCalculator
from src.computation.impl.indices.dnbr_calculator import DNBRCalculator
from src.computation.impl.indices.rdnbr_calculator import RdNBRCalculator
from src.computation.impl.indices.rbr_calculator import RBRCalculator

logger = logging.getLogger(__name__)


class IndexRegistry:
    """Registry for managing index calculators with proper dependency injection"""

    def __init__(self) -> None:
        """Initialize the registry and set up default calculators"""
        logger.info("Initializing IndexRegistry")
        self._calculators: Dict[str, IndexCalculator] = {}
        self._setup_calculators()

    def _setup_calculators(self) -> None:
        """Initialize all available calculators with proper dependencies"""
        logger.info("Setting up index calculators with dependencies")

        # Create base calculator (no dependencies)
        nbr_calc = NBRCalculator()

        # Create calculators with NBR dependency
        dnbr_calc = DNBRCalculator(nbr_calc)

        # Create calculators with multiple dependencies
        rdnbr_calc = RdNBRCalculator(nbr_calc, dnbr_calc)
        rbr_calc = RBRCalculator(nbr_calc, dnbr_calc)

        # Register all calculators
        self._calculators = {
            nbr_calc.index_name: nbr_calc,
            dnbr_calc.index_name: dnbr_calc,
            rdnbr_calc.index_name: rdnbr_calc,
            rbr_calc.index_name: rbr_calc,
        }

        logger.info(
            f"Registered {len(self._calculators)} index calculators: {list(self._calculators.keys())}"
        )

    def get_calculator(self, index_name: str) -> Optional[IndexCalculator]:
        """Get calculator by index name

        Args:
            index_name: Name of the index (e.g., 'nbr', 'dnbr')

        Returns:
            IndexCalculator instance or None if not found
        """
        calculator = self._calculators.get(index_name)
        if calculator is None:
            logger.warning(
                f"Calculator for index '{index_name}' not found. Available: {list(self._calculators.keys())}"
            )
        return calculator

    def get_available_indices(self) -> List[str]:
        """Get list of all available index names

        Returns:
            List of available index names
        """
        return list(self._calculators.keys())

    def add_calculator(self, calculator: IndexCalculator) -> None:
        """Add new index calculator at runtime

        Args:
            calculator: IndexCalculator instance to add

        Raises:
            ValueError: If calculator with same name already exists
        """
        if calculator.index_name in self._calculators:
            raise ValueError(
                f"Calculator for index '{calculator.index_name}' already exists"
            )

        self._calculators[calculator.index_name] = calculator
        logger.info(f"Added new calculator for index '{calculator.index_name}'")

    def replace_calculator(self, index_name: str, calculator: IndexCalculator) -> None:
        """Replace existing calculator with new implementation

        Args:
            index_name: Name of index to replace
            calculator: New IndexCalculator instance

        Raises:
            ValueError: If index names don't match or calculator doesn't exist
        """
        if calculator.index_name != index_name:
            raise ValueError(
                f"Calculator index name '{calculator.index_name}' doesn't match requested '{index_name}'"
            )

        if index_name not in self._calculators:
            raise ValueError(
                f"Calculator for index '{index_name}' doesn't exist. Use add_calculator() instead"
            )

        old_calculator = self._calculators[index_name]
        self._calculators[index_name] = calculator
        logger.info(
            f"Replaced calculator for index '{index_name}': {type(old_calculator).__name__} -> {type(calculator).__name__}"
        )

    def remove_calculator(self, index_name: str) -> bool:
        """Remove calculator from registry

        Args:
            index_name: Name of index to remove

        Returns:
            True if calculator was removed, False if not found
        """
        if index_name in self._calculators:
            del self._calculators[index_name]
            logger.info(f"Removed calculator for index '{index_name}'")
            return True
        else:
            logger.warning(
                f"Calculator for index '{index_name}' not found, nothing to remove"
            )
            return False

    def get_calculators_requiring_both_periods(self) -> List[str]:
        """Get list of indices that require both pre and post fire data

        Returns:
            List of index names that need both periods
        """
        return [
            name
            for name, calc in self._calculators.items()
            if calc.requires_pre_and_post()
        ]

    def get_calculators_with_dependencies(self) -> Dict[str, List[str]]:
        """Get mapping of index names to their dependencies

        Returns:
            Dictionary mapping index names to list of dependency names
        """
        return {
            name: calc.get_dependencies() for name, calc in self._calculators.items()
        }

    def validate_dependencies(self) -> bool:
        """Validate that all calculator dependencies are satisfied

        Returns:
            True if all dependencies are satisfied, False otherwise
        """
        available_indices = set(self._calculators.keys())

        for name, calc in self._calculators.items():
            dependencies = set(calc.get_dependencies())
            missing_deps = dependencies - available_indices

            if missing_deps:
                logger.error(
                    f"Calculator '{name}' has unsatisfied dependencies: {missing_deps}"
                )
                return False

        logger.debug("All calculator dependencies are satisfied")
        return True
