"""
Test that our statistical warning fixes work correctly.

This focuses on testing the specific numpy RuntimeWarning fixes we implemented.
"""

import warnings
import numpy as np
import pandas as pd
from unittest.mock import Mock, patch

from src.process.resolve_veg import (
    add_percentage_columns,
    create_veg_json_structure,
    _get_empty_zonal_statistics,
    _get_fallback_zonal_statistics,
)


class TestStatisticalWarningsFix:
    """Test that our warning fixes work correctly."""

    def test_add_percentage_columns_safe_division(self):
        """Test that percentage calculations handle edge cases safely."""
        # Test with zero total area
        df_zero = pd.DataFrame({
            "total_ha": [0.0, 0.0],
            "unburned_ha": [0.0, 0.0],
            "low_ha": [0.0, 0.0],
            "moderate_ha": [0.0, 0.0],
            "high_ha": [0.0, 0.0],
        }, index=["Empty1", "Empty2"])

        result = add_percentage_columns(df_zero)

        # Should handle zeros without division by zero warnings
        assert not result["unburned_percent"].isna().any()
        assert not result["total_percent"].isna().any()
        assert all(result["total_percent"] == 0.0)

        # Test with NaN values
        df_nan = pd.DataFrame({
            "total_ha": [100.0, np.nan],
            "unburned_ha": [60.0, np.nan],
            "low_ha": [20.0, np.nan],
            "moderate_ha": [15.0, np.nan],
            "high_ha": [5.0, np.nan],
        }, index=["Normal", "NaN_Type"])

        result = add_percentage_columns(df_nan)

        # Should handle NaN values safely
        assert not result["unburned_percent"].isna().any()
        assert not result["total_percent"].isna().any()
        # NaN row should become 0.0 percentages
        assert result.loc["NaN_Type", "unburned_percent"] == 0.0

    def test_create_veg_json_structure_nan_handling(self):
        """Test JSON structure creation with NaN values."""
        df_with_nans = pd.DataFrame({
            "total_ha": [100.0, np.nan],
            "unburned_ha": [60.0, np.nan],
            "low_ha": [20.0, np.nan],
            "moderate_ha": [15.0, np.nan],
            "high_ha": [5.0, np.nan],
            "unburned_percent": [60.0, 0.0],  # Already processed by add_percentage_columns
            "low_percent": [20.0, 0.0],
            "moderate_percent": [15.0, 0.0],
            "high_percent": [5.0, 0.0],
            "unburned_mean": [0.05, np.nan],
            "low_mean": [0.15, np.nan],
            "moderate_mean": [0.40, np.nan],
            "high_mean": [0.80, np.nan],
            "unburned_std": [0.02, np.nan],
            "low_std": [0.05, np.nan],
            "moderate_std": [0.10, np.nan],
            "high_std": [0.15, np.nan],
        }, index=["Normal", "NaN_Type"])

        result = create_veg_json_structure(df_with_nans)

        assert "vegetation_communities" in result
        assert len(result["vegetation_communities"]) == 2

        # The normal community should be fine
        normal_community = next(c for c in result["vegetation_communities"] if c["name"] == "Normal")
        assert normal_community["total_hectares"] == 100.0
        assert normal_community["percent_of_park"] == 100.0  # Normal is 100% of total park

        # The NaN community should be handled gracefully
        nan_community = next(c for c in result["vegetation_communities"] if c["name"] == "NaN_Type")
        assert nan_community["total_hectares"] == 0.0  # NaN converted to 0.0
        assert nan_community["percent_of_park"] == 0.0

    def test_empty_zonal_statistics_structure(self):
        """Test that empty statistics have correct structure."""
        empty_stats = _get_empty_zonal_statistics()

        expected_keys = [
            "unburned_ha", "low_ha", "moderate_ha", "high_ha",
            "total_pixel_count",
            "unburned_mean", "low_mean", "moderate_mean", "high_mean",
            "unburned_std", "low_std", "moderate_std", "high_std",
            "mean_severity", "std_dev"
        ]

        assert all(key in empty_stats for key in expected_keys)
        assert all(val == 0.0 for val in empty_stats.values())
        assert all(isinstance(val, float) for val in empty_stats.values())

    def test_fallback_zonal_statistics_realistic_values(self):
        """Test that fallback statistics produce realistic values."""
        total_area_m2 = 2500.0  # 2500 m²
        pixel_area_ha = 0.01  # 100 m² per pixel = 0.01 ha

        fallback_stats = _get_fallback_zonal_statistics(total_area_m2, pixel_area_ha)

        # Should convert area correctly
        assert fallback_stats["unburned_ha"] == 0.25  # 2500 m² = 0.25 ha
        assert fallback_stats["total_pixel_count"] == 25.0  # 2500 / 100 = 25 pixels

        # Should assume unburned for small areas
        assert fallback_stats["mean_severity"] == 0.0
        assert fallback_stats["std_dev"] == 0.0
        assert all(fallback_stats[f"{sev}_ha"] == 0.0 for sev in ["low", "moderate", "high"])

    def test_no_warnings_during_percentage_calculations(self):
        """Test that no warnings are emitted during percentage calculations."""
        # Create data that might trigger warnings
        df_problematic = pd.DataFrame({
            "total_ha": [0.0, np.inf, -1.0, 1e-100],
            "unburned_ha": [0.0, 50.0, -0.5, 5e-101],
            "low_ha": [0.0, 30.0, -0.3, 3e-101],
            "moderate_ha": [0.0, 15.0, -0.15, 1.5e-101],
            "high_ha": [0.0, 5.0, -0.05, 0.5e-101],
        }, index=["Zero", "Inf", "Negative", "Tiny"])

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")

            result = add_percentage_columns(df_problematic)

            # Should not emit division warnings
            division_warnings = [w for w in warning_list
                               if "divide" in str(w.message).lower()
                               or "invalid" in str(w.message).lower()]
            assert len(division_warnings) == 0, f"Division warnings: {[str(w.message) for w in division_warnings]}"

            # Result should have valid percentage columns
            assert "unburned_percent" in result.columns
            assert "total_percent" in result.columns

    def test_vegetation_command_edge_case_methods(self):
        """Test VegetationResolveCommand edge case methods."""
        from src.commands.impl.vegetation_resolve_command import VegetationResolveCommand

        command = VegetationResolveCommand()

        # Test empty statistics
        empty_stats = command._get_empty_statistics()
        assert isinstance(empty_stats, dict)
        assert empty_stats["total_pixel_count"] == 0.0
        assert empty_stats["mean_severity"] == 0.0

        # Test fallback statistics
        mock_veg_subset = Mock()
        mock_veg_subset.geometry.area.sum.return_value = 100.0
        metadata = {"pixel_area_ha": 0.01}

        fallback_stats = command._get_fallback_statistics(mock_veg_subset, metadata)
        assert isinstance(fallback_stats, dict)
        assert fallback_stats["unburned_ha"] == 0.01  # 100 m² = 0.01 ha
        assert fallback_stats["total_pixel_count"] == 1.0

    def test_no_statistical_warnings_with_mocked_xvec(self):
        """Test that our warning suppression works with mocked xvec calls."""
        # This simulates the scenario where xvec.zonal_stats would trigger warnings
        with patch('src.process.resolve_veg.calculate_zonal_stats') as mock_calc:
            # Mock return value from calculate_zonal_stats
            mock_calc.return_value = {
                "unburned_ha": 10.0,
                "low_ha": 5.0,
                "moderate_ha": 3.0,
                "high_ha": 2.0,
                "total_pixel_count": 100,
                "mean_severity": 0.25,
                "std_dev": 0.3,
                "unburned_mean": 0.05,
                "low_mean": 0.15,
                "moderate_mean": 0.4,
                "high_mean": 0.8,
                "unburned_std": 0.02,
                "low_std": 0.05,
                "moderate_std": 0.1,
                "high_std": 0.15,
            }

            # Create simple test data with all required columns
            df = pd.DataFrame({
                "total_ha": [20.0],
                "unburned_ha": [10.0],
                "low_ha": [5.0],
                "moderate_ha": [3.0],
                "high_ha": [2.0],
                "unburned_mean": [0.05],
                "low_mean": [0.15],
                "moderate_mean": [0.4],
                "high_mean": [0.8],
                "unburned_std": [0.02],
                "low_std": [0.05],
                "moderate_std": [0.1],
                "high_std": [0.15],
            }, index=["Test_Veg"])

            with warnings.catch_warnings(record=True) as warning_list:
                warnings.simplefilter("always")

                # Test percentage calculations
                result_df = add_percentage_columns(df)

                # Test JSON structure creation
                result_json = create_veg_json_structure(result_df)

                # Should not emit any statistical warnings
                stat_warnings = [w for w in warning_list
                               if any(phrase in str(w.message).lower() for phrase in [
                                   "degrees of freedom", "invalid value", "divide"
                               ])]
                assert len(stat_warnings) == 0, f"Statistical warnings: {[str(w.message) for w in stat_warnings]}"

                # Results should be valid
                assert len(result_df) == 1
                assert "vegetation_communities" in result_json
                assert len(result_json["vegetation_communities"]) == 1