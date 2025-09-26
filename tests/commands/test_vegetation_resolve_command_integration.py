"""
Integration tests for VegetationResolveCommand with real statistical edge cases.

These tests focus on scenarios that trigger numpy warnings and validate proper handling.
"""

import pytest
import warnings
import numpy as np
import pandas as pd
import geopandas as gpd
import xarray as xr
import rioxarray  # noqa: F401 - Required for .rio accessor in xarray operations
from unittest.mock import Mock, AsyncMock, patch
from shapely.geometry import Polygon
from geojson_pydantic import Polygon as GeoJSONPolygon

from src.commands.impl.vegetation_resolve_command import VegetationResolveCommand
from src.commands.interfaces.command_context import CommandContext


class TestVegetationResolveCommandRealEdgeCases:
    """Test real edge cases that trigger statistical warnings."""

    @pytest.fixture
    def command(self):
        """Create VegetationResolveCommand instance."""
        return VegetationResolveCommand()

    @pytest.fixture
    def mock_context(self):
        """Create mock CommandContext for testing."""
        mock_storage = Mock()
        mock_storage.copy_from_url = AsyncMock()
        mock_storage.get_bytes = AsyncMock()
        mock_storage.save_bytes = AsyncMock(return_value="mock://url")

        mock_storage_factory = Mock()
        mock_stac_manager = Mock()
        mock_stac_manager.get_items_by_id_and_coarseness = AsyncMock(return_value=None)
        mock_stac_manager.get_item_by_id = AsyncMock(return_value=None)
        mock_stac_manager.create_veg_matrix_item = AsyncMock()

        mock_index_registry = Mock()

        geometry = GeoJSONPolygon(
            type="Polygon",
            coordinates=[[
                [0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]
            ]]
        )

        return CommandContext(
            job_id="test_edge_case",
            fire_event_name="edge_fire",
            geometry=geometry,
            storage=mock_storage,
            storage_factory=mock_storage_factory,
            stac_manager=mock_stac_manager,
            index_registry=mock_index_registry,
            severity_breaks=[0.1, 0.27, 0.66],
            metadata={
                "veg_gpkg_url": "https://example.com/vegetation.gpkg",
                "fire_cog_url": "https://example.com/fire.tif",
                "geojson_url": "https://example.com/boundary.geojson",
            },
        )

    @pytest.fixture
    def tiny_fire_data(self):
        """Create tiny fire dataset that triggers statistical edge cases."""
        # Create minimal 3x3 fire dataset with identical values (triggers std warnings)
        data = np.full((3, 3), 0.15, dtype=np.float32)  # All same value
        data[1, 1] = 0.16  # Slight variation to avoid complete uniformity

        fire_array = xr.DataArray(
            data,
            coords={"y": [0, 1, 2], "x": [0, 1, 2]},
            dims=["y", "x"],
            name="band_data"
        )

        fire_ds = xr.Dataset({"band_data": fire_array})
        fire_ds = fire_ds.rio.write_crs("EPSG:32611")

        # Mock transform
        from rasterio.transform import from_bounds
        fire_ds = fire_ds.rio.write_transform(from_bounds(0, 0, 3, 3, 3, 3))

        return fire_ds

    @pytest.fixture
    def tiny_vegetation_gdf(self):
        """Create vegetation GeoDataFrame with tiny polygons."""
        # Create polygons smaller than a single pixel
        tiny_polygon = Polygon([(0.1, 0.1), (0.9, 0.1), (0.9, 0.9), (0.1, 0.9), (0.1, 0.1)])

        return gpd.GeoDataFrame(
            {"veg_type": ["Micro Shrub"], "geometry": [tiny_polygon]},
            crs="EPSG:32611"
        )

    @pytest.fixture
    def empty_vegetation_gdf(self):
        """Create completely empty vegetation GeoDataFrame."""
        return gpd.GeoDataFrame(
            columns=["veg_type", "geometry"],
            crs="EPSG:32611"
        )

    @pytest.fixture
    def boundary_gdf(self):
        """Create boundary GeoDataFrame."""
        boundary_polygon = Polygon([(0, 0), (3, 0), (3, 3), (0, 3), (0, 0)])
        return gpd.GeoDataFrame(
            {"geometry": [boundary_polygon]},
            crs="EPSG:32611"
        )

    @pytest.mark.asyncio
    async def test_calculate_zonal_stats_empty_vegetation(
        self, command, tiny_fire_data, boundary_gdf, empty_vegetation_gdf
    ):
        """Test zonal statistics calculation with empty vegetation data."""
        # Create masks
        masks = await command._create_severity_masks(
            tiny_fire_data["band_data"], [0.1, 0.27, 0.66], boundary_gdf
        )

        metadata = {
            "pixel_area_ha": 0.01,
            "x_coord": "x",
            "y_coord": "y"
        }

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")

            result = await command._calculate_zonal_statistics(
                masks, empty_vegetation_gdf, metadata
            )

            # Check that no statistical warnings were raised
            stat_warnings = [w for w in warning_list
                           if "degrees of freedom" in str(w.message).lower()]
            assert len(stat_warnings) == 0, f"Statistical warnings: {[str(w.message) for w in stat_warnings]}"

            # Should return empty statistics
            assert result == command._get_empty_statistics()

    @pytest.mark.asyncio
    async def test_calculate_zonal_stats_tiny_vegetation(
        self, command, tiny_fire_data, boundary_gdf, tiny_vegetation_gdf
    ):
        """Test zonal statistics calculation with very small vegetation polygons."""
        # Create masks
        masks = await command._create_severity_masks(
            tiny_fire_data["band_data"], [0.1, 0.27, 0.66], boundary_gdf
        )

        metadata = {
            "pixel_area_ha": 0.01,
            "x_coord": "x",
            "y_coord": "y"
        }

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")

            result = await command._calculate_zonal_statistics(
                masks, tiny_vegetation_gdf, metadata
            )

            # Check that no statistical warnings were raised
            stat_warnings = [w for w in warning_list
                           if any(phrase in str(w.message).lower() for phrase in [
                               "degrees of freedom",
                               "invalid value encountered",
                               "ddof"
                           ])]
            assert len(stat_warnings) == 0, f"Statistical warnings: {[str(w.message) for w in stat_warnings]}"

            # Should return valid statistics (even if small)
            assert isinstance(result, dict)
            assert "total_pixel_count" in result
            assert "mean_severity" in result
            assert not np.isnan(result["mean_severity"])
            assert not np.isnan(result["std_dev"])

    @pytest.mark.asyncio
    async def test_analyze_vegetation_impact_with_edge_cases(
        self, command, mock_context
    ):
        """Test complete vegetation impact analysis with edge case data."""
        # Create file data that will trigger edge cases
        file_data = {
            "vegetation": b"mock_tiny_vegetation",
            "fire_severity": b"mock_tiny_fire",
            "boundary": b"mock_boundary"
        }

        with (
            patch.object(command, "_load_fire_data_from_bytes") as mock_load_fire,
            patch.object(command, "_load_vegetation_data_from_bytes") as mock_load_veg,
            patch.object(command, "_load_boundary_data_from_bytes") as mock_load_boundary,
            patch("src.commands.impl.vegetation_resolve_command.gpd.clip") as mock_clip,
        ):
            # Setup mocks for edge case scenario with proper xarray mock
            # Create a realistic mock fire data array that supports comparison operations
            mock_fire_data = np.full((3, 3), 0.15, dtype=np.float32)
            mock_fire_array = xr.DataArray(
                mock_fire_data,
                coords={"y": [0, 1, 2], "x": [0, 1, 2]},
                dims=["y", "x"],
                name="band_data"
            )
            mock_fire_ds = xr.Dataset({"band_data": mock_fire_array})
            # Set CRS for rio operations
            from rasterio.transform import from_bounds
            mock_fire_ds = mock_fire_ds.rio.write_crs("EPSG:32611")
            mock_fire_ds = mock_fire_ds.rio.write_transform(from_bounds(0, 0, 3, 3, 3, 3))

            metadata = {
                "crs": "EPSG:32611",
                "data_var": "band_data",
                "pixel_area_ha": 0.01,
                "x_coord": "x",
                "y_coord": "y"
            }
            mock_load_fire.return_value = (mock_fire_ds, metadata)

            # Empty vegetation after clipping
            empty_gdf = gpd.GeoDataFrame(columns=["veg_type", "geometry"], crs="EPSG:32611")
            mock_load_veg.return_value = empty_gdf
            mock_clip.return_value = empty_gdf

            # Create a proper boundary GeoDataFrame instead of Mock
            boundary_polygon = Polygon([(0, 0), (3, 0), (3, 3), (0, 3), (0, 0)])
            mock_boundary_gdf = gpd.GeoDataFrame(
                {"geometry": [boundary_polygon]},
                crs="EPSG:32611"
            )
            mock_load_boundary.return_value = mock_boundary_gdf

            with warnings.catch_warnings(record=True) as warning_list:
                warnings.simplefilter("always")

                result_df, json_structure = await command._analyze_vegetation_impact(
                    file_data, [0.1, 0.27, 0.66], None
                )

                # Check warnings
                stat_warnings = [w for w in warning_list
                               if "degrees of freedom" in str(w.message).lower()]
                assert len(stat_warnings) == 0, f"Statistical warnings: {[str(w.message) for w in stat_warnings]}"

                # Should handle empty case gracefully
                assert len(result_df) == 0
                assert json_structure == {"vegetation_communities": []}

    @pytest.mark.asyncio
    async def test_analyze_vegetation_impact_with_very_small_polygons(
        self, command, mock_context
    ):
        """Test vegetation impact analysis with very small polygons that trigger fallback."""
        file_data = {
            "vegetation": b"mock_tiny_vegetation",
            "fire_severity": b"mock_fire",
            "boundary": b"mock_boundary"
        }

        with (
            patch.object(command, "_load_fire_data_from_bytes") as mock_load_fire,
            patch.object(command, "_load_vegetation_data_from_bytes") as mock_load_veg,
            patch.object(command, "_load_boundary_data_from_bytes") as mock_load_boundary,
            patch("src.commands.impl.vegetation_resolve_command.gpd.clip") as mock_clip,
            patch.object(command, "_create_severity_masks") as mock_create_masks,
        ):
            # Setup fire data with proper xarray mock
            mock_fire_data = np.full((10, 10), 0.15, dtype=np.float32)
            mock_fire_array = xr.DataArray(
                mock_fire_data,
                coords={"y": range(10), "x": range(10)},
                dims=["y", "x"],
                name="band_data"
            )
            mock_fire_ds = xr.Dataset({"band_data": mock_fire_array})
            # Set CRS for rio operations
            from rasterio.transform import from_bounds
            mock_fire_ds = mock_fire_ds.rio.write_crs("EPSG:32611")
            mock_fire_ds = mock_fire_ds.rio.write_transform(from_bounds(0, 0, 10, 10, 10, 10))

            metadata = {
                "crs": "EPSG:32611",
                "data_var": "band_data",
                "pixel_area_ha": 0.01,
                "x_coord": "x",
                "y_coord": "y"
            }
            mock_load_fire.return_value = (mock_fire_ds, metadata)

            # Create tiny vegetation polygon
            tiny_polygon = Polygon([(0, 0), (1e-8, 0), (1e-8, 1e-8), (0, 1e-8), (0, 0)])
            tiny_gdf = gpd.GeoDataFrame(
                {"veg_type": ["Tiny Plant"], "geometry": [tiny_polygon]},
                crs="EPSG:32611"
            )

            # Mock area calculation using a different approach - patch the property
            with patch.object(type(tiny_gdf.geometry), 'area', new_callable=lambda: Mock()) as mock_area:
                mock_area.sum.return_value = 1e-9  # Very small area

                mock_load_veg.return_value = tiny_gdf
                mock_clip.return_value = tiny_gdf

                # Create a proper boundary GeoDataFrame instead of Mock
                boundary_polygon = Polygon([(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)])
                mock_boundary_gdf = gpd.GeoDataFrame(
                    {"geometry": [boundary_polygon]},
                    crs="EPSG:32611"
                )
                mock_load_boundary.return_value = mock_boundary_gdf

                # Create proper mock masks that can work with the test
                mock_create_masks.return_value = {
                    "unburned": xr.DataArray(np.ones((10, 10)), dims=["y", "x"]),
                    "low": xr.DataArray(np.zeros((10, 10)), dims=["y", "x"]),
                    "moderate": xr.DataArray(np.zeros((10, 10)), dims=["y", "x"]),
                    "high": xr.DataArray(np.zeros((10, 10)), dims=["y", "x"]),
                    "original": xr.DataArray(np.full((10, 10), 0.15), dims=["y", "x"])
                }

                with warnings.catch_warnings(record=True) as warning_list:
                    warnings.simplefilter("always")

                    # Mock _calculate_zonal_statistics to simulate the actual fallback behavior
                    with patch.object(command, "_calculate_zonal_statistics") as mock_calc_stats:
                        # Return fallback statistics for tiny area
                        mock_calc_stats.return_value = command._get_fallback_statistics(tiny_gdf, metadata)

                        result_df, json_structure = await command._analyze_vegetation_impact(
                            file_data, [0.1, 0.27, 0.66], None
                        )

                        # Check warnings
                        stat_warnings = [w for w in warning_list
                                       if "degrees of freedom" in str(w.message).lower()]
                        assert len(stat_warnings) == 0, f"Statistical warnings: {[str(w.message) for w in stat_warnings]}"

                        # Should have used fallback statistics
                        assert len(result_df) == 1
                        assert "Tiny Plant" in result_df.index
                        # Fallback should assign to unburned category
                        assert result_df.loc["Tiny Plant", "unburned_ha"] > 0

    @pytest.mark.asyncio
    async def test_percentage_calculation_with_edge_values(self, command):
        """Test percentage calculations with edge case values."""
        # Test with very small values
        df = pd.DataFrame({
            "total_ha": [1e-10, 0.0, 1e-6],
            "unburned_ha": [5e-11, 0.0, 5e-7],
            "low_ha": [3e-11, 0.0, 3e-7],
            "moderate_ha": [1e-11, 0.0, 1e-7],
            "high_ha": [1e-11, 0.0, 1e-7],
        }, index=["Tiny", "Zero", "Small"])

        result = command._add_percentage_columns(df)

        # Should handle edge cases without errors
        assert not result["unburned_percent"].isna().any()
        assert not result["total_percent"].isna().any()
        assert all(result["unburned_percent"] >= 0)
        assert all(result["unburned_percent"] <= 100)

        # Zero total area should result in zero percentages
        assert result.loc["Zero", "unburned_percent"] == 0.0
        assert result.loc["Zero", "total_percent"] == 0.0

    @pytest.mark.asyncio
    async def test_json_structure_with_nan_values(self, command):
        """Test JSON structure creation with NaN and edge values."""
        df = pd.DataFrame({
            "total_ha": [100.0, np.nan, 0.0, 1e-10],
            "unburned_ha": [60.0, np.nan, 0.0, 5e-11],
            "low_ha": [20.0, np.nan, 0.0, 3e-11],
            "moderate_ha": [15.0, np.nan, 0.0, 1e-11],
            "high_ha": [5.0, np.nan, 0.0, 1e-11],
            "unburned_percent": [60.0, np.nan, 0.0, 50.0],
            "low_percent": [20.0, np.nan, 0.0, 30.0],
            "moderate_percent": [15.0, np.nan, 0.0, 10.0],
            "high_percent": [5.0, np.nan, 0.0, 10.0],
            "unburned_mean": [0.05, np.nan, 0.0, 0.01],
            "low_mean": [0.15, np.nan, 0.0, 0.12],
            "moderate_mean": [0.40, np.nan, 0.0, 0.35],
            "high_mean": [0.80, np.nan, 0.0, 0.75],
            "unburned_std": [0.02, np.nan, 0.0, 0.01],
            "low_std": [0.05, np.nan, 0.0, 0.03],
            "moderate_std": [0.10, np.nan, 0.0, 0.08],
            "high_std": [0.15, np.nan, 0.0, 0.12],
        }, index=["Normal", "NaN_Type", "Zero_Type", "Tiny_Type"])

        result = command._create_json_structure(df)

        # Should handle all edge cases gracefully
        assert "vegetation_communities" in result
        assert len(result["vegetation_communities"]) == 4

        for community in result["vegetation_communities"]:
            # All values should be finite numbers
            assert isinstance(community["total_hectares"], (int, float))
            assert isinstance(community["percent_of_park"], (int, float))
            assert not np.isnan(community["total_hectares"])
            assert not np.isnan(community["percent_of_park"])
            assert community["total_hectares"] >= 0
            assert community["percent_of_park"] >= 0

            # Check severity breakdown values
            for severity_data in community["severity_breakdown"].values():
                assert not np.isnan(severity_data["hectares"])
                assert not np.isnan(severity_data["percent"])
                assert not np.isnan(severity_data["mean_severity"])
                assert not np.isnan(severity_data["std_dev"])

    def test_empty_statistics_structure(self, command):
        """Test that empty statistics have proper structure."""
        empty_stats = command._get_empty_statistics()

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

    def test_fallback_statistics_realistic_values(self, command):
        """Test that fallback statistics produce realistic values."""
        # Create a real GeoDataFrame for testing fallback statistics
        test_polygon = Polygon([(0, 0), (50, 0), (50, 50), (0, 50), (0, 0)])  # 2500 m² polygon
        mock_veg_subset = gpd.GeoDataFrame(
            {"veg_type": ["Test Vegetation"], "geometry": [test_polygon]},
            crs="EPSG:32611"  # UTM projection where units are meters
        )

        metadata = {"pixel_area_ha": 0.01}  # 100 m² per pixel = 0.01 ha

        fallback_stats = command._get_fallback_statistics(mock_veg_subset, metadata)

        # Should convert area correctly
        assert fallback_stats["unburned_ha"] == 0.25  # 2500 m² = 0.25 ha
        assert fallback_stats["total_pixel_count"] == 25.0  # 2500 / 100 = 25 pixels

        # Other values should be reasonable defaults
        assert fallback_stats["mean_severity"] == 0.0  # Assume unburned
        assert fallback_stats["std_dev"] == 0.0
        assert all(fallback_stats[f"{sev}_ha"] == 0.0 for sev in ["low", "moderate", "high"])


class TestWarningSuppressionIntegration:
    """Test that warning suppression works correctly in integration scenarios."""

    @pytest.mark.asyncio
    async def test_no_warnings_in_complete_workflow(self):
        """Test that no statistical warnings are emitted in a complete workflow."""
        from src.commands.impl.vegetation_resolve_command import VegetationResolveCommand

        # This is an integration test that checks the complete workflow
        command = VegetationResolveCommand()

        # Mock the entire workflow to focus on warning suppression
        with (
            patch.object(command, "_download_input_files") as mock_download,
            patch.object(command, "_analyze_vegetation_impact") as mock_analyze,
            patch.object(command, "_save_analysis_reports") as mock_save,
            patch.object(command, "_create_vegetation_stac_metadata") as mock_stac,
        ):
            # Setup mocks to simulate edge case processing
            mock_download.return_value = {
                "vegetation": b"edge_case_data",
                "fire_severity": b"edge_case_fire",
                "boundary": b"edge_case_boundary"
            }

            # Simulate processing that might trigger warnings
            edge_case_df = pd.DataFrame({
                "total_ha": [1e-10],  # Very small value
                "unburned_ha": [1e-10],
                "low_ha": [0.0],
                "moderate_ha": [0.0],
                "high_ha": [0.0],
            }, index=["Edge Case Veg"])

            mock_analyze.return_value = (edge_case_df, {"vegetation_communities": []})
            mock_save.return_value = {"csv": "url", "json": "url"}
            mock_stac.return_value = "stac://url"

            # Create minimal context
            mock_context = Mock()
            mock_context.job_id = "test"
            mock_context.fire_event_name = "test"
            mock_context.severity_breaks = [0.1, 0.27, 0.66]
            mock_context.get_metadata.side_effect = lambda key: {
                "veg_gpkg_url": "https://example.com/veg.gpkg",
                "fire_cog_url": "https://example.com/fire.tif",
                "geojson_url": "https://example.com/boundary.geojson"
            }.get(key)

            with warnings.catch_warnings(record=True) as warning_list:
                warnings.simplefilter("always")

                result = await command.execute(mock_context)

                # Check that no statistical warnings were emitted during execution
                stat_warnings = [w for w in warning_list
                               if any(phrase in str(w.message).lower() for phrase in [
                                   "degrees of freedom",
                                   "invalid value encountered",
                                   "ddof"
                               ])]
                assert len(stat_warnings) == 0, f"Statistical warnings in workflow: {[str(w.message) for w in stat_warnings]}"

                # Execution should succeed
                assert result.is_success()