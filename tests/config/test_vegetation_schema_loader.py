"""
Comprehensive unit tests for VegetationSchemaLoader.

Tests cover configuration loading, caching, error handling, and thread safety.
"""

import json
import pytest
import tempfile
import threading
from pathlib import Path
from unittest.mock import patch
from typing import Dict, Any

from src.config.vegetation_schema_loader import VegetationSchemaLoader, VegetationSchemaConfig
from src.config.vegetation_schemas import VegetationSchema


@pytest.fixture
def sample_config_data() -> Dict[str, Any]:
    """Sample configuration data for testing."""
    return {
        "park_units": [
            {
                "id": "JOTR",
                "name": "Joshua Tree National Park",
                "layer_name": "JOTR_VegPolys",
                "vegetation_type_field": "MapUnit_Name",
                "description_field": "MapUnit_Name",
                "geometry_column": "geometry",
                "preserve_fields": ["OBJECTID", "Shape_Area", "Shape_Length"]
            },
            {
                "id": "MOJN",
                "name": "Mojave National Preserve", 
                "layer_name": None,
                "vegetation_type_field": "MAP_DESC",
                "description_field": "MAP_DESC",
                "geometry_column": "geometry",
                "preserve_fields": ["FID", "AREA", "PERIMETER"]
            },
            {
                "id": "DEFAULT",
                "name": "Default Vegetation Schema",
                "layer_name": None,
                "vegetation_type_field": "veg_type",
                "description_field": None,
                "geometry_column": "geometry",
                "preserve_fields": None
            }
        ]
    }


@pytest.fixture
def temp_config_file(sample_config_data):
    """Create a temporary configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(sample_config_data, f)
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


@pytest.fixture
def invalid_config_file():
    """Create a temporary invalid configuration file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        f.write("invalid json content")
        temp_path = f.name
    
    yield temp_path
    
    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


class TestVegetationSchemaConfig:
    """Test VegetationSchemaConfig class."""

    def test_load_from_file_success(self, temp_config_file):
        """Test successful loading of configuration from file."""
        config = VegetationSchemaConfig.load_from_file(temp_config_file)
        
        assert isinstance(config, VegetationSchemaConfig)
        park_units = config.get_park_units()
        assert len(park_units) == 3
        
        # Verify JOTR configuration
        jotr = next(unit for unit in park_units if unit["id"] == "JOTR")
        assert jotr["name"] == "Joshua Tree National Park"
        assert jotr["layer_name"] == "JOTR_VegPolys"
        assert jotr["vegetation_type_field"] == "MapUnit_Name"

    def test_load_from_file_not_found(self):
        """Test loading from non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError) as exc_info:
            VegetationSchemaConfig.load_from_file("non_existent_file.json")
        
        assert "Configuration file non_existent_file.json not found" in str(exc_info.value)

    def test_load_from_file_invalid_json(self, invalid_config_file):
        """Test loading invalid JSON raises JSONDecodeError."""
        with pytest.raises(json.JSONDecodeError):
            VegetationSchemaConfig.load_from_file(invalid_config_file)

    def test_load_from_file_missing_park_units(self):
        """Test loading configuration without park_units key."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump({"invalid": "structure"}, f)
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError) as exc_info:
                VegetationSchemaConfig.load_from_file(temp_path)
            
            assert "must contain 'park_units' array" in str(exc_info.value)
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_load_from_file_invalid_park_unit_structure(self):
        """Test loading configuration with invalid park unit structure."""
        invalid_data = {
            "park_units": [
                {
                    # Missing required fields
                    "id": "TEST"
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(invalid_data, f)
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError) as exc_info:
                VegetationSchemaConfig.load_from_file(temp_path)
            
            assert "missing required field" in str(exc_info.value)
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_load_from_file_invalid_park_unit_id(self):
        """Test loading configuration with invalid park unit ID."""
        invalid_data = {
            "park_units": [
                {
                    "id": "",  # Invalid empty ID
                    "name": "Test Park",
                    "vegetation_type_field": "test_field",
                    "geometry_column": "geometry"
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(invalid_data, f)
            temp_path = f.name
        
        try:
            with pytest.raises(ValueError) as exc_info:
                VegetationSchemaConfig.load_from_file(temp_path)
            
            assert "invalid 'id' field" in str(exc_info.value)
        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestVegetationSchemaLoader:
    """Test VegetationSchemaLoader class."""

    def setup_method(self):
        """Reset singleton instance before each test."""
        VegetationSchemaLoader._instance = None

    def test_get_instance_singleton(self, temp_config_file):
        """Test that get_instance returns a singleton."""
        loader1 = VegetationSchemaLoader.get_instance(temp_config_file)
        loader2 = VegetationSchemaLoader.get_instance(temp_config_file)
        
        assert loader1 is loader2

    def test_get_schema_success(self, temp_config_file):
        """Test successful schema retrieval."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        schema = loader.get_schema("JOTR")
        
        assert isinstance(schema, VegetationSchema)
        assert schema.layer_name == "JOTR_VegPolys"
        assert schema.vegetation_type_field == "MapUnit_Name"
        assert schema.description_field == "MapUnit_Name"
        assert schema.geometry_column == "geometry"
        assert schema.preserve_fields == ["OBJECTID", "Shape_Area", "Shape_Length"]

    def test_get_schema_mojn(self, temp_config_file):
        """Test retrieval of MOJN schema with null layer_name."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        schema = loader.get_schema("MOJN")
        
        assert isinstance(schema, VegetationSchema)
        assert schema.layer_name is None
        assert schema.vegetation_type_field == "MAP_DESC"
        assert schema.description_field == "MAP_DESC"
        assert schema.preserve_fields == ["FID", "AREA", "PERIMETER"]

    def test_get_schema_not_found(self, temp_config_file):
        """Test schema retrieval for non-existent park unit."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        with pytest.raises(ValueError) as exc_info:
            loader.get_schema("NONEXISTENT")
        
        assert "No vegetation schema found for park unit 'NONEXISTENT'" in str(exc_info.value)
        assert "Available park units:" in str(exc_info.value)

    def test_get_schema_invalid_id(self, temp_config_file):
        """Test schema retrieval with invalid park unit ID."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        with pytest.raises(ValueError) as exc_info:
            loader.get_schema("")
        
        assert "Invalid park unit ID" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            loader.get_schema(None)
        
        assert "Invalid park unit ID" in str(exc_info.value)

    def test_list_available_parks(self, temp_config_file):
        """Test listing available park units."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        parks = loader.list_available_parks()
        
        assert isinstance(parks, list)
        assert len(parks) == 3
        assert "JOTR" in parks
        assert "MOJN" in parks
        assert "DEFAULT" in parks

    def test_has_schema(self, temp_config_file):
        """Test checking if schema exists."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        assert loader.has_schema("JOTR") is True
        assert loader.has_schema("MOJN") is True
        assert loader.has_schema("NONEXISTENT") is False
        assert loader.has_schema("") is False
        assert loader.has_schema(None) is False

    def test_get_default_schema(self, temp_config_file):
        """Test retrieval of default schema."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        schema = loader.get_default_schema()
        
        assert isinstance(schema, VegetationSchema)
        assert schema.vegetation_type_field == "veg_type"
        assert schema.layer_name is None
        assert schema.preserve_fields is None

    def test_get_default_schema_not_configured(self):
        """Test default schema retrieval when not configured."""
        # Create config without DEFAULT schema
        config_data = {
            "park_units": [
                {
                    "id": "JOTR",
                    "name": "Joshua Tree National Park",
                    "layer_name": "JOTR_VegPolys",
                    "vegetation_type_field": "MapUnit_Name",
                    "description_field": "MapUnit_Name",
                    "geometry_column": "geometry",
                    "preserve_fields": ["OBJECTID"]
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name
        
        try:
            loader = VegetationSchemaLoader(temp_path)
            
            with pytest.raises(ValueError) as exc_info:
                loader.get_default_schema()
            
            assert "No default vegetation schema is configured" in str(exc_info.value)
        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_get_schema_or_default_with_valid_id(self, temp_config_file):
        """Test get_schema_or_default with valid park unit ID."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        schema = loader.get_schema_or_default("JOTR")
        
        assert schema.vegetation_type_field == "MapUnit_Name"

    def test_get_schema_or_default_with_invalid_id(self, temp_config_file):
        """Test get_schema_or_default with invalid park unit ID returns default."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        schema = loader.get_schema_or_default("NONEXISTENT")
        
        assert schema.vegetation_type_field == "veg_type"  # Default schema

    def test_get_schema_or_default_with_none(self, temp_config_file):
        """Test get_schema_or_default with None returns default."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        schema = loader.get_schema_or_default(None)
        
        assert schema.vegetation_type_field == "veg_type"  # Default schema

    def test_reload_schemas(self, temp_config_file):
        """Test schema cache reload functionality."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        # Load schemas initially
        schema1 = loader.get_schema("JOTR")
        
        # Reload schemas
        loader.reload_schemas()
        
        # Load schemas again
        schema2 = loader.get_schema("JOTR")
        
        # Should be equivalent but potentially different objects due to reload
        assert schema1.vegetation_type_field == schema2.vegetation_type_field
        assert schema1.layer_name == schema2.layer_name

    def test_caching_behavior(self, temp_config_file):
        """Test that schemas are cached and not reloaded unnecessarily."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        with patch.object(loader, '_load_schemas') as mock_load:
            mock_load.return_value = {
                "JOTR": VegetationSchema(vegetation_type_field="test", geometry_column="geom")
            }
            
            # First call should trigger loading
            schema1 = loader.get_schema("JOTR")
            assert mock_load.call_count == 1
            
            # Second call should use cache
            schema2 = loader.get_schema("JOTR")
            assert mock_load.call_count == 1  # Still 1, not called again
            
            # Should return the same object from cache
            assert schema1 is schema2

    def test_thread_safety(self, temp_config_file):
        """Test thread safety of schema loading and caching."""
        loader = VegetationSchemaLoader(temp_config_file)
        results = []
        errors = []
        
        def load_schema():
            try:
                schema = loader.get_schema("JOTR")
                results.append(schema)
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=load_schema)
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Verify results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 10
        
        # All results should be the same schema
        first_schema = results[0]
        for schema in results[1:]:
            assert schema is first_schema  # Same object from cache

    def test_file_not_found_error(self):
        """Test behavior when configuration file is not found."""
        loader = VegetationSchemaLoader("nonexistent_file.json")
        
        with pytest.raises(FileNotFoundError):
            loader.get_schema("JOTR")

    def test_configuration_file_changes(self, temp_config_file):
        """Test behavior when configuration file is modified."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        # Load initial schema
        schema1 = loader.get_schema("JOTR")
        assert schema1.vegetation_type_field == "MapUnit_Name"
        
        # Modify configuration file
        modified_config = {
            "park_units": [
                {
                    "id": "JOTR",
                    "name": "Modified Joshua Tree",
                    "layer_name": "Modified_Layer",
                    "vegetation_type_field": "Modified_Field",
                    "description_field": "Modified_Desc",
                    "geometry_column": "geometry",
                    "preserve_fields": []
                }
            ]
        }
        
        with open(temp_config_file, 'w') as f:
            json.dump(modified_config, f)
        
        # Reload schemas
        loader.reload_schemas()
        
        # Load schema again
        schema2 = loader.get_schema("JOTR")
        assert schema2.vegetation_type_field == "Modified_Field"

    @patch('src.config.vegetation_schema_loader.logger')
    def test_logging_behavior(self, mock_logger, temp_config_file):
        """Test that appropriate logging occurs."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        # Test successful schema loading
        loader.get_schema("JOTR")
        
        # Verify debug logging occurred
        mock_logger.debug.assert_called()
        
        # Test error logging for non-existent schema
        with pytest.raises(ValueError):
            loader.get_schema("NONEXISTENT")
        
        mock_logger.error.assert_called()


class TestIntegration:
    """Integration tests for VegetationSchemaLoader with actual configuration."""

    def test_load_actual_config_file(self):
        """Test loading the actual configuration file from the project."""
        config_path = "config/vegetation_schemas.json"
        
        # Skip test if actual config file doesn't exist
        if not Path(config_path).exists():
            pytest.skip(f"Configuration file {config_path} not found")
        
        loader = VegetationSchemaLoader(config_path)
        
        # Test that we can load known schemas
        available_parks = loader.list_available_parks()
        assert len(available_parks) > 0
        
        # Test loading each available schema
        for park_id in available_parks:
            schema = loader.get_schema(park_id)
            assert isinstance(schema, VegetationSchema)
            assert schema.vegetation_type_field is not None
            assert schema.geometry_column is not None

    def test_schema_consistency(self, temp_config_file):
        """Test that schemas loaded through different methods are consistent."""
        loader = VegetationSchemaLoader(temp_config_file)
        
        # Load schema directly
        schema1 = loader.get_schema("JOTR")
        
        # Load schema through get_schema_or_default
        schema2 = loader.get_schema_or_default("JOTR")
        
        # Should be the same object
        assert schema1 is schema2
        
        # Load schema after checking has_schema
        assert loader.has_schema("JOTR")
        schema3 = loader.get_schema("JOTR")
        
        # Should still be the same object
        assert schema1 is schema3