"""
Vegetation schema loader for managing park unit configurations.

This module provides a VegetationSchemaLoader class that loads vegetation schema
configurations from a JSON file and provides thread-safe caching functionality.
It integrates with the existing VegetationSchema dataclass and follows the
patterns established by the StacProviderConfig loader.
"""

import json
import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional

from src.config.vegetation_schemas import VegetationSchema

logger = logging.getLogger(__name__)


class VegetationSchemaConfig:
    """Configuration model for vegetation schemas loaded from JSON."""

    def __init__(self, park_units: List[Dict]) -> None:
        """
        Initialize configuration with park unit data.

        Args:
            park_units: List of park unit configuration dictionaries
        """
        self.park_units = park_units

    @classmethod
    def load_from_file(
        cls, filepath: str = "config/vegetation_schemas.json"
    ) -> "VegetationSchemaConfig":
        """
        Load configuration from a JSON file.

        Args:
            filepath: Path to the vegetation schemas JSON file

        Returns:
            VegetationSchemaConfig instance with loaded data

        Raises:
            FileNotFoundError: If the configuration file is not found
            json.JSONDecodeError: If the JSON is malformed
            ValueError: If the configuration structure is invalid
        """
        file_path = Path(filepath)

        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file {filepath} not found.")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Validate the JSON structure
            if not isinstance(data, dict):
                raise ValueError("Configuration file must contain a JSON object")

            if "park_units" not in data:
                raise ValueError("Configuration file must contain 'park_units' array")

            park_units = data["park_units"]
            if not isinstance(park_units, list):
                raise ValueError("'park_units' must be an array")

            # Validate each park unit configuration
            for i, park_unit in enumerate(park_units):
                if not isinstance(park_unit, dict):
                    raise ValueError(f"Park unit at index {i} must be an object")

                required_fields = [
                    "id",
                    "name",
                    "vegetation_type_field",
                    "geometry_column",
                ]
                for field in required_fields:
                    if field not in park_unit:
                        raise ValueError(
                            f"Park unit at index {i} missing required field: {field}"
                        )

                # Validate field types
                if not isinstance(park_unit["id"], str) or not park_unit["id"].strip():
                    raise ValueError(f"Park unit at index {i} has invalid 'id' field")

                if (
                    not isinstance(park_unit["name"], str)
                    or not park_unit["name"].strip()
                ):
                    raise ValueError(f"Park unit at index {i} has invalid 'name' field")

            logger.info(
                f"Successfully loaded {len(park_units)} park unit configurations from {filepath}"
            )
            return cls(park_units=park_units)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in configuration file {filepath}: {str(e)}")
            raise json.JSONDecodeError(
                f"Invalid JSON in configuration file {filepath}: {str(e)}", e.doc, e.pos
            )
        except Exception as e:
            logger.error(f"Failed to load configuration from {filepath}: {str(e)}")
            raise

    def get_park_units(self) -> List[Dict]:
        """
        Get the park units configuration list.

        Returns:
            List of park unit configuration dictionaries
        """
        return self.park_units


class VegetationSchemaLoader:
    """
    Thread-safe loader for vegetation schema configurations.

    This class provides centralized access to vegetation schema configurations
    with in-memory caching for performance. It follows the singleton-like pattern
    used in the codebase for configuration loaders.
    """

    _instance: Optional["VegetationSchemaLoader"] = None
    _lock = threading.Lock()

    def __init__(self, config_path: str = "config/vegetation_schemas.json") -> None:
        """
        Initialize the vegetation schema loader.

        Args:
            config_path: Path to the vegetation schemas configuration file
        """
        self._config_path = config_path
        self._schemas_cache: Optional[Dict[str, VegetationSchema]] = None
        self._cache_lock = threading.Lock()

        logger.debug(
            f"VegetationSchemaLoader initialized with config path: {config_path}"
        )

    @classmethod
    def get_instance(
        cls, config_path: str = "config/vegetation_schemas.json"
    ) -> "VegetationSchemaLoader":
        """
        Get singleton instance of VegetationSchemaLoader.

        Args:
            config_path: Path to the vegetation schemas configuration file

        Returns:
            VegetationSchemaLoader singleton instance
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(config_path)
        return cls._instance

    def _load_schemas(self) -> Dict[str, VegetationSchema]:
        """
        Load schemas from configuration file and convert to VegetationSchema instances.

        Returns:
            Dictionary mapping park unit IDs to VegetationSchema instances

        Raises:
            FileNotFoundError: If configuration file is not found
            ValueError: If configuration is invalid
        """
        try:
            config = VegetationSchemaConfig.load_from_file(self._config_path)
            schemas = {}

            for park_unit in config.get_park_units():
                park_id = park_unit["id"]

                # Convert to VegetationSchema dataclass
                schema = VegetationSchema(
                    layer_name=park_unit.get("layer_name"),
                    vegetation_type_field=park_unit["vegetation_type_field"],
                    description_field=park_unit.get("description_field"),
                    color_field=park_unit.get("color_field"),
                    geometry_column=park_unit["geometry_column"],
                    preserve_fields=park_unit.get("preserve_fields"),
                )

                schemas[park_id] = schema
                logger.debug(f"Loaded schema for park unit: {park_id}")

            logger.info(f"Successfully loaded {len(schemas)} vegetation schemas")
            return schemas

        except Exception as e:
            logger.error(f"Failed to load vegetation schemas: {str(e)}")
            raise

    def _get_schemas_cache(self) -> Dict[str, VegetationSchema]:
        """
        Get cached schemas, loading them if necessary (thread-safe).

        Returns:
            Dictionary mapping park unit IDs to VegetationSchema instances
        """
        if self._schemas_cache is None:
            with self._cache_lock:
                if self._schemas_cache is None:
                    self._schemas_cache = self._load_schemas()
        return self._schemas_cache

    def get_schema(self, park_unit_id: str) -> VegetationSchema:
        """
        Get vegetation schema for a specific park unit.

        Args:
            park_unit_id: ID of the park unit (e.g., "JOTR", "MOJN")

        Returns:
            VegetationSchema configuration for the specified park unit

        Raises:
            ValueError: If park_unit_id is not found in configuration
        """
        if not park_unit_id or not isinstance(park_unit_id, str):
            raise ValueError(f"Invalid park unit ID: {park_unit_id}")

        schemas = self._get_schemas_cache()

        if park_unit_id not in schemas:
            available_parks = list(schemas.keys())
            logger.error(
                f"Park unit '{park_unit_id}' not found. Available parks: {available_parks}"
            )
            raise ValueError(
                f"No vegetation schema found for park unit '{park_unit_id}'. "
                f"Available park units: {', '.join(available_parks)}"
            )

        schema = schemas[park_unit_id]
        logger.debug(f"Retrieved schema for park unit: {park_unit_id}")
        return schema

    def list_available_parks(self) -> List[str]:
        """
        Get list of available park unit IDs.

        Returns:
            List of park unit IDs that have configured schemas
        """
        schemas = self._get_schemas_cache()
        park_ids = list(schemas.keys())
        logger.debug(f"Available park units: {park_ids}")
        return park_ids

    def has_schema(self, park_unit_id: str) -> bool:
        """
        Check if a schema exists for the given park unit ID.

        Args:
            park_unit_id: ID of the park unit to check

        Returns:
            True if schema exists, False otherwise
        """
        if not park_unit_id or not isinstance(park_unit_id, str):
            return False

        schemas = self._get_schemas_cache()
        return park_unit_id in schemas

    def reload_schemas(self) -> None:
        """
        Force reload of schemas from configuration file.

        This method clears the cache and reloads the configuration,
        useful for testing or if the configuration file has been updated.
        """
        with self._cache_lock:
            self._schemas_cache = None
            logger.info("Vegetation schemas cache cleared, will reload on next access")

    def get_default_schema(self) -> VegetationSchema:
        """
        Get the default vegetation schema.

        Returns:
            Default VegetationSchema configuration

        Raises:
            ValueError: If no default schema is configured
        """
        try:
            return self.get_schema("DEFAULT")
        except ValueError:
            logger.error("No default schema configured")
            raise ValueError("No default vegetation schema is configured")

    def get_schema_or_default(self, park_unit_id: Optional[str]) -> VegetationSchema:
        """
        Get schema for park unit or return default if not found.

        Args:
            park_unit_id: Optional park unit ID

        Returns:
            VegetationSchema for the park unit or default schema
        """
        if park_unit_id and self.has_schema(park_unit_id):
            return self.get_schema(park_unit_id)

        logger.info(
            f"No specific schema found for '{park_unit_id}', using default schema"
        )
        return self.get_default_schema()

    def validate_schema_against_data(
        self, schema: VegetationSchema, data_columns: set
    ) -> None:
        """
        Validate that a schema's required fields exist in the data and log warnings for missing preserve_fields.

        Args:
            schema: VegetationSchema to validate
            data_columns: Set of column names available in the data

        Raises:
            ValueError: If required fields are missing from the data
        """
        missing_required = []

        # Check required fields
        if schema.vegetation_type_field not in data_columns:
            missing_required.append(schema.vegetation_type_field)

        if schema.geometry_column not in data_columns:
            missing_required.append(schema.geometry_column)

        if schema.description_field and schema.description_field not in data_columns:
            missing_required.append(schema.description_field)

        if schema.color_field and schema.color_field not in data_columns:
            missing_required.append(schema.color_field)

        if missing_required:
            logger.error(f"Required fields missing from data: {missing_required}")
            raise ValueError(
                f"Required fields missing from data: {', '.join(missing_required)}"
            )

        # Check preserve_fields and log warnings for missing ones
        if schema.preserve_fields:
            missing_preserve = [
                field for field in schema.preserve_fields if field not in data_columns
            ]
            if missing_preserve:
                logger.warning(
                    f"Preserve fields not found in data (will be skipped): {missing_preserve}. "
                    f"Available columns: {sorted(data_columns)}"
                )
            else:
                logger.debug(
                    f"All preserve fields found in data: {schema.preserve_fields}"
                )

        logger.info(
            f"Schema validation completed successfully for vegetation_type_field='{schema.vegetation_type_field}'"
        )

    def get_validated_schema(
        self, park_unit_id: str, data_columns: set
    ) -> VegetationSchema:
        """
        Get schema for park unit and validate it against actual data columns.

        Args:
            park_unit_id: ID of the park unit
            data_columns: Set of column names available in the data

        Returns:
            Validated VegetationSchema configuration

        Raises:
            ValueError: If park_unit_id is not found or required fields are missing
        """
        schema = self.get_schema(park_unit_id)
        self.validate_schema_against_data(schema, data_columns)
        return schema
