from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from src.core.storage.interface import StorageInterface
from src.core.storage.storage_factory import StorageFactory
from src.stac.stac_json_manager import STACJSONManager
from src.computation.registry.index_registry import IndexRegistry
from geojson_pydantic import Polygon, MultiPolygon, Feature


@dataclass
class CommandContext:
    """
    Encapsulates all data and dependencies needed for command execution.

    This context object is passed to commands and contains all the necessary
    information for execution without requiring commands to have knowledge
    of how to obtain these dependencies.
    """

    # Core execution parameters
    job_id: str
    fire_event_name: str

    # Dependencies (injected by command registry)
    storage: StorageInterface
    storage_factory: StorageFactory
    stac_manager: STACJSONManager
    index_registry: IndexRegistry

    # Geometry data (GeoJSON format only)
    # Optional to support workflows where geometry is loaded from external sources
    geometry: Optional[Polygon | MultiPolygon | Feature] = None

    # Optional execution parameters
    prefire_date_range: Optional[List[str]] = None
    postfire_date_range: Optional[List[str]] = None
    severity_breaks: Optional[List[float]] = None

    # Additional configuration
    computation_config: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        """Validate context after initialization"""
        if not self.job_id:
            raise ValueError("job_id is required")
        if not self.fire_event_name:
            raise ValueError("fire_event_name is required")
        # Note: geometry is optional - commands that require it validate in validate_context()
        if not self.storage:
            raise ValueError("storage interface is required")
        if not self.storage_factory:
            raise ValueError("storage_factory is required")
        if not self.stac_manager:
            raise ValueError("stac_manager is required")
        if not self.index_registry:
            raise ValueError("index_registry is required")

    def get_computation_config(self, key: str, default: Any = None) -> Any:
        """Get computation configuration value with fallback"""
        if self.computation_config is None:
            return default
        return self.computation_config.get(key, default)

    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata value with fallback"""
        if self.metadata is None:
            return default
        return self.metadata.get(key, default)

    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata key-value pair"""
        if self.metadata is None:
            self.metadata = {}
        self.metadata[key] = value
