from typing import Any, Dict, Optional
import logging
from src.core.storage.interface import StorageInterface
from src.core.storage.minio import MinioCloudStorage
from src.core.storage.memory import MemoryStorage


logger = logging.getLogger(__name__)


class StorageFactory:
    """
    Factory class for creating storage interface instances.
    
    This factory provides dependency injection for storage interfaces,
    enabling easy testing and configuration management. It supports
    creating both temporary and permanent storage instances based on
    configuration.
    """
    
    def __init__(
        self,
        temp_storage_type: str = "memory",
        temp_storage_config: Optional[Dict[str, Any]] = None,
        final_storage_type: str = "minio",
        final_storage_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize storage factory with configuration.
        
        Args:
            temp_storage_type: Type of temporary storage ('memory', 'minio')
            temp_storage_config: Configuration for temporary storage
            final_storage_type: Type of permanent storage ('minio')
            final_storage_config: Configuration for permanent storage
        """
        logger.info("Initializing StorageFactory")
        
        self._temp_storage_type = temp_storage_type
        self._temp_storage_config = temp_storage_config or {}
        self._final_storage_type = final_storage_type
        self._final_storage_config = final_storage_config or {}
        
        # Cache storage instances for reuse
        self._temp_storage_instance: Optional[StorageInterface] = None
        self._final_storage_instance: Optional[StorageInterface] = None
        
        logger.info(
            f"StorageFactory configured: temp={temp_storage_type}, final={final_storage_type}"
        )
    
    def get_temp_storage(self) -> StorageInterface:
        """
        Get temporary storage instance.
        
        Temporary storage is used for intermediate processing files
        that can be cleaned up after processing completes.
        
        Returns:
            StorageInterface instance for temporary storage
        """
        if self._temp_storage_instance is None:
            logger.debug(f"Creating temporary storage instance: {self._temp_storage_type}")
            self._temp_storage_instance = self._create_storage(
                self._temp_storage_type, self._temp_storage_config
            )
        
        return self._temp_storage_instance
    
    def get_final_storage(self) -> StorageInterface:
        """
        Get permanent storage instance.
        
        Final storage is used for long-term storage of results
        and assets that need to be preserved.
        
        Returns:
            StorageInterface instance for permanent storage
        """
        if self._final_storage_instance is None:
            logger.debug(f"Creating final storage instance: {self._final_storage_type}")
            self._final_storage_instance = self._create_storage(
                self._final_storage_type, self._final_storage_config
            )
        
        return self._final_storage_instance
    
    def create_custom_storage(
        self, storage_type: str, config: Dict[str, Any]
    ) -> StorageInterface:
        """
        Create a custom storage instance with specific configuration.
        
        This method creates a new storage instance each time it's called,
        useful for creating storage with specific settings.
        
        Args:
            storage_type: Type of storage to create
            config: Configuration dictionary for the storage
            
        Returns:
            New StorageInterface instance
        """
        logger.debug(f"Creating custom storage instance: {storage_type}")
        return self._create_storage(storage_type, config)
    
    def _create_storage(self, storage_type: str, config: Dict[str, Any]) -> StorageInterface:
        """
        Create storage instance based on type and configuration.
        
        Args:
            storage_type: Type of storage ('memory', 'minio')
            config: Configuration dictionary
            
        Returns:
            StorageInterface instance
            
        Raises:
            ValueError: If storage_type is not supported
        """
        storage_type_lower = storage_type.lower()
        
        if storage_type_lower == "memory":
            base_url = config.get("base_url", "memory://")
            return MemoryStorage(base_url=base_url)
        
        elif storage_type_lower == "minio":
            bucket_name = config.get("bucket_name")
            if not bucket_name:
                raise ValueError("bucket_name is required for Minio storage")
            
            return MinioCloudStorage(
                bucket_name=bucket_name,
                endpoint=config.get("endpoint", "storage.googleapis.com"),
                access_key=config.get("access_key"),
                secret_key=config.get("secret_key"),
                region=config.get("region", "auto"),
                secure=config.get("secure", True),
                base_url=config.get("base_url"),
            )
        
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")
    
    @classmethod
    def for_development(cls) -> "StorageFactory":
        """
        Create factory configured for development environment.
        
        Uses memory storage for both temp and final storage to avoid
        external dependencies during development and testing.
        
        Returns:
            StorageFactory configured for development
        """
        return cls(
            temp_storage_type="memory",
            temp_storage_config={"base_url": "memory://temp/"},
            final_storage_type="memory",
            final_storage_config={"base_url": "memory://final/"},
        )
    
    @classmethod
    def for_production(
        cls,
        temp_bucket_name: str,
        final_bucket_name: str,
        endpoint: str = "storage.googleapis.com",
        **kwargs: Any,
    ) -> "StorageFactory":
        """
        Create factory configured for production environment.
        
        Uses Minio/GCS storage for both temp and final storage with
        separate buckets for isolation and cleanup management.
        
        Args:
            temp_bucket_name: Bucket name for temporary storage
            final_bucket_name: Bucket name for permanent storage
            endpoint: Storage endpoint (default: GCS)
            **kwargs: Additional configuration passed to storage
            
        Returns:
            StorageFactory configured for production
        """
        temp_config = {
            "bucket_name": temp_bucket_name,
            "endpoint": endpoint,
            **kwargs,
        }
        
        final_config = {
            "bucket_name": final_bucket_name,
            "endpoint": endpoint,
            **kwargs,
        }
        
        return cls(
            temp_storage_type="minio",
            temp_storage_config=temp_config,
            final_storage_type="minio",
            final_storage_config=final_config,
        )
    
    @classmethod
    def for_testing(cls) -> "StorageFactory":
        """
        Create factory configured for testing.
        
        Uses memory storage with predictable base URLs for testing.
        
        Returns:
            StorageFactory configured for testing
        """
        return cls(
            temp_storage_type="memory",
            temp_storage_config={"base_url": "test://temp/"},
            final_storage_type="memory",
            final_storage_config={"base_url": "test://final/"},
        )
    
    def get_storage_info(self) -> Dict[str, Any]:
        """
        Get information about the configured storage types.
        
        Returns:
            Dictionary containing storage configuration info
        """
        return {
            "temp_storage_type": self._temp_storage_type,
            "final_storage_type": self._final_storage_type,
            "temp_storage_config": {
                k: v for k, v in self._temp_storage_config.items()
                if k not in ["access_key", "secret_key"]  # Don't expose secrets
            },
            "final_storage_config": {
                k: v for k, v in self._final_storage_config.items()
                if k not in ["access_key", "secret_key"]  # Don't expose secrets
            },
        }
    
    def reset_instances(self) -> None:
        """
        Reset cached storage instances.
        
        Useful for testing or when configuration changes.
        """
        logger.debug("Resetting storage instances")
        self._temp_storage_instance = None
        self._final_storage_instance = None