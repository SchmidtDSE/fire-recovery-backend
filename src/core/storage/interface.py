from abc import ABC, abstractmethod
import io
import os
from typing import Callable, Dict, Any, BinaryIO, Optional, List


class StorageInterface(ABC):
    """Abstract interface for all storage operations"""

    @abstractmethod
    async def save_bytes(self, data: bytes, path: str, temporary: bool = False) -> str:
        """
        Save binary data to storage and return access URL

        Args:
            data: Binary data to save
            path: Storage path (e.g., "fire_event/job_id/filename.tif")
            temporary: If True, data is stored temporarily and may be cleaned up later

        Returns:
            URL to access the saved data
        """
        pass

    @abstractmethod
    async def get_bytes(self, path: str) -> bytes:
        """
        Get binary data from storage

        Args:
            path: Storage path to retrieve

        Returns:
            Binary data
        """
        pass

    @abstractmethod
    async def save_json(
        self, data: Dict[str, Any], path: str, temporary: bool = False
    ) -> str:
        """
        Save JSON data to storage and return access URL

        Args:
            data: JSON data to save
            path: Storage path (e.g., "fire_event/job_id/data.json")
            temporary: If True, data is stored temporarily and may be cleaned up later

        Returns:
            URL to access the saved JSON
        """
        pass

    @abstractmethod
    async def get_json(self, path: str) -> Dict[str, Any]:
        """
        Get JSON data from storage

        Args:
            path: Storage path to retrieve

        Returns:
            JSON data as dictionary
        """
        pass

    @abstractmethod
    async def list_files(self, prefix: str) -> List[str]:
        """
        List files in storage with given prefix

        Args:
            prefix: Path prefix to list

        Returns:
            List of file paths
        """
        pass

    @abstractmethod
    def get_url(self, path: str) -> str:
        """
        Get public URL for a stored object

        Args:
            path: Storage path

        Returns:
            Public URL for accessing the object
        """
        pass

    @abstractmethod
    async def process_stream(
        self,
        source_path: str,
        processor: Callable[[BinaryIO], bytes],
        target_path: str,
        temporary: bool = False,
    ) -> str:
        """
        Process a stream directly without creating temporary files

        Args:
            source_path: Source path to read
            processor: Function that processes a file-like object and returns bytes
            target_path: Target path to write processed result
            temporary: If True, processed data is stored temporarily and may be cleaned up later

        Returns:
            URL to access the processed data
        """
        pass

    @abstractmethod
    async def copy_from_url(
        self, url: str, target_path: str, temporary: bool = False
    ) -> str:
        """
        Download from URL directly to storage without temp files

        Args:
            url: URL to download
            target_path: Target path in storage
            temporary: If True, downloaded file is stored temporarily and may be cleaned up later

        Returns:
            Storage URL for the downloaded file
        """
        pass

    @abstractmethod
    async def cleanup(self, max_age_seconds: Optional[float] = None) -> int:
        """
        Cleanup old files in storage

        Args:
            max_age_seconds: Maximum age of files to keep, if None, remove all files

        Returns:
            Number of files deleted
        """
        pass

    def create_output_buffer(self, filename: str = "output_file") -> io.BytesIO:
        """
        Create an empty file-like BytesIO buffer for writing
        
        Args:
            filename: Optional filename to set on the buffer
        
        Returns:
            Empty BytesIO buffer that can be written to
        """
        buffer = io.BytesIO()
        buffer.name = filename
        return buffer

    def _guess_content_type(self, path: str) -> str:
        """
        Guess content type from file extension

        This is a common utility method shared by all storage implementations.

        Args:
            path: File path with extension

        Returns:
            MIME type string
        """
        ext = os.path.splitext(path)[1].lower()
        mime_types = {
            ".json": "application/json",
            ".geojson": "application/geo+json",
            ".tif": "image/tiff",
            ".tiff": "image/tiff",
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".csv": "text/csv",
            ".parquet": "application/octet-stream",
        }
        return mime_types.get(ext, "application/octet-stream")

    def create_file_like_buffer(self, data: bytes, filename: str = "temp_file") -> io.BytesIO:
        """
        Create a file-like BytesIO buffer from bytes data
        
        This utility method creates a BytesIO buffer that behaves like a file object,
        which is useful for libraries that expect file-like objects but you have bytes data.
        
        Args:
            data: Bytes data to wrap
            filename: Optional filename to set on the buffer (some libraries use this)
        
        Returns:
            BytesIO buffer that can be used as a file-like object
        """
        buffer = io.BytesIO(data)
        buffer.name = filename  # Some libraries (like rustac) need a name attribute
        return buffer
