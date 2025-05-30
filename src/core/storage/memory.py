import json
import base64
import tempfile
import os
import io
from typing import BinaryIO, Callable, Dict, Any, List, Union, Optional
from src.core.storage.interface import StorageInterface
import aiohttp


class MemoryStorage(StorageInterface):
    """
    In-memory implementation of StorageInterface for in-browser / Pyodide environments
    (or envionments where file system access is not available)
    """

    def __init__(self, base_url: str = "memory://", use_buffers: bool = True):
        """
        Initialize in-memory storage

        Args:
            base_url: Base URL prefix for virtual URLs
            use_buffers: Use BytesIO buffers instead of raw bytes (more efficient for streaming)
        """
        self.storage = {}  # In-memory storage dictionary
        self.base_url = base_url
        self.use_buffers = use_buffers

    async def save_bytes(self, data: bytes, path: str) -> str:
        """Save binary data to in-memory storage"""
        if self.use_buffers:
            # Store as BytesIO buffer for more efficient streaming operations
            buffer = io.BytesIO(data)
            buffer.seek(0)  # Reset position to beginning
            self.storage[path] = buffer
        else:
            # Store raw bytes
            self.storage[path] = data

        return self.get_url(path)

    async def get_bytes(self, path: str) -> bytes:
        """Get binary data from in-memory storage"""
        if path not in self.storage:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

        stored_data = self.storage[path]
        if isinstance(stored_data, io.BytesIO):
            # Get data from BytesIO buffer
            stored_data.seek(0)  # Reset position to beginning
            return stored_data.getvalue()
        else:
            # Return raw bytes
            return stored_data

    async def save_json(self, data: Dict[str, Any], path: str) -> str:
        """Save JSON data to in-memory storage"""
        json_str = json.dumps(data)
        return await self.save_bytes(json_str.encode("utf-8"), path)

    async def get_json(self, path: str) -> Dict[str, Any]:
        """Get JSON data from in-memory storage"""
        data = await self.get_bytes(path)
        return json.loads(data.decode("utf-8"))

    async def list_files(self, prefix: str) -> List[str]:
        """List files in in-memory storage with given prefix"""
        return [key for key in self.storage.keys() if key.startswith(prefix)]

    def get_url(self, path: str) -> str:
        """Get URL for a stored object (virtual URL for in-memory storage)"""
        return f"{self.base_url}/{path}"

    async def download_to_temp(self, path: str) -> str:
        """Create temporary file with content from in-memory storage"""
        data = await self.get_bytes(path)

        # Create temporary file
        fd, temp_path = tempfile.mkstemp(suffix=os.path.splitext(path)[1])
        os.close(fd)

        # Write data to temporary file
        with open(temp_path, "wb") as f:
            f.write(data)

        return temp_path

    def get_file_obj(self, path: str) -> io.BytesIO:
        """
        Get a file-like object for the stored data

        Args:
            path: Storage path

        Returns:
            BytesIO object for the stored data
        """
        if path not in self.storage:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

        stored_data = self.storage[path]
        if isinstance(stored_data, io.BytesIO):
            # Return existing BytesIO buffer (reset position first)
            stored_data.seek(0)
            return stored_data
        else:
            # Create new BytesIO from raw bytes
            buffer = io.BytesIO(stored_data)
            buffer.seek(0)
            return buffer

    def get_data_url(self, path: str) -> str:
        """
        Get data URL for a stored object (for browser usage)

        Returns:
            Data URL with base64-encoded content
        """
        if path not in self.storage:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

        data = self.get_bytes_sync(path)
        mime_type = self._guess_mime_type(path)
        b64_data = base64.b64encode(data).decode("ascii")
        return f"data:{mime_type};base64,{b64_data}"

    def get_bytes_sync(self, path: str) -> bytes:
        """
        Synchronous version of get_bytes for non-async contexts

        Args:
            path: Storage path

        Returns:
            Bytes data
        """
        if path not in self.storage:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

        stored_data = self.storage[path]
        if isinstance(stored_data, io.BytesIO):
            stored_data.seek(0)
            return stored_data.getvalue()
        else:
            return stored_data

    async def process_stream(
        self, source_path: str, processor: Callable[[BinaryIO], bytes], target_path: str
    ) -> str:
        """Process data in memory without temporary files"""
        # Get source as file-like object
        source_file_obj = self.get_file_obj(source_path)

        # Process it
        result_bytes = processor(source_file_obj)

        # Save result
        return await self.save_bytes(result_bytes, target_path)

    async def copy_from_url(self, url: str, target_path: str) -> str:
        """Download from URL directly to memory storage"""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download {url}: {response.status}")

                content = await response.read()
                return await self.save_bytes(content, target_path)

    def _guess_mime_type(self, path: str) -> str:
        """Guess MIME type from file extension"""
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
