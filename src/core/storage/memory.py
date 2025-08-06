import json
import base64
import tempfile
import os
import io
import time
from typing import BinaryIO, Callable, Dict, Any, List, Optional
from src.core.storage.interface import StorageInterface
import aiohttp


class MemoryStorage(StorageInterface):
    """
    In-memory implementation of StorageInterface for in-browser / Pyodide environments
    (or environments where file system access is not available)
    """

    def __init__(self, base_url: str = "memory://"):
        """
        Initialize in-memory storage

        Args:
            base_url: Base URL prefix for virtual URLs
        """
        self._storage: Dict[str, Dict[str, Any]] = {}
        self._base_url: str = base_url

    @property
    def storage(self) -> Dict[str, Dict[str, Any]]:
        """Get the internal storage dictionary"""
        return self._storage

    @property
    def base_url(self) -> str:
        """Get the base URL"""
        return self._base_url

    async def save_bytes(self, data: bytes, path: str, temporary: bool = False) -> str:
        """
        Save binary data to in-memory storage

        Args:
            data: Binary data to store
            path: Storage path
            temporary: If True, file will be deleted during cleanup
        """
        buffer = io.BytesIO(data)
        buffer.seek(0)
        file_data = buffer

        # Store with metadata
        self._storage[path] = {
            "data": file_data,
            "timestamp": time.time(),
            "temporary": temporary,
        }

        return self.get_url(path)

    async def get_bytes(self, path: str) -> bytes:
        """Get binary data from in-memory storage"""
        if path not in self._storage:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

        stored_item = self._storage[path]
        stored_data = stored_item["data"]

        stored_data.seek(0)
        return stored_data.getvalue()

    async def save_json(
        self, data: Dict[str, Any], path: str, temporary: bool = False
    ) -> str:
        """Save JSON data to in-memory storage"""
        json_str = json.dumps(data)
        return await self.save_bytes(json_str.encode("utf-8"), path, temporary)

    async def get_json(self, path: str) -> Dict[str, Any]:
        """Get JSON data from in-memory storage"""
        data = await self.get_bytes(path)
        return json.loads(data.decode("utf-8"))

    async def list_files(self, prefix: str) -> List[str]:
        """List files in in-memory storage with given prefix"""
        return [key for key in self._storage.keys() if key.startswith(prefix)]

    def get_url(self, path: str) -> str:
        """Get URL for a stored object (virtual URL for in-memory storage)"""
        return f"{self._base_url}/{path}"

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
        if path not in self._storage:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

        stored_data = self._storage[path]["data"]

        # Return existing BytesIO buffer (reset position first)
        stored_data.seek(0)
        return stored_data

    def get_data_url(self, path: str) -> str:
        """
        Get data URL for a stored object (for browser usage)

        Returns:
            Data URL with base64-encoded content
        """
        if path not in self._storage:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

        data = self.get_bytes_sync(path)
        mime_type = self._guess_content_type(path)
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
        if path not in self._storage:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

        stored_data = self._storage[path]["data"]

        stored_data.seek(0)
        return stored_data.getvalue()

    async def process_stream(
        self,
        source_path: str,
        processor: Callable[[BinaryIO], bytes],
        target_path: str,
        temporary: bool = False,
    ) -> str:
        """Process data in memory without temporary files"""
        # Get source as file-like object
        source_file_obj = self.get_file_obj(source_path)

        # Process it
        result_bytes = processor(source_file_obj)

        # Save result
        return await self.save_bytes(result_bytes, target_path, temporary)

    async def copy_from_url(
        self, url: str, target_path: str, temporary: bool = False
    ) -> str:
        """Download from URL directly to memory storage"""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download {url}: {response.status}")

                content = await response.read()
                return await self.save_bytes(content, target_path, temporary)

    async def cleanup(self, max_age_seconds: Optional[float] = None) -> int:
        """
        Remove temporary files and optionally files older than max_age_seconds

        Args:
            max_age_seconds: If provided, removes files older than this many seconds

        Returns:
            Number of files removed
        """
        current_time = time.time()
        removed_count = 0

        for path in list(self._storage.keys()):
            item = self._storage[path]

            # Check if file is temporary
            is_temporary = item["temporary"]

            # Check if file is older than max_age_seconds
            is_old = False
            if max_age_seconds is not None:
                age = current_time - item["timestamp"]
                is_old = age > max_age_seconds

            # Remove if temporary or too old
            if is_temporary or is_old:
                del self._storage[path]
                removed_count += 1

        return removed_count
