import json
import base64
import io
import time
from typing import BinaryIO, Callable, Dict, Any, List, Optional
from src.core.storage.interface import StorageInterface
import aiohttp
import obstore as obs
from obstore.store import MemoryStore


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
        self._store = MemoryStore()
        self._base_url: str = base_url
        self._metadata: Dict[str, Dict[str, Any]] = {}

    def get_obstore(self) -> MemoryStore:
        """Get the internal obstore MemoryStore"""
        return self._store

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
        await obs.put_async(self._store, path, data)

        # Store metadata separately
        self._metadata[path] = {
            "timestamp": time.time(),
            "temporary": temporary,
        }

        return self.get_url(path)

    async def get_bytes(self, path: str) -> bytes:
        """Get binary data from in-memory storage"""
        try:
            result = await obs.get_async(self._store, path)
            return bytes(await result.bytes_async())
        except Exception:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

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
        objects = self._store.list(prefix=prefix)
        return [obj["path"] for obj in await objects.collect_async()]

    def get_url(self, path: str) -> str:
        """Get URL for a stored object (virtual URL for in-memory storage)"""
        return f"{self._base_url}/{path}"

    async def get_file_obj(self, path: str) -> io.BytesIO:
        """
        Get a file-like object for the stored data

        Args:
            path: Storage path

        Returns:
            BytesIO object for the stored data
        """
        data = await self.get_bytes(path)
        return io.BytesIO(data)

    async def get_data_url(self, path: str) -> str:
        """
        Get data URL for a stored object (for browser usage)

        Returns:
            Data URL with base64-encoded content
        """
        data = await self.get_bytes(path)
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
        try:
            result = obs.get(self._store, path)
            return bytes(result.bytes())
        except Exception:
            raise FileNotFoundError(f"Path not found in memory storage: {path}")

    async def process_stream(
        self,
        source_path: str,
        processor: Callable[[BinaryIO], bytes],
        target_path: str,
        temporary: bool = False,
    ) -> str:
        """Process data in memory without temporary files"""
        # Get source as file-like object
        source_file_obj = await self.get_file_obj(source_path)

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
        paths_to_remove = []

        for path, metadata in list(self._metadata.items()):
            # Check if file is temporary
            is_temporary = metadata["temporary"]

            # Check if file is older than max_age_seconds
            is_old = False
            if max_age_seconds is not None:
                age = current_time - metadata["timestamp"]
                is_old = age > max_age_seconds

            # Remove if temporary or too old
            if is_temporary or is_old:
                paths_to_remove.append(path)

        # Remove from obstore and metadata
        for path in paths_to_remove:
            try:
                await obs.delete_async(self._store, path)
                del self._metadata[path]
                removed_count += 1
            except Exception:
                pass  # Path might not exist in store

        return removed_count
