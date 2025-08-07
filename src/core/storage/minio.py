from datetime import datetime, timezone
import io
import json
import os
from typing import BinaryIO, Callable, Dict, Any, List, Optional
import aiohttp
import obstore as obs
from obstore.store import S3Store

from src.core.storage.interface import StorageInterface
from src.config.constants import DEFAULT_TEMP_FILE_MAX_AGE_SECONDS


class MinioCloudStorage(StorageInterface):
    """Storage implementation for any S3-compatible storage using obstore S3Store"""

    def __init__(
        self,
        bucket_name: str,
        endpoint: str = "storage.googleapis.com",
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = "auto",
        secure: bool = True,
        base_url: Optional[str] = None,
    ):
        """
        Initialize S3-compatible storage client

        Args:
            bucket_name: Name of the S3 bucket
            endpoint: S3-compatible service endpoint (default: storage.googleapis.com for GCS)
            access_key: Access key (will use environment variables if not provided)
            secret_key: Secret key (will use environment variables if not provided)
            region: S3 region (default: "auto")
            secure: Use HTTPS if True (default: True)
            base_url: Base URL for public access (default: https://{endpoint}/{bucket_name})
        """
        self._bucket_name = bucket_name
        self._endpoint = endpoint

        # Get credentials from args or environment variables
        self._access_key = (
            access_key
            or os.environ.get("S3_ACCESS_KEY_ID")
            or os.environ.get("GCP_ACCESS_KEY_ID")
        )
        self._secret_key = (
            secret_key
            or os.environ.get("S3_SECRET_ACCESS_KEY")
            or os.environ.get("GCP_SECRET_ACCESS_KEY")
        )

        if not self._access_key or not self._secret_key:
            raise ValueError(
                "Access key and secret key must be provided either as arguments or through environment variables"
            )

        # Initialize obstore S3Store
        client_options = {}
        if not secure:
            client_options["allow_http"] = True

        self._store = S3Store(
            bucket_name,
            endpoint=f"{'https' if secure else 'http'}://{endpoint}",
            access_key_id=self._access_key,
            secret_access_key=self._secret_key,
            region=region,
            virtual_hosted_style_request=False,
            client_options=client_options,
        )

        # Set base URL for public access
        self._base_url = base_url or f"{endpoint}/{bucket_name}"

    @property
    def bucket_name(self) -> str:
        """Get the bucket name"""
        return self._bucket_name

    @property
    def endpoint(self) -> str:
        """Get the endpoint"""
        return self._endpoint

    @property
    def access_key(self) -> str | None:
        """Get the access key"""
        return self._access_key

    @property
    def secret_key(self) -> str | None:
        """Get the secret key"""
        return self._secret_key

    @property
    def store(self) -> S3Store:
        """Get the obstore S3Store"""
        return self._store

    @property
    def base_url(self) -> str:
        """Get the base URL"""
        return self._base_url

    async def save_bytes(self, data: bytes, path: str, temporary: bool = False) -> str:
        """Save binary data to S3 bucket without using temp files"""
        # If temporary, directly store in temp/ directory
        if temporary and not path.startswith("temp/"):
            path = f"temp/{path}"

        await obs.put_async(self._store, path, data)
        return self.get_url(path)

    async def get_bytes(self, path: str) -> bytes:
        """Get binary data from S3 bucket using direct streaming"""
        try:
            result = await obs.get_async(self._store, path)
            return bytes(await result.bytes_async())
        except Exception as e:
            raise Exception(f"Failed to get object {path}: {e}")

    async def save_json(
        self, data: Dict[str, Any], path: str, temporary: bool = False
    ) -> str:
        """Save JSON data to S3 bucket"""
        json_str = json.dumps(data)
        return await self.save_bytes(json_str.encode("utf-8"), path, temporary)

    async def get_json(self, path: str) -> Dict[str, Any]:
        """Get JSON data from S3 bucket"""
        data = await self.get_bytes(path)
        return json.loads(data.decode("utf-8"))

    async def list_files(self, prefix: str) -> List[str]:
        """List files in S3 bucket with given prefix"""
        objects = self._store.list(prefix=prefix)
        return [obj["path"] for obj in objects.collect()]

    def get_url(self, path: str) -> str:
        """Get public URL for an S3 object"""
        return f"{self._base_url}/{path}"

    async def process_stream(
        self,
        source_path: str,
        processor: Callable[[BinaryIO], bytes],
        target_path: str,
        temporary: bool = False,
    ) -> str:
        """Process data with minimal temporary storage

        Args:
            source_path: Path in the bucket to read from
            processor: Function that takes a BinaryIO and returns processed bytes
            target_path: Path in the bucket to save the processed data
            temporary: If True, processed data is stored temporarily and may be cleaned up later

        Returns:
            Path in the bucket where the processed data was saved
        """
        # Get source data and create file-like object
        source_data = await self.get_bytes(source_path)
        source_file_obj = io.BytesIO(source_data)

        # Process it
        result_bytes = processor(source_file_obj)

        # Save result directly
        return await self.save_bytes(result_bytes, target_path, temporary)

    async def copy_from_url(
        self, url: str, target_path: str, temporary: bool = False
    ) -> str:
        """Download from URL directly to blob storage.

        Args:
            url: URL to download
            target_path: Path in the bucket to save the downloaded file
            temporary: If True, downloaded file is stored temporarily and may be cleaned up later

        Returns:
            Path in the bucket where the file was saved
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download {url}: {response.status}")

                content = await response.read()
                return await self.save_bytes(content, target_path, temporary)

    async def cleanup(self, max_age_seconds: Optional[float] = None) -> int:
        """
        Cleanup old files in the bucket

        Args:
            max_age_seconds: Maximum age of files to keep. If None, uses default based on bucket type.

        Returns:
            Number of files removed
        """
        # Use default if not provided
        if max_age_seconds is None:
            max_age_seconds = DEFAULT_TEMP_FILE_MAX_AGE_SECONDS

        current_time = datetime.now(timezone.utc)
        removed_count = 0
        paths_to_remove = []

        # List all objects in the bucket
        objects = self._store.list()
        for obj in objects.collect():
            # Check if the object is temporary (in temp/ directory)
            is_temporary = obj["path"].startswith("temp/")

            # Check if the object is older than max_age_seconds
            age_seconds = (current_time - obj["last_modified"]).total_seconds()
            is_old = age_seconds > max_age_seconds

            # Remove if temporary or too old
            if is_temporary or is_old:
                paths_to_remove.append(obj["path"])

        # Remove objects
        for path in paths_to_remove:
            try:
                await obs.delete_async(self._store, path)
                removed_count += 1
            except Exception as e:
                print(f"Failed to delete {path}: {e}")

        return removed_count
