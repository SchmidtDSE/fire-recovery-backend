from datetime import datetime, timezone
import io
import json
import os
import tempfile
from typing import BinaryIO, Callable, Dict, Any, List, Optional
import aiohttp
from minio import Minio
from minio.error import S3Error
from pathlib import Path

from src.core.storage.interface import StorageInterface
from src.config.constants import DEFAULT_TEMP_FILE_MAX_AGE_SECONDS


class MinioCloudStorage(StorageInterface):
    """Storage implementation for any S3-compatible storage using Minio client"""

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

        # Initialize Minio client
        self._client = Minio(
            endpoint,
            access_key=self._access_key,
            secret_key=self._secret_key,
            secure=secure,
            region=region,
        )

        # Set base URL for public access
        self._base_url = base_url or f"{endpoint}/{bucket_name}"

        # Create bucket if it doesn't exist
        self._ensure_bucket_exists()

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
    def client(self) -> Minio:
        """Get the Minio client"""
        return self._client

    @property
    def base_url(self) -> str:
        """Get the base URL"""
        return self._base_url

    def _ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists, create it if not"""
        if not self._client.bucket_exists(self._bucket_name):
            self._client.make_bucket(self._bucket_name)

    async def save_bytes(self, data: bytes, path: str, temporary: bool = False) -> str:
        """Save binary data to S3 bucket without using temp files"""
        # If temporary, directly store in temp/ directory
        if temporary and not path.startswith("temp/"):
            path = f"temp/{path}"

        # Determine content type based on file extension
        content_type = self._guess_content_type(path)

        # Use BytesIO to create a file-like object in memory
        data_stream = io.BytesIO(data)

        # Upload directly from memory without temp files
        self._client.put_object(
            self._bucket_name, path, data_stream, len(data), content_type=content_type
        )

        return self.get_url(path)

    async def get_bytes(self, path: str) -> bytes:
        """Get binary data from S3 bucket"""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Download object to temporary file
            self._client.fget_object(self._bucket_name, path, tmp_path)

            # Read file content
            with open(tmp_path, "rb") as f:
                return f.read()
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    async def save_json(self, data: Dict[str, Any], path: str) -> str:
        """Save JSON data to S3 bucket"""
        json_str = json.dumps(data)
        return await self.save_bytes(json_str.encode("utf-8"), path)

    async def get_json(self, path: str) -> Dict[str, Any]:
        """Get JSON data from S3 bucket"""
        data = await self.get_bytes(path)
        return json.loads(data.decode("utf-8"))

    async def list_files(self, prefix: str) -> List[str]:
        """List files in S3 bucket with given prefix"""
        objects = self._client.list_objects(
            self._bucket_name, prefix=prefix, recursive=True
        )
        return [obj.object_name for obj in objects]

    def get_url(self, path: str) -> str:
        """Get public URL for an S3 object"""
        return f"{self._base_url}/{path}"

    async def download_to_temp(self, path: str) -> str:
        """Download file from S3 to temporary file"""
        # Create temporary file
        fd, temp_path = tempfile.mkstemp(suffix=Path(path).suffix)
        os.close(fd)

        # Download to temporary file
        self._client.fget_object(self._bucket_name, path, temp_path)
        return temp_path

    async def download_url_to_temp(self, url: str) -> str:
        """
        Download a file from URL to temporary file

        Args:
            url: URL to download

        Returns:
            Path to local temporary file
        """
        # Create temporary file
        fd, temp_path = tempfile.mkstemp()
        os.close(fd)

        # Download file from URL
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download {url}: {response.status}")

                with open(temp_path, "wb") as f:
                    f.write(await response.read())

        return temp_path

    async def process_stream(
        self, source_path: str, processor: Callable[[BinaryIO], bytes], target_path: str
    ) -> str:
        """Process data with minimal temporary storage

        Args:
            source_path: Path in the bucket to read from
            processor: Function that takes a BinaryIO and returns processed bytes
            target_path: Path in the bucket to save the processed data

        Returns:
            Path in the bucket where the processed data was saved
        """
        # For Minio, we need a temporary buffer in memory
        # but we avoid disk I/O
        source_data = await self.get_bytes(source_path)
        source_file_obj = io.BytesIO(source_data)

        # Process it
        result_bytes = processor(source_file_obj)

        # Save result directly
        return await self.save_bytes(result_bytes, target_path)

    async def copy_from_url(self, url: str, target_path: str) -> str:
        """Download from URL directly to blob storage.

        Args:
            url: URL to download
            target_path: Path in the bucket to save the downloaded file

        Returns:
            Path in the bucket where the file was saved
        """
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download {url}: {response.status}")

                content = await response.read()
                return await self.save_bytes(content, target_path)

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

        # List all objects in the bucket
        objects = self._client.list_objects(self._bucket_name, recursive=True)
        removed_count = 0

        for obj in objects:
            # Check if the object is older than max_age_seconds
            age_seconds = (current_time - obj.last_modified).total_seconds()
            if age_seconds > max_age_seconds:
                try:
                    self._client.remove_object(self._bucket_name, obj.object_name)
                    removed_count += 1
                except S3Error as e:
                    print(f"Failed to delete {obj.object_name}: {e}")

        return removed_count
