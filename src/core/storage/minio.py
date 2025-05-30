import io
import json
import os
import tempfile
from typing import BinaryIO, Callable, Dict, Any, List, Optional
import aiohttp
from minio import Minio
from minio.error import S3Error
from pathlib import Path
import aiohttp

from src.core.storage.interface import StorageInterface


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
        self.bucket_name = bucket_name
        self.endpoint = endpoint

        # Get credentials from args or environment variables
        self.access_key = (
            access_key
            or os.environ.get("S3_ACCESS_KEY_ID")
            or os.environ.get("GCP_ACCESS_KEY_ID")
        )
        self.secret_key = (
            secret_key
            or os.environ.get("S3_SECRET_ACCESS_KEY")
            or os.environ.get("GCP_SECRET_ACCESS_KEY")
        )

        if not self.access_key or not self.secret_key:
            raise ValueError(
                "Access key and secret key must be provided either as arguments or through environment variables"
            )

        # Initialize Minio client
        self.client = Minio(
            endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=secure,
            region=region,
        )

        # Set base URL for public access
        self.base_url = base_url or f"https://{endpoint}/{bucket_name}"

        # Create bucket if it doesn't exist
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        """Ensure the bucket exists, create it if not"""
        if not self.client.bucket_exists(self.bucket_name):
            self.client.make_bucket(self.bucket_name)

    async def save_bytes(self, data: bytes, path: str) -> str:
        """Save binary data to S3 bucket"""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(data)
            tmp_path = tmp.name

        try:
            # Determine content type based on file extension
            content_type = self._guess_content_type(path)

            # Upload file
            self.client.fput_object(
                self.bucket_name, path, tmp_path, content_type=content_type
            )
            return self.get_url(path)
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    async def get_bytes(self, path: str) -> bytes:
        """Get binary data from S3 bucket"""
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Download object to temporary file
            self.client.fget_object(self.bucket_name, path, tmp_path)

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
        objects = self.client.list_objects(
            self.bucket_name, prefix=prefix, recursive=True
        )
        return [obj.object_name for obj in objects]

    def get_url(self, path: str) -> str:
        """Get public URL for an S3 object"""
        return f"{self.base_url}/{path}"

    async def download_to_temp(self, path: str) -> str:
        """Download file from S3 to temporary file"""
        # Create temporary file
        fd, temp_path = tempfile.mkstemp(suffix=Path(path).suffix)
        os.close(fd)

        # Download to temporary file
        self.client.fget_object(self.bucket_name, path, temp_path)
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
        """Process data with minimal temporary storage"""
        # For Minio, we need a temporary buffer in memory
        # but we avoid disk I/O
        source_data = await self.get_bytes(source_path)
        source_file_obj = io.BytesIO(source_data)

        # Process it
        result_bytes = processor(source_file_obj)

        # Save result directly
        return await self.save_bytes(result_bytes, target_path)

    async def copy_from_url(self, url: str, target_path: str) -> str:
        """Download from URL directly to blob storage"""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(f"Failed to download {url}: {response.status}")

                content = await response.read()
                return await self.save_bytes(content, target_path)

    async def cleanup(self, max_age_seconds: Optional[float] = None) -> int:
        """Cleanup old files in the bucket"""
        if max_age_seconds is None:
            raise ValueError("max_age_seconds must be provided for cleanup")

        # List all objects in the bucket
        objects = self.client.list_objects(self.bucket_name, recursive=True)
        removed_count = 0

        for obj in objects:
            # Check if the object is older than max_age_seconds
            if (obj.last_modified - obj.created).total_seconds() > max_age_seconds:
                try:
                    self.client.remove_object(self.bucket_name, obj.object_name)
                    removed_count += 1
                except S3Error as e:
                    print(f"Failed to delete {obj.object_name}: {e}")

        return removed_count

    def _guess_content_type(self, path: str) -> str:
        """Guess content type from file extension"""
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
