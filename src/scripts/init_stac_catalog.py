#!/usr/bin/env python3
"""
Initialize STAC catalog for testing environments.

This script is used in CI/CD workflows to ensure a STAC catalog exists
before running tests. It uses the application's own STACCatalogManager
to create the catalog structure programmatically.
"""

import asyncio
import os
import sys

from src.core.storage.minio import MinioCloudStorage
from src.stac.stac_catalog_manager import STACCatalogManager


async def main() -> int:
    """Initialize STAC catalog in S3-compatible storage.

    Reads configuration from environment variables:
    - S3_ENDPOINT: Storage endpoint (e.g., localhost:9000)
    - S3_BUCKET: Bucket name
    - S3_ACCESS_KEY_ID: Access key
    - S3_SECRET_ACCESS_KEY: Secret key
    - S3_SECURE: Whether to use HTTPS (default: False)

    Returns:
        0 on success, 1 on failure
    """
    try:
        # Read configuration from environment
        endpoint = os.environ["S3_ENDPOINT"]
        bucket = os.environ["S3_BUCKET"]
        access_key = os.environ["S3_ACCESS_KEY_ID"]
        secret_key = os.environ["S3_SECRET_ACCESS_KEY"]
        secure = os.environ.get("S3_SECURE", "False").lower() == "true"

        protocol = "https" if secure else "http"
        base_url = f"{protocol}://{endpoint}/{bucket}/stac"

        print(f"Initializing STAC catalog at: {base_url}")

        # Create storage instance
        storage = MinioCloudStorage(
            endpoint=endpoint,
            bucket_name=bucket,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        # Create catalog manager
        catalog_manager = STACCatalogManager.for_production(
            base_url=base_url, storage=storage
        )

        # Check if catalog exists
        existing = await catalog_manager.get_catalog()
        if existing:
            print("✓ STAC catalog already exists")
        else:
            print("Initializing new STAC catalog...")
            await catalog_manager.initialize_catalog()
            print("✓ STAC catalog initialized successfully")

        return 0

    except KeyError as e:
        print(f"✗ Error: Missing required environment variable: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"✗ Error initializing STAC catalog: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
