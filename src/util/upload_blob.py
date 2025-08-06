#!/usr/bin/env python3
import argparse
import os
import sys
from pathlib import Path

from src.config.storage import get_temp_storage, get_final_storage


async def upload_to_gcs(
    source_path_or_bytes: str | bytes, destination_blob_name: str
) -> str:
    """
    Upload a file or bytes to GCS using the storage provider

    Args:
        source_path_or_bytes: Either a path in the storage system or bytes content
        bucket_name: GCS bucket name
        destination_blob_name: Destination blob name

    Returns:
        Public URL of the uploaded blob
    """
    temp_storage = get_temp_storage()
    permanent_storage = get_final_storage()

    # If source is bytes, save directly to permanent storage
    if isinstance(source_path_or_bytes, bytes):
        return await permanent_storage.save_bytes(
            source_path_or_bytes, destination_blob_name
        )

    # If source is a path in our temporary storage, get the bytes and save to permanent
    content = await temp_storage.get_bytes(source_path_or_bytes)
    return await permanent_storage.save_bytes(content, destination_blob_name)


def main():
    parser = argparse.ArgumentParser(
        description="Upload GeoParquet STAC files to Google Cloud Storage"
    )
    parser.add_argument(
        "--source-dir", required=True, help="Directory containing GeoParquet files"
    )
    parser.add_argument("--bucket", required=True, help="GCS bucket name")
    parser.add_argument("--prefix", default="stac", help="Prefix for blob names")
    args = parser.parse_args()

    source_dir = args.source_dir
    bucket_name = args.bucket
    prefix = args.prefix

    if not os.path.exists(source_dir):
        print(f"Error: Source directory {source_dir} does not exist")
        sys.exit(1)

    # Find all parquet files in the source directory
    parquet_files = list(Path(source_dir).glob("*.parquet"))

    if not parquet_files:
        print(f"No parquet files found in {source_dir}")
        sys.exit(1)

    print(f"Found {len(parquet_files)} parquet files")

    # Upload each file
    for parquet_file in parquet_files:
        file_name = parquet_file.name
        destination_blob_name = f"{prefix}/{file_name}"
        try:
            upload_to_gcs(str(parquet_file), bucket_name, destination_blob_name)
        except Exception as e:
            print(f"Error uploading {file_name}: {e}")

    print("Upload complete!")


if __name__ == "__main__":
    main()
