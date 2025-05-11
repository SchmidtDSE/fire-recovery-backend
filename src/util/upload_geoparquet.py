#!/usr/bin/env python3
import argparse
import os
import sys
from google.cloud import storage
from pathlib import Path

def upload_to_gcs(source_file: str, bucket_name: str, destination_blob_name: str):
    """Uploads a file to the specified GCS bucket."""
    # Initialize the client
    storage_client = storage.Client()
    
    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    
    # Upload the file
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file)
    
    print(f"File {source_file} uploaded to gs://{bucket_name}/{destination_blob_name}")
    
    # Make the blob publicly readable
    blob.make_public()
    print(f"Public URL: {blob.public_url}")
    
    return blob.public_url

def main():
    parser = argparse.ArgumentParser(description="Upload GeoParquet STAC files to Google Cloud Storage")
    parser.add_argument('--source-dir', required=True, help="Directory containing GeoParquet files")
    parser.add_argument('--bucket', required=True, help="GCS bucket name")
    parser.add_argument('--prefix', default="stac", help="Prefix for blob names")
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