#!/usr/bin/env python3
import argparse
import os
import sys
from minio import Minio
from minio.error import S3Error
from pathlib import Path
from urllib.parse import urlparse

def upload_to_gcs(source_file: str, bucket_name: str, destination_blob_name: str):
    """Uploads a file to the specified GCS bucket using Minio client."""
    # Get credentials from environment variables
    access_key = os.environ.get('GCP_ACCESS_KEY_ID')
    secret_key = os.environ.get('GCP_SECRET_ACCESS_KEY')
    
    if not access_key or not secret_key:
        raise ValueError("GCP_ACCESS_KEY_ID and GCP_SECRET_ACCESS_KEY environment variables must be set")
    
    # Initialize Minio client - for GCS compatibility use their endpoint
    endpoint = "storage.googleapis.com"
    client = Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=True,  # Use HTTPS
        region="auto"
    )
    
    # Check if bucket exists, create if not
    if not client.bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} does not exist. Creating...")
        client.make_bucket(bucket_name)
    
    # Upload file
    try:
        client.fput_object(
            bucket_name, 
            destination_blob_name, 
            source_file,
            content_type="application/octet-stream"
        )
        
        print(f"File {source_file} uploaded to gs://{bucket_name}/{destination_blob_name}")
        
        # Make object publicly readable by setting policy (if needed)
        # This is simplified - you may need a more elaborate policy
        client.set_object_acl(bucket_name, destination_blob_name, "public-read")
        
        # Construct public URL
        public_url = f"https://{endpoint}/{bucket_name}/{destination_blob_name}"
        print(f"Public URL: {public_url}")
        
        return public_url
    except S3Error as e:
        raise Exception(f"Error uploading to GCS: {e}")

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