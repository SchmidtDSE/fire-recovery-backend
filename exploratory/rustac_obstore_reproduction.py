#!/usr/bin/env python3
"""
Reproducible example demonstrating rustac-py obstore issues with GCP MinIO.

This script demonstrates two scenarios:
1. Pure obstore operations (write, read, append, write)
2. Mixed obstore/HTTPS operations (write, search via HTTPS, append, write)

Based on rustac-py documentation:
- https://stac-utils.github.io/rustac-py/latest/notebooks/stac-geoparquet/
- https://stac-utils.github.io/rustac-py/latest/notebooks/store/
"""

import asyncio
import os
import time
import uuid
from typing import Dict, Any, List

import rustac
from src.core.storage.minio import MinioCloudStorage


def create_sample_stac_items() -> List[Dict[str, Any]]:
    """Create sample STAC items for testing"""
    base_item = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "properties": {
            "datetime": "2023-08-15T12:00:00Z",
            "fire_event_name": "test_fire",
            "product_type": "fire_severity",
        },
        "geometry": {"type": "Point", "coordinates": [-120.0, 35.0]},
        "bbox": [-120.5, 35.5, -120.0, 36.0],
        "assets": {
            "data": {
                "href": "https://example.com/data.tif",
                "type": "image/tiff",
                "title": "Fire Severity Data",
                "roles": ["data"],
            }
        },
        "links": [],
    }

    # Create 3 unique items
    items = []
    for i in range(3):
        item = base_item.copy()
        item["id"] = f"test-item-{i + 1}"
        item["properties"] = item["properties"].copy()
        item["properties"]["sequence_number"] = i + 1
        items.append(item)

    return items


async def wait_for_consistency(storage: MinioCloudStorage, test_path: str, expected_count: int, max_retries: int = 10, delay: float = 0.5) -> bool:
    """
    Wait for storage consistency by polling until expected count is reached
    or max retries exceeded.
    """
    print(f"    Waiting for consistency: expecting {expected_count} items...")
    
    for attempt in range(max_retries):
        try:
            # Try both access methods
            store = storage.get_obstore()
            https_url = f"https://{storage.get_url(test_path)}"
            
            # Check via obstore
            try:
                obstore_result = await rustac.read(test_path, store=store)
                obstore_count = len(obstore_result["features"])
            except:
                obstore_count = 0
                
            # Check via HTTPS
            try:
                https_result = await rustac.search(https_url)
                https_count = len(https_result)
            except:
                https_count = 0
                
            print(f"    Attempt {attempt + 1}: obstore={obstore_count}, https={https_count}")
            
            if obstore_count == https_count == expected_count:
                print(f"    ✓ Consistency achieved after {attempt + 1} attempts")
                return True
                
            # Wait before next attempt
            await asyncio.sleep(delay)
            
        except Exception as e:
            print(f"    Attempt {attempt + 1} error: {str(e)}")
            await asyncio.sleep(delay)
    
    print(f"    ❌ Consistency not achieved after {max_retries} attempts")
    return False


async def robust_write_and_verify(storage: MinioCloudStorage, test_path: str, items: List[Dict[str, Any]], expected_count: int) -> bool:
    """
    Write items and wait for both access methods to see them consistently
    """
    store = storage.get_obstore()
    
    # Write the items
    await rustac.write(test_path, items, store=store, format="geoparquet")
    print(f"    ✓ Write completed ({len(items)} items)")
    
    # Wait for consistency
    consistent = await wait_for_consistency(storage, test_path, expected_count)
    
    if not consistent:
        print(f"    ⚠️  Warning: Storage may not be consistent")
    
    return consistent


async def scenario_1_pure_obstore(storage: MinioCloudStorage, test_path: str) -> None:
    """
    Scenario 1: Pure obstore operations
    - Write initial items using obstore
    - Read back using obstore
    - Append new items using obstore
    - Write again using obstore
    """
    print("\n=== SCENARIO 1: Pure obstore operations ===")

    items = create_sample_stac_items()
    initial_items = items[:2]  # First 2 items
    additional_item = [items[2]]  # Third item

    store = storage.get_obstore()

    try:
        # Step 1: Write initial items using obstore with consistency check
        print(f"Step 1: Writing {len(initial_items)} initial items to {test_path}")
        consistent_1 = await robust_write_and_verify(storage, test_path, initial_items, 2)

        # Step 2: Read back using obstore (with explicit verification)
        print("Step 2: Reading items back using obstore")
        result = await rustac.read(test_path, store=store)
        existing_items = result["features"]
        print(f"✓ Read {len(existing_items)} items successfully")

        # Step 3: Append new items (combine existing + new)
        print("Step 3: Appending additional item")
        all_items = existing_items + additional_item
        print(f"  Total items to write: {len(all_items)}")

        # Step 4: Write combined items back using obstore with consistency check
        print("Step 4: Writing combined items back using obstore")
        consistent_2 = await robust_write_and_verify(storage, test_path, all_items, 3)

        # Step 5: Final verification with detailed comparison
        print("Step 5: Final verification with detailed comparison")
        await wait_for_consistency(storage, test_path, 3, max_retries=15, delay=0.5)
        
        # Get final counts from both methods
        try:
            final_result_obstore = await rustac.read(test_path, store=store)
            obstore_final_count = len(final_result_obstore["features"])
        except Exception as e:
            print(f"  Obstore read failed: {e}")
            obstore_final_count = -1
            
        try:
            https_url = f"https://{storage.get_url(test_path)}"
            final_result_https = await rustac.search(https_url)
            https_final_count = len(final_result_https)
        except Exception as e:
            print(f"  HTTPS search failed: {e}")
            https_final_count = -1

        print(f"✓ Final counts - Obstore: {obstore_final_count}, HTTPS: {https_final_count} (expected: 3)")

        if obstore_final_count == https_final_count == 3 and consistent_1 and consistent_2:
            print("🎉 SCENARIO 1 PASSED: Pure obstore operations work correctly")
        else:
            print(f"❌ SCENARIO 1 FAILED:")
            print(f"   Obstore: {obstore_final_count}, HTTPS: {https_final_count}, expected: 3")
            print(f"   Consistency: Initial={consistent_1}, Append={consistent_2}")

    except Exception as e:
        print(f"❌ SCENARIO 1 ERROR: {str(e)}")
        print(f"   Error type: {type(e).__name__}")


async def scenario_2_mixed_operations(
    storage: MinioCloudStorage, test_path: str
) -> None:
    """
    Scenario 2: Mixed obstore/HTTPS operations
    - Write 3 items using obstore
    - Search for one item using HTTPS (demonstrating obstore limitation)
    - Append more items using obstore
    - Write again using obstore
    """
    print("\n=== SCENARIO 2: Mixed obstore/HTTPS operations ===")

    items = create_sample_stac_items()
    store = storage.get_obstore()
    https_url = f"https://{storage.get_url(test_path)}"

    try:
        # Step 1: Write initial 3 items using obstore with consistency check
        print(f"Step 1: Writing {len(items)} items to {test_path}")
        consistent_1 = await robust_write_and_verify(storage, test_path, items, 3)

        # Step 2: Try to search using obstore (should fail per your comment)
        print("Step 2: Attempting search using obstore (expecting failure)")
        try:
            search_result_obstore = await rustac.search(
                test_path,
                store=store,
                filter={
                    "op": "=",
                    "args": [{"property": "properties.sequence_number"}, 2],
                },
            )
            print(
                f"  Unexpected success with obstore search: {len(search_result_obstore)} results"
            )
        except Exception as e:
            print(f"  Expected failure with obstore search: {str(e)}")

        # Step 3: Search using HTTPS URL (should work) - with retry for consistency
        print("Step 3: Searching using HTTPS URL")
        
        # Wait a bit to ensure HTTPS endpoint is consistent
        await asyncio.sleep(1.0)
        
        search_result_https = await rustac.search(
            https_url,
            filter={"op": "=", "args": [{"property": "properties.sequence_number"}, 2]},
        )
        print(f"✓ HTTPS search successful: found {len(search_result_https)} items")

        # Step 4: Read existing items for append operation (with consistency check)
        print("Step 4: Reading existing items using obstore for append")
        
        # Wait for consistency before reading for append
        await wait_for_consistency(storage, test_path, 3, max_retries=10)
        
        existing_result = await rustac.read(test_path, store=store)
        existing_items = existing_result["features"]
        print(f"✓ Read {len(existing_items)} existing items")

        # Step 5: Create additional item and append
        print("Step 5: Creating additional item and appending")
        new_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "test-item-4",
            "properties": {
                "datetime": "2023-08-15T12:00:00Z",
                "fire_event_name": "test_fire",
                "product_type": "fire_severity",
                "sequence_number": 4,
            },
            "geometry": {"type": "Point", "coordinates": [-120.0, 35.0]},
            "bbox": [-120.5, 35.5, -120.0, 36.0],
            "assets": {
                "data": {
                    "href": "https://example.com/data4.tif",
                    "type": "image/tiff",
                    "title": "Fire Severity Data 4",
                    "roles": ["data"],
                }
            },
            "links": [],
        }

        all_items = existing_items + [new_item]
        print(f"  Total items to write: {len(all_items)}")

        # Step 6: Write combined items back using obstore with consistency check
        print("Step 6: Writing combined items back using obstore")
        consistent_2 = await robust_write_and_verify(storage, test_path, all_items, 4)

        # Step 7: Final verification with extended consistency checks
        print("Step 7: Final verification with extended consistency checks")
        await wait_for_consistency(storage, test_path, 4, max_retries=20, delay=0.5)

        # Get final counts with error handling
        try:
            final_result_obstore = await rustac.read(test_path, store=store)
            obstore_count = len(final_result_obstore["features"])
        except Exception as e:
            print(f"  Obstore read failed: {e}")
            obstore_count = -1

        try:
            final_result_https = await rustac.search(https_url)
            https_count = len(final_result_https)
        except Exception as e:
            print(f"  HTTPS search failed: {e}")
            https_count = -1

        print(f"✓ Final counts - Obstore: {obstore_count}, HTTPS: {https_count} (expected: 4)")

        if obstore_count == https_count == 4 and consistent_1 and consistent_2:
            print("🎉 SCENARIO 2 PASSED: Mixed operations work correctly")
        else:
            print(f"❌ SCENARIO 2 FAILED:")
            print(f"   Obstore: {obstore_count}, HTTPS: {https_count}, expected: 4")
            print(f"   Consistency: Initial={consistent_1}, Append={consistent_2}")
            if obstore_count != https_count:
                print("   This indicates inconsistency between access methods")

    except Exception as e:
        print(f"❌ SCENARIO 2 ERROR: {str(e)}")
        print(f"   Error type: {type(e).__name__}")


async def main() -> None:
    """Main function to run both scenarios"""
    print("rustac-py obstore Reproduction Script")
    print("=====================================")

    # Check environment
    required_vars = [
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_TEST_BUCKET",
    ]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        print(f"❌ Missing required environment variables: {missing_vars}")
        print("Please set these variables to run the reproduction script.")
        return

    # Initialize storage
    storage = MinioCloudStorage(
        endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY"),
        secret_key=os.environ.get("MINIO_SECRET_KEY"),
        secure=os.environ.get("MINIO_SECURE", "True").lower() == "true",
        bucket_name=os.environ.get("MINIO_TEST_BUCKET", "test-bucket"),
    )

    # Generate unique test paths
    test_id = str(uuid.uuid4())[:8]
    test_path_1 = f"repro/scenario1_{test_id}.parquet"
    test_path_2 = f"repro/scenario2_{test_id}.parquet"

    print(f"Test ID: {test_id}")
    print(f"Storage endpoint: {storage.endpoint}")
    print(f"Bucket: {storage.bucket_name}")

    try:
        # Run both scenarios
        await scenario_1_pure_obstore(storage, test_path_1)
        await scenario_2_mixed_operations(storage, test_path_2)

        print("\n" + "=" * 50)
        print("SUMMARY")
        print("=" * 50)
        print("If both scenarios pass, rustac-py obstore works correctly.")
        print("If scenario 1 fails, there's an obstore overwrite issue.")
        print(
            "If scenario 2 shows inconsistent counts, there's a sync issue between obstore and HTTPS access."
        )

    finally:
        # Cleanup
        print(f"\nCleaning up test files...")
        try:
            await storage.cleanup()
            print("✓ Cleanup completed")
        except Exception as e:
            print(f"⚠️  Cleanup warning: {str(e)}")


if __name__ == "__main__":
    asyncio.run(main())
