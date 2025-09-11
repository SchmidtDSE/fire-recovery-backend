import pytest
import uuid

from geojson_pydantic import Polygon
from src.stac.stac_json_manager import STACJSONManager
from src.core.storage.minio import MinioCloudStorage


@pytest.mark.asyncio
async def test_stac_json_manager_create_fire_severity_item(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test creating a fire severity STAC item with MinIO storage using JSON files"""
    test_id = str(uuid.uuid4())
    base_url = f"https://test.example.com/{test_id}"
    manager = STACJSONManager(base_url=base_url, storage=minio_storage)

    # Test data
    fire_event_name = f"test_fire_{test_id}"
    job_id = f"job_{test_id}"
    cog_urls = {
        "rbr": f"https://storage.example.com/rbr_{test_id}.tif",
        "dnbr": f"https://storage.example.com/dnbr_{test_id}.tif",
        "rdnbr": f"https://storage.example.com/rdnbr_{test_id}.tif",
    }
    geometry_dict = {
        "type": "Polygon",
        "coordinates": [
            [
                [-120.5, 35.5],
                [-120.0, 35.5],
                [-120.0, 36.0],
                [-120.5, 36.0],
                [-120.5, 35.5],
            ]
        ],
    }
    geometry = Polygon.model_validate(geometry_dict)
    datetime_str = "2023-08-15T12:00:00Z"

    try:
        # Create fire severity item
        await manager.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_urls=cog_urls,
            geometry=geometry,
            datetime_str=datetime_str,
            boundary_type="coarse",
            skip_validation=True,
        )

        # Get the actual STAC item for validation
        retrieved_items = await manager.get_items_by_fire_event(fire_event_name)
        assert len(retrieved_items) == 1
        stac_item = retrieved_items[0]  # Get the first (and only) item

        # Validate the created item structure (pystac validation already done in factory)
        assert stac_item["id"] == f"{fire_event_name}-severity-{job_id}"
        assert stac_item["properties"]["fire_event_name"] == fire_event_name
        assert stac_item["properties"]["job_id"] == job_id
        assert stac_item["properties"]["product_type"] == "fire_severity"
        assert stac_item["properties"]["boundary_type"] == "coarse"
        assert stac_item["geometry"]["type"] == "Polygon"
        assert len(stac_item["geometry"]["coordinates"]) == 1  # One ring
        assert len(stac_item["geometry"]["coordinates"][0]) == 5  # Closed polygon

        # Check assets
        assert "rbr" in stac_item["assets"]
        assert "dnbr" in stac_item["assets"]
        assert "rdnbr" in stac_item["assets"]
        assert stac_item["assets"]["rbr"]["href"] == cog_urls["rbr"]

        # Verify item was stored as individual JSON file
        assert stac_item["id"] == f"{fire_event_name}-severity-{job_id}"

        # Verify file exists in storage with correct path structure
        __expected_path = f"stac/{fire_event_name}/fire_severity-{job_id}.json"
        files = await minio_storage.list_files(f"stac/{fire_event_name}/")
        assert any("fire_severity-" in f for f in files), (
            f"Expected severity file not found in: {files}"
        )

    finally:
        # Cleanup
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_stac_json_manager_create_boundary_item(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test creating a boundary STAC item with MinIO storage using JSON files"""
    test_id = str(uuid.uuid4())
    base_url = f"https://test.example.com/{test_id}"
    manager = STACJSONManager(base_url=base_url, storage=minio_storage)

    # Test data
    fire_event_name = f"test_fire_{test_id}"
    job_id = f"job_{test_id}"
    boundary_geojson_url = f"https://storage.example.com/boundary_{test_id}.geojson"
    bbox = [-120.5, 35.5, -120.0, 36.0]
    datetime_str = "2023-08-15T12:00:00Z"

    try:
        # Create boundary item
        stac_item = await manager.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            boundary_geojson_url=boundary_geojson_url,
            bbox=bbox,
            datetime_str=datetime_str,
            boundary_type="refined",
            skip_validation=True,
        )

        # Validate the created item
        assert stac_item["id"] == f"{fire_event_name}-boundary-{job_id}"
        assert stac_item["properties"]["fire_event_name"] == fire_event_name
        assert stac_item["properties"]["product_type"] == "fire_boundary"
        assert stac_item["properties"]["boundary_type"] == "refined"
        assert stac_item["bbox"] == bbox

        # Check assets
        assert "refined_boundary" in stac_item["assets"]
        assert stac_item["assets"]["refined_boundary"]["href"] == boundary_geojson_url

        # Verify item was stored
        retrieved_item = await manager.get_item_by_fire_event_and_id(
            fire_event_name, stac_item["id"]
        )
        assert retrieved_item is not None
        assert retrieved_item["id"] == stac_item["id"]

    finally:
        # Cleanup
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_stac_json_manager_create_veg_matrix_item(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test creating a vegetation matrix STAC item with MinIO storage using JSON files"""
    test_id = str(uuid.uuid4())
    base_url = f"https://test.example.com/{test_id}"
    manager = STACJSONManager(base_url=base_url, storage=minio_storage)

    # Test data
    fire_event_name = f"test_fire_{test_id}"
    job_id = f"job_{test_id}"
    csv_url = f"https://storage.example.com/veg_matrix_{test_id}.csv"
    json_url = f"https://storage.example.com/veg_matrix_{test_id}.json"
    geometry_dict = {
        "type": "Polygon",
        "coordinates": [
            [
                [-120.5, 35.5],
                [-120.0, 35.5],
                [-120.0, 36.0],
                [-120.5, 36.0],
                [-120.5, 35.5],
            ]
        ],
    }
    geometry = Polygon.model_validate(geometry_dict)
    bbox = [-120.5, 35.5, -120.0, 36.0]
    classification_breaks = [0.1, 0.27, 0.44, 0.66]
    datetime_str = "2023-08-15T12:00:00Z"

    try:
        # Create veg matrix item
        stac_item = await manager.create_veg_matrix_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            fire_veg_matrix_csv_url=csv_url,
            fire_veg_matrix_json_url=json_url,
            geometry=geometry,
            bbox=bbox,
            classification_breaks=classification_breaks,
            datetime_str=datetime_str,
            skip_validation=True,
        )

        # Validate the created item
        assert stac_item["id"] == f"{fire_event_name}-veg-matrix-{job_id}"
        assert stac_item["properties"]["fire_event_name"] == fire_event_name
        assert stac_item["properties"]["product_type"] == "vegetation_fire_matrix"
        assert stac_item["properties"]["classification_breaks"] == classification_breaks

        # Check assets
        assert "fire_veg_matrix_csv" in stac_item["assets"]
        assert "fire_veg_matrix_json" in stac_item["assets"]
        assert stac_item["assets"]["fire_veg_matrix_csv"]["href"] == csv_url
        assert stac_item["assets"]["fire_veg_matrix_json"]["href"] == json_url

        # Verify item retrieval by classification breaks
        retrieved_item = await manager.get_items_by_id_and_classification_breaks(
            stac_item["id"], classification_breaks
        )
        assert retrieved_item is not None
        assert retrieved_item["id"] == stac_item["id"]

    finally:
        # Cleanup
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_stac_json_manager_multiple_items_and_search(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test creating multiple items and searching with MinIO storage using JSON files"""
    test_id = str(uuid.uuid4())
    base_url = f"https://test.example.com/{test_id}"
    manager = STACJSONManager(base_url=base_url, storage=minio_storage)

    # Test data for multiple fire events
    fire_event_1 = f"fire_one_{test_id}"
    fire_event_2 = f"fire_two_{test_id}"
    job_id = f"job_{test_id}"

    try:
        # Create items for first fire event
        await manager.create_fire_severity_item(
            fire_event_name=fire_event_1,
            job_id=job_id,
            cog_urls={"rbr": "https://example.com/rbr1.tif"},
            geometry=Polygon.model_validate({"type": "Polygon", "coordinates": [[[-120.0, 35.0], [-119.9, 35.0], [-119.9, 35.1], [-120.0, 35.1], [-120.0, 35.0]]]}),
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        await manager.create_boundary_item(
            fire_event_name=fire_event_1,
            job_id=job_id,
            boundary_geojson_url="https://example.com/boundary1.geojson",
            bbox=[-120.5, 35.5, -120.0, 36.0],
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        # Create items for second fire event
        await manager.create_fire_severity_item(
            fire_event_name=fire_event_2,
            job_id=job_id,
            cog_urls={"dnbr": "https://example.com/dnbr2.tif"},
            geometry=Polygon.model_validate({"type": "Polygon", "coordinates": [[[-121.0, 36.0], [-120.9, 36.0], [-120.9, 36.1], [-121.0, 36.1], [-121.0, 36.0]]]}),
            datetime_str="2023-08-16T12:00:00Z",
            skip_validation=True,
        )

        # Test searching by fire event
        fire1_items = await manager.get_items_by_fire_event(fire_event_1)
        assert len(fire1_items) == 2

        fire2_items = await manager.get_items_by_fire_event(fire_event_2)
        assert len(fire2_items) == 1

        # Test searching by product type
        severity_items = await manager.search_items(
            fire_event_name=fire_event_1, product_type="fire_severity"
        )
        assert len(severity_items) == 1
        assert severity_items[0]["properties"]["product_type"] == "fire_severity"

        boundary_items = await manager.search_items(
            fire_event_name=fire_event_1, product_type="fire_boundary"
        )
        assert len(boundary_items) == 1
        assert boundary_items[0]["properties"]["product_type"] == "fire_boundary"

        # Verify individual JSON file storage structure
        fire1_files = await minio_storage.list_files(f"stac/{fire_event_1}/")
        assert len([f for f in fire1_files if f.endswith(".json")]) == 2

        fire2_files = await minio_storage.list_files(f"stac/{fire_event_2}/")
        assert len([f for f in fire2_files if f.endswith(".json")]) == 1

    finally:
        # Cleanup
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_stac_json_manager_factory_methods(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test the class method factory functions"""
    test_id = str(uuid.uuid4())
    base_url = f"https://test.example.com/{test_id}"

    # Test testing factory method
    test_manager = STACJSONManager.for_testing(base_url)
    assert test_manager.base_url == base_url
    assert test_manager.storage is not None
    # Testing storage should be MemoryStorage (per config: TEMP_STORAGE_PROVIDER_TYPE = "memory")
    assert test_manager.storage.__class__.__name__ == "MemoryStorage"

    # Test production factory method
    prod_manager = STACJSONManager.for_production(base_url)
    assert prod_manager.base_url == base_url
    assert prod_manager.storage is not None
    # Production storage should be MinIO (per config: FINAL_STORAGE_PROVIDER_TYPE = "minio")
    assert prod_manager.storage.__class__.__name__ == "MinioCloudStorage"
    assert hasattr(prod_manager.storage, "bucket_name")

    # They should use different storage instances
    assert test_manager.storage != prod_manager.storage


@pytest.mark.asyncio
async def test_stac_json_manager_empty_handling(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test handling when no JSON files exist yet"""
    test_id = str(uuid.uuid4())
    base_url = f"https://test.example.com/{test_id}"
    manager = STACJSONManager(base_url=base_url, storage=minio_storage)

    fire_event_name = f"empty_test_{test_id}"

    try:
        # Search for items in non-existent fire event should return empty list
        items = await manager.get_items_by_fire_event(fire_event_name)
        assert items == []

        # Get item by ID should return None
        item = await manager.get_item_by_id("non-existent-id")
        assert item is None

        # Search should return empty list
        search_results = await manager.search_items(fire_event_name)
        assert search_results == []

    finally:
        # Cleanup
        await minio_storage.cleanup()


@pytest.mark.asyncio
async def test_stac_json_manager_individual_file_storage(
    minio_storage: MinioCloudStorage,
) -> None:
    """Test that items are stored as individual JSON files with correct naming"""
    test_id = str(uuid.uuid4())
    base_url = f"https://test.example.com/{test_id}"
    manager = STACJSONManager(base_url=base_url, storage=minio_storage)

    fire_event_name = f"storage_test_{test_id}"
    job_id = f"job_{test_id}"

    try:
        # Create items of different types
        await manager.create_fire_severity_item(
            fire_event_name=fire_event_name,
            job_id=job_id,
            cog_urls={"rbr": "https://example.com/rbr.tif"},
            geometry=Polygon.model_validate({"type": "Polygon", "coordinates": [[[-120.0, 35.0], [-119.9, 35.0], [-119.9, 35.1], [-120.0, 35.1], [-120.0, 35.0]]]}),
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        await manager.create_boundary_item(
            fire_event_name=fire_event_name,
            job_id=f"{job_id}_boundary",
            boundary_geojson_url="https://example.com/boundary.geojson",
            bbox=[-120.5, 35.5, -120.0, 36.0],
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        # Verify individual files exist with correct naming convention
        files = await minio_storage.list_files(f"stac/{fire_event_name}/")
        json_files = [f for f in files if f.endswith(".json")]

        assert len(json_files) == 2

        # Check file naming convention: {product_type}-{job_id}.json
        expected_files = [
            f"fire_severity-{job_id}.json",
            f"fire_boundary-{job_id}_boundary.json",
        ]

        for expected_file in expected_files:
            assert any(expected_file in f for f in json_files), (
                f"Expected {expected_file} not found in {json_files}"
            )

        # Verify we can retrieve individual items directly
        severity_item = await manager.get_item_by_fire_event_and_id(
            fire_event_name, f"{fire_event_name}-severity-{job_id}"
        )
        assert severity_item is not None
        assert severity_item["properties"]["product_type"] == "fire_severity"

        boundary_item = await manager.get_item_by_fire_event_and_id(
            fire_event_name, f"{fire_event_name}-boundary-{job_id}_boundary"
        )
        assert boundary_item is not None
        assert boundary_item["properties"]["product_type"] == "fire_boundary"

    finally:
        # Cleanup
        await minio_storage.cleanup()
