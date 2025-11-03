import io
import os
import uuid
import zipfile

import geopandas as gpd
import pytest
from geojson_pydantic import Polygon
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry import Point as ShapelyPoint

from src.core.storage.memory import MemoryStorage
from src.core.storage.interface import StorageInterface


@pytest.fixture
def memory_storage() -> StorageInterface:
    """Fixture for memory storage"""
    return MemoryStorage(base_url="memory://test")


@pytest.fixture
async def memory_storage_with_file(
    memory_storage: StorageInterface,
) -> tuple[StorageInterface, str]:
    """Fixture for memory storage with a test file"""
    test_content = b"This is test content"
    test_path = "test_file.txt"
    await memory_storage.save_bytes(test_content, test_path, temporary=True)
    return memory_storage, test_path


@pytest.fixture
def minio_available() -> bool:
    """Check if S3-compatible storage credentials are available for testing"""
    # Check if required S3_* environment variables are set
    # Only S3_* variables are supported
    has_endpoint = os.environ.get("S3_ENDPOINT")
    has_access_key = os.environ.get("S3_ACCESS_KEY_ID")
    has_secret_key = os.environ.get("S3_SECRET_ACCESS_KEY")
    has_bucket = os.environ.get("S3_BUCKET")

    return all([has_endpoint, has_access_key, has_secret_key, has_bucket])


@pytest.fixture
def minio_storage(minio_available: bool) -> StorageInterface:
    """Fixture for S3-compatible storage (MinIO/GCS)"""
    if not minio_available:
        pytest.skip("S3-compatible storage credentials not available for testing")

    from src.core.storage.minio import MinioCloudStorage

    # Use S3_* variables only
    endpoint = os.environ.get("S3_ENDPOINT") or "localhost:9000"
    access_key = os.environ.get("S3_ACCESS_KEY_ID")
    secret_key = os.environ.get("S3_SECRET_ACCESS_KEY")
    secure_str = os.environ.get("S3_SECURE") or "True"
    secure = secure_str.lower() == "true"
    bucket_name = os.environ.get("S3_BUCKET") or "test-bucket"

    return MinioCloudStorage(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
        bucket_name=bucket_name,
    )


@pytest.fixture
def unique_test_id() -> str:
    """Generate a unique test identifier for test isolation"""
    return str(uuid.uuid4())


# STAC Test Asset URLs - these should exist on your S3-compatible test bucket
# Only S3_* variables are supported
_endpoint = os.environ.get("S3_ENDPOINT") or "storage.googleapis.com"
_secure_str = os.environ.get("S3_SECURE") or "True"
_secure = _secure_str.lower() == "true"
_test_bucket = os.environ.get("S3_BUCKET") or "fire-recovery-temp"
_protocol = "https" if _secure else "http"
STAC_TEST_BASE_URL = f"{_protocol}://{_endpoint}/{_test_bucket}/stac"
STAC_TEST_CATALOG_URL = f"{STAC_TEST_BASE_URL}/catalog.json"

# Test asset URLs that should be available on MinIO
STAC_TEST_ASSETS = {
    "rbr_cog": f"{STAC_TEST_BASE_URL}/assets/test_fire-rbr.tif",
    "dnbr_cog": f"{STAC_TEST_BASE_URL}/assets/test_fire-dnbr.tif",
    "rdnbr_cog": f"{STAC_TEST_BASE_URL}/assets/test_fire-rdnbr.tif",
    "boundary_geojson": f"{STAC_TEST_BASE_URL}/assets/test_fire-boundary.geojson",
    "veg_matrix_csv": f"{STAC_TEST_BASE_URL}/assets/test_fire-veg-matrix.csv",
    "veg_matrix_json": f"{STAC_TEST_BASE_URL}/assets/test_fire-veg-matrix.json",
}


@pytest.fixture
def unique_parquet_path(unique_test_id: str) -> str:
    """Generate a unique parquet path for test isolation"""
    return f"stac/test_{unique_test_id}_fire_recovery_stac.parquet"


@pytest.fixture
def sample_geometry() -> Polygon:
    """Sample geometry for testing - shared across all STAC tests"""
    test_polygon = {
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
    return Polygon.model_validate(test_polygon)


@pytest.fixture
def sample_shapefile_zip_bytes(tmp_path) -> bytes:
    """Create a valid shapefile zip in memory for testing"""
    import tempfile

    # Create a simple polygon
    polygon = ShapelyPolygon(
        [
            (-120.0, 35.0),
            (-120.0, 36.0),
            (-119.0, 36.0),
            (-119.0, 35.0),
            (-120.0, 35.0),
        ]
    )

    gdf = gpd.GeoDataFrame({"geometry": [polygon]}, crs="EPSG:4326")

    # Use a temporary directory to write shapefile components
    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = os.path.join(tmpdir, "test.shp")
        gdf.to_file(shp_path, driver="ESRI Shapefile")

        # Create a zip file in memory containing the shapefile
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in os.listdir(tmpdir):
                file_path = os.path.join(tmpdir, file)
                zipf.write(file_path, arcname=file)

        zip_buffer.seek(0)
        return zip_buffer.read()


@pytest.fixture
def multipolygon_shapefile_zip_bytes(tmp_path) -> bytes:
    """Create shapefile with multiple polygons"""
    import tempfile

    polygon1 = ShapelyPolygon(
        [(-120, 35), (-120, 36), (-119, 36), (-119, 35), (-120, 35)]
    )
    polygon2 = ShapelyPolygon(
        [(-119, 35), (-119, 36), (-118, 36), (-118, 35), (-119, 35)]
    )

    gdf = gpd.GeoDataFrame({"geometry": [polygon1, polygon2]}, crs="EPSG:4326")

    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = os.path.join(tmpdir, "test.shp")
        gdf.to_file(shp_path, driver="ESRI Shapefile")

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in os.listdir(tmpdir):
                file_path = os.path.join(tmpdir, file)
                zipf.write(file_path, arcname=file)

        zip_buffer.seek(0)
        return zip_buffer.read()


@pytest.fixture
def empty_shapefile_zip_bytes(tmp_path) -> bytes:
    """Create empty shapefile"""
    import tempfile

    gdf = gpd.GeoDataFrame({"geometry": []}, crs="EPSG:4326")

    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = os.path.join(tmpdir, "test.shp")
        gdf.to_file(shp_path, driver="ESRI Shapefile")

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in os.listdir(tmpdir):
                file_path = os.path.join(tmpdir, file)
                zipf.write(file_path, arcname=file)

        zip_buffer.seek(0)
        return zip_buffer.read()


@pytest.fixture
def point_shapefile_zip_bytes(tmp_path) -> bytes:
    """Create shapefile with Point geometry (invalid for boundaries)"""
    import tempfile

    point = ShapelyPoint(-120.0, 35.0)
    gdf = gpd.GeoDataFrame({"geometry": [point]}, crs="EPSG:4326")

    with tempfile.TemporaryDirectory() as tmpdir:
        shp_path = os.path.join(tmpdir, "test.shp")
        gdf.to_file(shp_path, driver="ESRI Shapefile")

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file in os.listdir(tmpdir):
                file_path = os.path.join(tmpdir, file)
                zipf.write(file_path, arcname=file)

        zip_buffer.seek(0)
        return zip_buffer.read()


@pytest.fixture
def zip_without_shp_bytes() -> bytes:
    """Create zip file without .shp component"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        zf.writestr("test.txt", "Not a shapefile")
    buffer.seek(0)
    return buffer.read()
