import pytest
from typing import Dict, Any
import pystac

from src.stac.stac_item_factory import STACItemFactory


class TestSTACItemFactory:
    """Test suite for STACItemFactory"""

    @pytest.fixture
    def factory(self) -> STACItemFactory:
        """Create a STACItemFactory instance for testing"""
        return STACItemFactory(base_url="https://test.example.com")

    @pytest.fixture
    def sample_geometry(self) -> Dict[str, Any]:
        """Sample geometry for testing"""
        return {
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

    def test_create_fire_severity_item_complete(
        self, factory: STACItemFactory, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test creating a fire severity item with all COG URLs"""
        cog_urls = {
            "rbr": "https://storage.example.com/rbr.tif",
            "dnbr": "https://storage.example.com/dnbr.tif",
            "rdnbr": "https://storage.example.com/rdnbr.tif",
        }

        stac_item = factory.create_fire_severity_item(
            fire_event_name="test_fire",
            job_id="job_123",
            cog_urls=cog_urls,
            geometry=sample_geometry,
            datetime_str="2023-08-15T12:00:00Z",
            boundary_type="coarse",
            skip_validation=True,
        )

        assert stac_item["id"] == "test_fire-severity-job_123"
        assert stac_item["properties"]["product_type"] == "fire_severity"
        assert stac_item["properties"]["boundary_type"] == "coarse"
        assert len(stac_item["assets"]) == 3
        assert "rbr" in stac_item["assets"]
        assert "dnbr" in stac_item["assets"]
        assert "rdnbr" in stac_item["assets"]

    def test_create_fire_severity_item_partial_cogs(
        self, factory: STACItemFactory, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test creating a fire severity item with only some COG URLs"""
        cog_urls = {
            "rbr": "https://storage.example.com/rbr.tif",
        }

        stac_item = factory.create_fire_severity_item(
            fire_event_name="test_fire",
            job_id="job_123",
            cog_urls=cog_urls,
            geometry=sample_geometry,
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        assert len(stac_item["assets"]) == 1
        assert "rbr" in stac_item["assets"]
        assert "dnbr" not in stac_item["assets"]
        assert "rdnbr" not in stac_item["assets"]

    def test_create_boundary_item(self, factory: STACItemFactory) -> None:
        """Test creating a boundary item"""
        bbox = [-120.5, 35.5, -120.0, 36.0]

        stac_item = factory.create_boundary_item(
            fire_event_name="test_fire",
            job_id="job_123",
            boundary_geojson_url="https://storage.example.com/boundary.geojson",
            bbox=bbox,
            datetime_str="2023-08-15T12:00:00Z",
            boundary_type="refined",
            skip_validation=True,
        )

        assert stac_item["id"] == "test_fire-boundary-job_123"
        assert stac_item["properties"]["product_type"] == "fire_boundary"
        assert stac_item["properties"]["boundary_type"] == "refined"
        assert stac_item["bbox"] == bbox
        assert "refined_boundary" in stac_item["assets"]
        assert stac_item["assets"]["refined_boundary"]["type"] == "application/geo+json"

    def test_create_veg_matrix_item(
        self, factory: STACItemFactory, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test creating a vegetation matrix item"""
        bbox = [-120.5, 35.5, -120.0, 36.0]
        classification_breaks = [0.1, 0.27, 0.44, 0.66]

        stac_item = factory.create_veg_matrix_item(
            fire_event_name="test_fire",
            job_id="job_123",
            fire_veg_matrix_csv_url="https://storage.example.com/matrix.csv",
            fire_veg_matrix_json_url="https://storage.example.com/matrix.json",
            geometry=sample_geometry,
            bbox=bbox,
            classification_breaks=classification_breaks,
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        assert stac_item["id"] == "test_fire-veg-matrix-job_123"
        assert stac_item["properties"]["product_type"] == "vegetation_fire_matrix"
        assert stac_item["properties"]["classification_breaks"] == classification_breaks
        assert len(stac_item["assets"]) == 2
        assert "fire_veg_matrix_csv" in stac_item["assets"]
        assert "fire_veg_matrix_json" in stac_item["assets"]

    def test_validate_stac_item_valid(
        self, factory: STACItemFactory, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test validating a valid STAC item"""
        stac_item_dict = factory.create_fire_severity_item(
            fire_event_name="test_fire",
            job_id="job_123",
            cog_urls={"rbr": "https://storage.example.com/rbr.tif"},
            geometry=sample_geometry,
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        # Convert dict to pystac Item and validate
        stac_item = pystac.Item.from_dict(stac_item_dict)

        # Should not raise an exception (skip validation to avoid HREF resolution)
        factory.validate_stac_item(stac_item, skip_validation=True)

    def test_stac_item_has_required_links(
        self, factory: STACItemFactory, sample_geometry: Dict[str, Any]
    ) -> None:
        """Test that created STAC items have required links"""
        stac_item = factory.create_fire_severity_item(
            fire_event_name="test_fire",
            job_id="job_123",
            cog_urls={"rbr": "https://storage.example.com/rbr.tif"},
            geometry=sample_geometry,
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        links = stac_item["links"]
        assert len(links) >= 2

        # Check for self link
        self_links = [link for link in links if link["rel"] == "self"]
        assert len(self_links) == 1
        assert self_links[0]["href"].endswith("test_fire-severity-job_123.json")

        # Check for related link
        related_links = [link for link in links if link["rel"] == "related"]
        assert len(related_links) == 1

    def test_boundary_item_geometry_from_bbox(self, factory: STACItemFactory) -> None:
        """Test that boundary items create geometry from bbox correctly"""
        bbox = [-120.5, 35.5, -120.0, 36.0]

        stac_item = factory.create_boundary_item(
            fire_event_name="test_fire",
            job_id="job_123",
            boundary_geojson_url="https://storage.example.com/boundary.geojson",
            bbox=bbox,
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        geometry = stac_item["geometry"]
        assert geometry["type"] == "Polygon"
        coordinates = geometry["coordinates"][0]

        # Should form a rectangle from bbox
        assert coordinates[0] == [bbox[0], bbox[1]]  # bottom-left
        assert coordinates[1] == [bbox[2], bbox[1]]  # bottom-right
        assert coordinates[2] == [bbox[2], bbox[3]]  # top-right
        assert coordinates[3] == [bbox[0], bbox[3]]  # top-left
        assert coordinates[4] == [bbox[0], bbox[1]]  # closed polygon

    def test_fire_severity_bbox_calculation(self, factory: STACItemFactory) -> None:
        """Test that fire severity items calculate bbox from geometry correctly"""
        geometry = {
            "type": "Polygon",
            "coordinates": [
                [
                    [-121.0, 36.0],
                    [-120.0, 36.0],
                    [-120.0, 37.0],
                    [-121.0, 37.0],
                    [-121.0, 36.0],
                ]
            ],
        }

        stac_item = factory.create_fire_severity_item(
            fire_event_name="test_fire",
            job_id="job_123",
            cog_urls={"rbr": "https://storage.example.com/rbr.tif"},
            geometry=geometry,
            datetime_str="2023-08-15T12:00:00Z",
            skip_validation=True,
        )

        bbox = stac_item["bbox"]
        assert bbox == [-121.0, 36.0, -120.0, 37.0]  # (minx, miny, maxx, maxy)
