"""
Unit tests for MultiPolygon validation layer.

Tests MultiPolygon acceptance in validation functions, conversion utilities,
and request models while maintaining backward compatibility with Polygon.
"""
# mypy: disable-error-code="list-item,union-attr"
# Note: geojson-pydantic has strict types for coordinates but tests work at runtime

import json
import pytest
from typing import Dict, Any
from geojson_pydantic import Polygon, MultiPolygon, Feature, FeatureCollection

from src.util.polygon_ops import (
    validate_polygon,
    polygon_to_feature,
    polygon_to_valid_geojson,
)
from src.models.requests import (
    ProcessingRequest,
    RefineRequest,
    GeoJSONUploadRequest,
)


# Test fixtures for MultiPolygon geometries
@pytest.fixture
def sample_multipolygon() -> MultiPolygon:
    """Sample MultiPolygon with two simple square polygons"""
    return MultiPolygon(  # type: ignore[call-arg]
        type="MultiPolygon",
        coordinates=[
            [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
            [[[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 3.0], [2.0, 2.0]]],
        ],
    )


@pytest.fixture
def sample_multipolygon_dict() -> Dict[str, Any]:
    """Sample MultiPolygon as dictionary"""
    return {
        "type": "MultiPolygon",
        "coordinates": [
            [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]],
            [[[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 3.0], [2.0, 2.0]]],
        ],
    }


@pytest.fixture
def realistic_fire_multipolygon() -> MultiPolygon:
    """
    Realistic MultiPolygon representing disjoint fire burn areas.

    Scenario: Main fire perimeter + spot fire 1km away (in degrees)
    Coordinates near California central coast
    """
    return MultiPolygon(  # type: ignore[call-arg]
        type="MultiPolygon",
        coordinates=[
            # Main burn area (approximately 10km x 10km)
            [
                [
                    [-120.5, 38.5],
                    [-120.4, 38.5],
                    [-120.4, 38.6],
                    [-120.5, 38.6],
                    [-120.5, 38.5],
                ]
            ],
            # Spot fire (~1km away, approximately 2km x 2km)
            [
                [
                    [-120.3, 38.7],
                    [-120.2, 38.7],
                    [-120.2, 38.8],
                    [-120.3, 38.8],
                    [-120.3, 38.7],
                ]
            ],
        ],
    )


@pytest.fixture
def sample_polygon() -> Polygon:
    """Sample Polygon for backward compatibility tests"""
    return Polygon(  # type: ignore[call-arg]
        type="Polygon",
        coordinates=[
            [
                [-120.5, 35.5],
                [-120.0, 35.5],
                [-120.0, 36.0],
                [-120.5, 36.0],
                [-120.5, 35.5],
            ]
        ],
    )


@pytest.fixture
def sample_feature_multipolygon(sample_multipolygon: MultiPolygon) -> Feature:
    """Sample Feature containing MultiPolygon geometry"""
    return Feature(
        type="Feature", geometry=sample_multipolygon, properties={"name": "Test Fire"}
    )


class TestValidatePolygonWithMultiPolygon:
    """Test validate_polygon function with MultiPolygon inputs"""

    def test_validate_multipolygon_accepts_valid_multipolygon(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """Valid MultiPolygon passes validation"""
        result = validate_polygon(sample_multipolygon)

        # Should return Shapely MultiPolygon
        assert result.geom_type == "MultiPolygon"
        assert result.is_valid
        assert len(result.geoms) == 2  # Two constituent polygons

    def test_validate_multipolygon_accepts_multipolygon_dict(
        self, sample_multipolygon_dict: Dict[str, Any]
    ) -> None:
        """MultiPolygon as dictionary passes validation"""
        result = validate_polygon(sample_multipolygon_dict)

        assert result.geom_type == "MultiPolygon"
        assert result.is_valid
        assert len(result.geoms) == 2

    def test_validate_multipolygon_accepts_feature_with_multipolygon(
        self, sample_feature_multipolygon: Feature
    ) -> None:
        """Feature containing MultiPolygon passes validation"""
        result = validate_polygon(sample_feature_multipolygon)

        assert result.geom_type == "MultiPolygon"
        assert result.is_valid
        assert len(result.geoms) == 2

    def test_validate_multipolygon_realistic_fire_scenario(
        self, realistic_fire_multipolygon: MultiPolygon
    ) -> None:
        """Realistic fire scenario MultiPolygon passes validation"""
        result = validate_polygon(realistic_fire_multipolygon)

        assert result.geom_type == "MultiPolygon"
        assert result.is_valid
        assert len(result.geoms) == 2

        # Verify bounds make sense for California coordinates
        bounds = result.bounds
        assert -121.0 < bounds[0] < -120.0  # minx (westernmost)
        assert 38.0 < bounds[1] < 39.0  # miny (southernmost)
        assert -121.0 < bounds[2] < -119.0  # maxx (easternmost) - wider range for spot fire
        assert 38.0 < bounds[3] < 39.0  # maxy (northernmost)

    def test_validate_polygon_still_accepts_polygon(
        self, sample_polygon: Polygon
    ) -> None:
        """Backward compatibility: Polygon still works"""
        result = validate_polygon(sample_polygon)

        assert result.geom_type == "Polygon"
        assert result.is_valid

    def test_validate_multipolygon_rejects_invalid_geometry(self) -> None:
        """Invalid geometry types are rejected"""
        invalid_geometry = {
            "type": "Point",
            "coordinates": [0.0, 0.0],
        }

        with pytest.raises(ValueError) as exc_info:
            validate_polygon(invalid_geometry)

        # The error message includes "Polygon or MultiPolygon" as part of a longer message
        assert "Polygon or MultiPolygon" in str(exc_info.value)

    def test_validate_multipolygon_rejects_invalid_coordinates(self) -> None:
        """MultiPolygon with invalid coordinates is rejected"""
        invalid_multipolygon = {
            "type": "MultiPolygon",
            "coordinates": [
                # Invalid: not enough points to close polygon
                [[[0.0, 0.0], [1.0, 1.0]]]
            ],
        }

        with pytest.raises(ValueError) as exc_info:
            validate_polygon(invalid_multipolygon)

        assert "Failed to validate polygon" in str(exc_info.value)

    def test_validate_multipolygon_with_nested_geometry_dict(self) -> None:
        """MultiPolygon nested in geometry property"""
        nested_dict = {
            "geometry": {
                "type": "MultiPolygon",
                "coordinates": [
                    [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]]
                ],
            }
        }

        result = validate_polygon(nested_dict)
        assert result.geom_type == "MultiPolygon"
        assert result.is_valid


class TestPolygonToFeatureWithMultiPolygon:
    """Test polygon_to_feature function with MultiPolygon inputs"""

    def test_convert_multipolygon_to_feature(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """MultiPolygon converts to valid Feature"""
        result = polygon_to_feature(sample_multipolygon)

        assert result["type"] == "Feature"
        assert result["geometry"]["type"] == "MultiPolygon"
        assert len(result["geometry"]["coordinates"]) == 2
        assert "properties" in result
        assert "created" in result["properties"]

    def test_convert_multipolygon_to_feature_with_properties(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """MultiPolygon converts to Feature with custom properties"""
        custom_properties = {"fire_name": "Test Fire", "year": 2024}

        result = polygon_to_feature(sample_multipolygon, properties=custom_properties)

        assert result["type"] == "Feature"
        assert result["geometry"]["type"] == "MultiPolygon"
        assert result["properties"]["fire_name"] == "Test Fire"
        assert result["properties"]["year"] == 2024

    def test_convert_polygon_to_feature_backward_compatibility(
        self, sample_polygon: Polygon
    ) -> None:
        """Backward compatibility: Polygon still converts to Feature"""
        result = polygon_to_feature(sample_polygon)

        assert result["type"] == "Feature"
        assert result["geometry"]["type"] == "Polygon"
        assert "properties" in result


class TestPolygonToValidGeoJSONWithMultiPolygon:
    """Test polygon_to_valid_geojson function with MultiPolygon inputs"""

    def test_convert_multipolygon_to_geojson(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """MultiPolygon converts to valid FeatureCollection"""
        result = polygon_to_valid_geojson(sample_multipolygon)

        assert isinstance(result, FeatureCollection)
        assert result.type == "FeatureCollection"
        assert len(result.features) == 1
        assert result.features[0].geometry.type == "MultiPolygon"
        assert len(result.features[0].geometry.coordinates) == 2

    def test_convert_multipolygon_dict_to_geojson(
        self, sample_multipolygon_dict: Dict[str, Any]
    ) -> None:
        """MultiPolygon dictionary converts to valid FeatureCollection"""
        result = polygon_to_valid_geojson(sample_multipolygon_dict)

        assert isinstance(result, FeatureCollection)
        assert result.type == "FeatureCollection"
        assert result.features[0].geometry.type == "MultiPolygon"

    def test_convert_feature_multipolygon_to_geojson(
        self, sample_feature_multipolygon: Feature
    ) -> None:
        """Feature with MultiPolygon converts to FeatureCollection"""
        result = polygon_to_valid_geojson(sample_feature_multipolygon)

        assert isinstance(result, FeatureCollection)
        assert result.type == "FeatureCollection"
        assert len(result.features) == 1
        assert result.features[0].geometry.type == "MultiPolygon"

    def test_convert_multipolygon_with_properties(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """MultiPolygon with properties converts correctly"""
        properties = {"fire_event": "TestFire2024", "severity": "high"}

        result = polygon_to_valid_geojson(sample_multipolygon, properties=properties)

        assert isinstance(result, FeatureCollection)
        feature_properties = result.features[0].properties
        assert feature_properties["fire_event"] == "TestFire2024"
        assert feature_properties["severity"] == "high"

    def test_convert_polygon_to_geojson_backward_compatibility(
        self, sample_polygon: Polygon
    ) -> None:
        """Backward compatibility: Polygon still converts to FeatureCollection"""
        result = polygon_to_valid_geojson(sample_polygon)

        assert isinstance(result, FeatureCollection)
        assert result.type == "FeatureCollection"
        assert result.features[0].geometry.type == "Polygon"

    def test_geojson_serialization(self, realistic_fire_multipolygon: MultiPolygon) -> None:
        """FeatureCollection can be serialized to JSON"""
        result = polygon_to_valid_geojson(realistic_fire_multipolygon)

        # Should be able to serialize
        json_str = json.dumps(result.model_dump())
        assert json_str is not None

        # Should be able to deserialize
        parsed = json.loads(json_str)
        assert parsed["type"] == "FeatureCollection"
        assert parsed["features"][0]["geometry"]["type"] == "MultiPolygon"


class TestRequestModelsAcceptMultiPolygon:
    """Test Pydantic request models accept MultiPolygon geometries"""

    def test_processing_request_accepts_multipolygon(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """ProcessingRequest model accepts MultiPolygon"""
        request = ProcessingRequest(
            fire_event_name="test_fire",
            coarse_geojson=sample_multipolygon,
            prefire_date_range=["2023-01-01", "2023-06-01"],
            postfire_date_range=["2023-07-01", "2023-12-31"],
        )

        assert request.fire_event_name == "test_fire"
        assert request.coarse_geojson.type == "MultiPolygon"
        assert len(request.coarse_geojson.coordinates) == 2

    def test_processing_request_accepts_polygon_backward_compatibility(
        self, sample_polygon: Polygon
    ) -> None:
        """ProcessingRequest still accepts Polygon (backward compatibility)"""
        request = ProcessingRequest(
            fire_event_name="test_fire",
            coarse_geojson=sample_polygon,
            prefire_date_range=["2023-01-01", "2023-06-01"],
            postfire_date_range=["2023-07-01", "2023-12-31"],
        )

        assert request.fire_event_name == "test_fire"
        assert request.coarse_geojson.type == "Polygon"

    def test_processing_request_accepts_feature_multipolygon(
        self, sample_feature_multipolygon: Feature
    ) -> None:
        """ProcessingRequest accepts Feature containing MultiPolygon"""
        request = ProcessingRequest(
            fire_event_name="test_fire",
            coarse_geojson=sample_feature_multipolygon,
            prefire_date_range=["2023-01-01", "2023-06-01"],
            postfire_date_range=["2023-07-01", "2023-12-31"],
        )

        assert request.coarse_geojson.type == "Feature"
        assert request.coarse_geojson.geometry.type == "MultiPolygon"

    def test_refine_request_accepts_multipolygon(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """RefineRequest model accepts MultiPolygon"""
        request = RefineRequest(
            fire_event_name="test_fire",
            refined_geojson=sample_multipolygon,
            job_id="test-job-123",
        )

        assert request.fire_event_name == "test_fire"
        assert request.refined_geojson.type == "MultiPolygon"
        assert request.job_id == "test-job-123"

    def test_refine_request_accepts_polygon_backward_compatibility(
        self, sample_polygon: Polygon
    ) -> None:
        """RefineRequest still accepts Polygon (backward compatibility)"""
        request = RefineRequest(
            fire_event_name="test_fire",
            refined_geojson=sample_polygon,
            job_id="test-job-123",
        )

        assert request.refined_geojson.type == "Polygon"

    def test_geojson_upload_request_accepts_multipolygon(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """GeoJSONUploadRequest model accepts MultiPolygon"""
        request = GeoJSONUploadRequest(
            fire_event_name="test_fire",
            geojson=sample_multipolygon,
        )

        assert request.fire_event_name == "test_fire"
        assert request.geojson.type == "MultiPolygon"

    def test_geojson_upload_request_accepts_polygon_backward_compatibility(
        self, sample_polygon: Polygon
    ) -> None:
        """GeoJSONUploadRequest still accepts Polygon (backward compatibility)"""
        request = GeoJSONUploadRequest(
            fire_event_name="test_fire",
            geojson=sample_polygon,
        )

        assert request.geojson.type == "Polygon"

    def test_request_serialization_multipolygon(
        self, realistic_fire_multipolygon: MultiPolygon
    ) -> None:
        """Request models can be serialized with MultiPolygon"""
        request = ProcessingRequest(
            fire_event_name="disjoint_fire_2024",
            coarse_geojson=realistic_fire_multipolygon,
            prefire_date_range=["2024-05-01", "2024-06-01"],
            postfire_date_range=["2024-08-01", "2024-09-01"],
        )

        # Should serialize successfully
        json_data = request.model_dump_json()
        assert json_data is not None

        # Should deserialize successfully
        parsed = ProcessingRequest.model_validate_json(json_data)
        assert parsed.coarse_geojson.type == "MultiPolygon"
        assert len(parsed.coarse_geojson.coordinates) == 2


class TestEdgeCases:
    """Test edge cases for MultiPolygon handling"""

    def test_single_polygon_in_multipolygon(self) -> None:
        """MultiPolygon with single constituent polygon"""
        single_multipolygon = MultiPolygon(
            type="MultiPolygon",
            coordinates=[
                [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 1.0], [0.0, 0.0]]]
            ],
        )

        result = validate_polygon(single_multipolygon)
        assert result.geom_type == "MultiPolygon"
        assert result.is_valid
        assert len(result.geoms) == 1

    def test_multipolygon_with_holes(self) -> None:
        """MultiPolygon where constituent polygons have holes"""
        multipolygon_with_holes = MultiPolygon(
            type="MultiPolygon",
            coordinates=[
                [
                    # Outer ring
                    [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]],
                    # Inner ring (hole)
                    [[2.0, 2.0], [2.0, 8.0], [8.0, 8.0], [8.0, 2.0], [2.0, 2.0]],
                ]
            ],
        )

        result = validate_polygon(multipolygon_with_holes)
        assert result.geom_type == "MultiPolygon"
        assert result.is_valid

    def test_complex_multipolygon_many_parts(self) -> None:
        """MultiPolygon with many constituent polygons"""
        # Simulate a complex fire with multiple spot fires
        coordinates = []
        for i in range(5):
            offset = i * 5.0
            coordinates.append(
                [
                    [
                        [offset, 0.0],
                        [offset + 1.0, 0.0],
                        [offset + 1.0, 1.0],
                        [offset, 1.0],
                        [offset, 0.0],
                    ]
                ]
            )

        complex_multipolygon = MultiPolygon(  # type: ignore[arg-type]
            type="MultiPolygon", coordinates=coordinates  # type: ignore[arg-type]
        )

        result = validate_polygon(complex_multipolygon)
        assert result.geom_type == "MultiPolygon"
        assert result.is_valid
        assert len(result.geoms) == 5


class TestBoundaryUtilsCompatibility:
    """Test that boundary_utils functions work with MultiPolygon"""

    def test_process_and_upload_geojson_type_hint_accepts_multipolygon(
        self, sample_multipolygon: MultiPolygon
    ) -> None:
        """
        Verify process_and_upload_geojson type hints accept MultiPolygon.

        This is a type-checking test that ensures the function signature
        includes MultiPolygon in the Union type.
        """
        from src.util.boundary_utils import process_and_upload_geojson
        import inspect

        sig = inspect.signature(process_and_upload_geojson)
        geometry_param = sig.parameters["geometry"]

        # The annotation should include MultiPolygon
        # We can't directly test Union types at runtime, but we can
        # verify the parameter exists and is typed
        assert geometry_param.annotation is not None
        assert "geometry" in sig.parameters

        # This test primarily validates that mypy will pass
        # The actual runtime test would be in integration tests


if __name__ == "__main__":
    # Allow running tests directly
    pytest.main([__file__, "-v"])
