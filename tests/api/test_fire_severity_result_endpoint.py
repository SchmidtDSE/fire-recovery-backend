"""End of the provenance path: what the severity result endpoint reports.

Which provider served an analysis is decided at run time, so it is recorded on
the STAC item and read back here. These tests cover that last hop, from stored
item to HTTP response.
"""

from typing import Any, Dict, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

SOURCE_DATA = {
    "provider": "Microsoft Planetary Computer",
    "provider_id": "MICROSOFT_PLANETARY_COMPUTER",
    "collection": "sentinel-2-l2a",
    "sensor": "sentinel-2",
    "prefire_scene_count": 2,
    "postfire_scene_count": 7,
}


def severity_item(properties: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": "Geology_Fire-severity-job123",
        "properties": properties,
        "assets": {"rbr": {"href": "https://storage.example.com/rbr.tif"}},
    }


def get_result(item: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Call the severity result endpoint with a stubbed STAC item."""
    from src.app import app
    from src.routers.fire_recovery import get_stac_manager, get_storage_factory

    mock_stac_manager = AsyncMock()
    mock_stac_manager.get_item_by_fire_event_and_id = AsyncMock(return_value=item)

    app.dependency_overrides[get_stac_manager] = lambda: mock_stac_manager
    app.dependency_overrides[get_storage_factory] = lambda: MagicMock()

    try:
        # No persisted job result, so the endpoint falls through to the item.
        with patch(
            "src.routers.fire_recovery.get_job_result", AsyncMock(return_value=None)
        ):
            response = TestClient(app).get(
                "/fire-recovery/result/analyze_fire_severity/Geology_Fire/job123"
            )
        assert response.status_code == 200, response.text
        return response.json()
    finally:
        app.dependency_overrides.clear()


class TestSourceDataInSeverityResult:
    def test_reports_provider_and_scene_counts(self) -> None:
        body = get_result(
            severity_item({"product_type": "fire_severity", "source_data": SOURCE_DATA})
        )

        assert body["status"] == "complete"
        assert body["source_data"] == SOURCE_DATA

    @pytest.mark.parametrize(
        "properties",
        [
            pytest.param({"product_type": "fire_severity"}, id="legacy_item"),
            pytest.param(
                {"source_data": {"provider": "Element 84"}}, id="incomplete_fragment"
            ),
        ],
    )
    def test_serves_items_without_usable_provenance(self, properties) -> None:
        """Analyses predating provenance still return their COGs."""
        body = get_result(severity_item(properties))

        assert body["status"] == "complete"
        assert body["source_data"] is None
        assert body["coarse_severity_cog_urls"] == {
            "rbr": "https://storage.example.com/rbr.tif"
        }
