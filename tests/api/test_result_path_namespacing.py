"""Regression tests for be-result-path.

Done when: a failed veg resolve no longer makes
GET /result/analyze_fire_severity/... report 'failed', and 'success' is never
visible while the result endpoint would say 'pending'.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.commands.interfaces.command_result import CommandResult
from src.routers.fire_recovery import process_fire_severity


@pytest.mark.asyncio
async def test_analyze_persists_only_after_boundary_upload_and_stac_item() -> None:
    """A success result must not be visible before the boundary artifacts exist."""
    call_order: list[str] = []

    success_result = CommandResult.success(
        job_id="job-1",
        fire_event_name="fire-1",
        command_name="fire_severity_analysis",
        execution_time_ms=1.0,
    )

    mock_command = MagicMock()
    mock_command.execute = AsyncMock(return_value=success_result)

    async def fake_upload(**kwargs: object) -> tuple:
        call_order.append("boundary_upload")
        return ("mock://boundary.geojson", None, [0, 0, 1, 1])

    async def fake_create_item(**kwargs: object) -> None:
        call_order.append("stac_item")

    async def fake_persist(result: CommandResult, storage_factory: object) -> str:
        call_order.append("persist")
        return "mock://persisted"

    stac_manager = MagicMock()
    stac_manager.create_boundary_item = AsyncMock(side_effect=fake_create_item)

    storage_factory = MagicMock()
    storage_factory.get_final_storage.return_value = MagicMock()

    with (
        patch(
            "src.routers.fire_recovery.FireSeverityAnalysisCommand",
            return_value=mock_command,
        ),
        patch(
            "src.routers.fire_recovery.process_and_upload_geojson",
            AsyncMock(side_effect=fake_upload),
        ),
        patch(
            "src.routers.fire_recovery.persist_job_result",
            AsyncMock(side_effect=fake_persist),
        ),
    ):
        await process_fire_severity(
            job_id="job-1",
            fire_event_name="fire-1",
            geometry={
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
            prefire_date_range=["2023-01-01", "2023-01-05"],
            postfire_date_range=["2023-02-01", "2023-02-05"],
            stac_manager=stac_manager,
            storage_factory=storage_factory,
            index_registry=MagicMock(),
        )

    # The result is only persisted once the boundary artifacts it depends on exist,
    # so the result endpoint never reports "complete" while it would say "pending".
    assert call_order == ["boundary_upload", "stac_item", "persist"]


@pytest.mark.asyncio
async def test_analyze_failure_persists_immediately_without_boundary_work() -> None:
    """A failed analysis must persist without touching the boundary upload/STAC item."""
    failure_result = CommandResult.failure(
        job_id="job-2",
        fire_event_name="fire-2",
        command_name="fire_severity_analysis",
        execution_time_ms=1.0,
        error_message="no imagery",
    )

    mock_command = MagicMock()
    mock_command.execute = AsyncMock(return_value=failure_result)

    upload_mock = AsyncMock()
    persist_mock = AsyncMock(return_value="mock://persisted")
    stac_manager = MagicMock()
    stac_manager.create_boundary_item = AsyncMock()

    storage_factory = MagicMock()
    storage_factory.get_final_storage.return_value = MagicMock()

    with (
        patch(
            "src.routers.fire_recovery.FireSeverityAnalysisCommand",
            return_value=mock_command,
        ),
        patch("src.routers.fire_recovery.process_and_upload_geojson", upload_mock),
        patch("src.routers.fire_recovery.persist_job_result", persist_mock),
    ):
        await process_fire_severity(
            job_id="job-2",
            fire_event_name="fire-2",
            geometry={
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
            prefire_date_range=["2023-01-01", "2023-01-05"],
            postfire_date_range=["2023-02-01", "2023-02-05"],
            stac_manager=stac_manager,
            storage_factory=storage_factory,
            index_registry=MagicMock(),
        )

    persist_mock.assert_awaited_once_with(failure_result, storage_factory)
    upload_mock.assert_not_awaited()
    stac_manager.create_boundary_item.assert_not_awaited()


class TestResultEndpointsDoNotCrossRead:
    """Analyze, refine and veg each read their own command's result file."""

    @pytest.mark.asyncio
    async def test_failed_veg_resolve_does_not_appear_as_a_failed_analyze_result(
        self,
    ) -> None:
        from fastapi.testclient import TestClient
        from unittest.mock import AsyncMock as AM

        from src.app import app
        from src.routers.fire_recovery import get_stac_manager, get_storage_factory
        from src.util.job_result_persistence import get_job_result_path
        from tests.util.test_job_result_persistence import MockStorage

        storage = MockStorage()

        # A failed veg resolve, sharing the same job_id as the analyze job.
        veg_failure = CommandResult.failure(
            job_id="shared-job-id",
            fire_event_name="Geology_Fire",
            command_name="vegetation_resolve",
            execution_time_ms=1.0,
            error_message="veg resolve blew up",
        )
        storage._json_data[
            get_job_result_path("shared-job-id", "vegetation_resolve")
        ] = veg_failure.to_dict()

        storage_factory = MagicMock()
        storage_factory.get_final_storage.return_value = storage

        mock_stac_manager = AM()
        mock_stac_manager.get_item_by_fire_event_and_id = AM(
            return_value={
                "id": "Geology_Fire-severity-shared-job-id",
                "properties": {},
                "assets": {"rbr": {"href": "https://storage.example.com/rbr.tif"}},
            }
        )

        app.dependency_overrides[get_stac_manager] = lambda: mock_stac_manager
        app.dependency_overrides[get_storage_factory] = lambda: storage_factory
        try:
            response = TestClient(app).get(
                "/fire-recovery/result/analyze_fire_severity/Geology_Fire/shared-job-id"
            )
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 200, response.text
        # The veg failure, stored under its own command_name, must not leak into
        # the analyze result read.
        assert response.json()["status"] == "complete"
