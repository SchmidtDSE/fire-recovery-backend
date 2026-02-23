"""
Job result persistence utilities.

Provides functions for persisting and retrieving CommandResult objects
to/from storage, enabling job failure reporting via result endpoints.
"""

import logging
from typing import Optional

from src.commands.interfaces.command_result import CommandResult
from src.core.storage.storage_factory import StorageFactory


logger = logging.getLogger(__name__)

# Path template for job result files
JOB_RESULT_PATH_TEMPLATE = "assets/{job_id}/job_result.json"


def get_job_result_path(job_id: str) -> str:
    """
    Get the storage path for a job result file.

    Args:
        job_id: Unique job identifier

    Returns:
        Storage path for the job result JSON file
    """
    return JOB_RESULT_PATH_TEMPLATE.format(job_id=job_id)


async def persist_job_result(
    result: CommandResult,
    storage_factory: StorageFactory,
) -> str:
    """
    Persist a CommandResult to storage.

    Stores the job result as a JSON file at assets/{job_id}/job_result.json.
    This file can be retrieved later to determine job status and error details.

    Args:
        result: CommandResult to persist
        storage_factory: StorageFactory for accessing final storage

    Returns:
        URL of the persisted job result file
    """
    storage = storage_factory.get_final_storage()
    path = get_job_result_path(result.job_id)

    result_dict = result.to_dict()
    url = await storage.save_json(result_dict, path)

    logger.info(
        f"Persisted job result for job_id={result.job_id}, "
        f"status={result.status.value}, path={path}"
    )

    return url


async def get_job_result(
    job_id: str,
    storage_factory: StorageFactory,
) -> Optional[CommandResult]:
    """
    Retrieve a CommandResult from storage.

    Looks for a job result file at assets/{job_id}/job_result.json.
    Returns None if the file does not exist (job still processing).

    Args:
        job_id: Unique job identifier
        storage_factory: StorageFactory for accessing final storage

    Returns:
        CommandResult if found, None if job is still processing
    """
    storage = storage_factory.get_final_storage()
    path = get_job_result_path(job_id)

    try:
        result_dict = await storage.get_json(path)
        result = CommandResult.from_dict(result_dict)
        logger.debug(
            f"Retrieved job result for job_id={job_id}, status={result.status.value}"
        )
        return result
    except Exception as e:
        # File not found or other error - job is still processing
        logger.debug(f"Job result not found for job_id={job_id}: {e}")
        return None
