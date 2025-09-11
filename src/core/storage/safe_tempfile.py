from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
import uuid
import os

from src.core.storage.interface import StorageInterface


@asynccontextmanager
async def safe_tempfile(
    storage: StorageInterface,
    suffix: str = "", 
    content: Optional[bytes] = None
) -> AsyncGenerator[str, None]:
    """Context manager for temporary files using storage provider"""
    path = None
    try:
        # Generate a unique path
        file_id = str(uuid.uuid4())
        path = f"{file_id}{suffix}"

        # Store content if provided
        if content:
            await storage.save_bytes(content, path, temporary=True)

        yield path
    finally:
        # Clean up local files that may have been created
        if path and os.path.exists(path):
            try:
                os.unlink(path)
            except OSError:
                # If file doesn't exist or can't be deleted, ignore
                pass
