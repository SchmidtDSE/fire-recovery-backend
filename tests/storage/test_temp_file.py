import pytest
from src.core.storage.safe_tempfile import safe_tempfile
from src.core.storage.storage_factory import StorageFactory


class TestTempFile:
    @pytest.mark.asyncio
    async def test_temp_file_without_content(self) -> None:
        """Test temp_file context manager without initial content"""
        storage_factory = StorageFactory.for_development()
        temp_storage = storage_factory.get_temp_storage()

        # Use the context manager
        async with safe_tempfile(temp_storage, suffix=".txt") as path:
            # Verify the path was generated
            assert path.endswith(".txt")

            # Save some content to the file
            test_content = b"test content"
            await temp_storage.save_bytes(test_content, path, temporary=True)

            # Verify content was saved
            retrieved_content = await temp_storage.get_bytes(path)
            assert retrieved_content == test_content

        # After exiting the context, the file should still exist
        # (since cleanup is handled separately by the storage provider)
        retrieved_content = await temp_storage.get_bytes(path)
        assert retrieved_content == test_content

        # Manually clean up
        await temp_storage.cleanup()

    @pytest.mark.asyncio
    async def test_temp_file_with_content(self) -> None:
        """Test temp_file context manager with initial content"""
        storage_factory = StorageFactory.for_development()
        temp_storage = storage_factory.get_temp_storage()
        initial_content = b"initial content"

        # Use the context manager with content
        async with safe_tempfile(
            temp_storage, suffix=".bin", content=initial_content
        ) as path:
            # Verify the path was generated
            assert path.endswith(".bin")

            # Verify content was saved
            retrieved_content = await temp_storage.get_bytes(path)
            assert retrieved_content == initial_content

            # Update content
            updated_content = b"updated content"
            await temp_storage.save_bytes(updated_content, path, temporary=True)

            # Verify content was updated
            retrieved_content = await temp_storage.get_bytes(path)
            assert retrieved_content == updated_content

        # Manually clean up
        await temp_storage.cleanup()
