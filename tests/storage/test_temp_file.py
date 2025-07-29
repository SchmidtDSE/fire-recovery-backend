import pytest
from src.config.storage import temp_file, get_temp_storage


class TestTempFile:
    @pytest.mark.asyncio
    async def test_temp_file_without_content(self):
        """Test temp_file context manager without initial content"""
        temp_storage = get_temp_storage()

        # Use the context manager
        async with temp_file(suffix=".txt") as path:
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
    async def test_temp_file_with_content(self):
        """Test temp_file context manager with initial content"""
        temp_storage = get_temp_storage()
        initial_content = b"initial content"

        # Use the context manager with content
        async with temp_file(suffix=".bin", content=initial_content) as path:
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
