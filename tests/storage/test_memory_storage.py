import pytest
from src.core.storage.memory import MemoryStorage


class TestMemoryStorage:
    @pytest.mark.asyncio
    async def test_save_and_get_bytes(self, memory_storage):
        """Test saving and retrieving binary data"""
        test_data = b"Hello, world!"
        test_path = "test_file.bin"

        # Save data
        url = await memory_storage.save_bytes(test_data, test_path)

        # Verify URL format
        assert url == f"memory://test/{test_path}"

        # Retrieve data
        retrieved_data = await memory_storage.get_bytes(test_path)
        assert retrieved_data == test_data

    @pytest.mark.asyncio
    async def test_save_and_get_json(self, memory_storage):
        """Test saving and retrieving JSON data"""
        test_data = {"key": "value", "nested": {"key2": 42}}
        test_path = "test_file.json"

        # Save data
        url = await memory_storage.save_json(test_data, test_path)

        # Verify URL format
        assert url == f"memory://test/{test_path}"

        # Retrieve data
        retrieved_data = await memory_storage.get_json(test_path)
        assert retrieved_data == test_data

    @pytest.mark.asyncio
    async def test_list_files(self, memory_storage):
        """Test listing files with prefix"""
        # Save multiple files
        await memory_storage.save_bytes(b"data1", "prefix/file1.bin")
        await memory_storage.save_bytes(b"data2", "prefix/file2.bin")
        await memory_storage.save_bytes(b"data3", "other/file3.bin")

        # List files with prefix
        files = await memory_storage.list_files("prefix/")
        assert len(files) == 2
        assert "prefix/file1.bin" in files
        assert "prefix/file2.bin" in files
        assert "other/file3.bin" not in files

    @pytest.mark.asyncio
    async def test_cleanup(self, memory_storage):
        """Test cleanup of temporary files"""
        # Save temporary and permanent files
        await memory_storage.save_bytes(b"temp data", "temp_file.bin", temporary=True)
        await memory_storage.save_bytes(b"perm data", "perm_file.bin", temporary=False)

        # Verify both files exist
        files = await memory_storage.list_files("")
        assert len(files) == 2
        assert "temp_file.bin" in files
        assert "perm_file.bin" in files

        # Clean up temporary files
        removed = await memory_storage.cleanup()
        assert removed == 1

        # Verify only permanent file remains
        files = await memory_storage.list_files("")
        assert len(files) == 1
        assert "perm_file.bin" in files

    @pytest.mark.asyncio
    async def test_copy_from_url(self, memory_storage):
        """Test copying from URL to storage"""
        # Use a real URL for testing
        url = "https://raw.githubusercontent.com/pytorch/pytorch/main/README.md"
        target_path = "downloaded_readme.md"

        result_url = await memory_storage.copy_from_url(url, target_path)

        # Verify the file was saved
        assert result_url == f"memory://test/{target_path}"
        retrieved_data = await memory_storage.get_bytes(target_path)
        assert len(retrieved_data) > 0  # Should have content
        assert b"PyTorch" in retrieved_data  # Should contain this text
