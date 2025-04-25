import pytest
import time
import uuid
from unittest.mock import patch, MagicMock

def test_root_endpoint(client):
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welcome to the Fire Recovery Backend API"}

def test_process_data(client, valid_request_body):
    with patch('src.app.process_remote_sensing_data') as mock_process:
        response = client.post("/process/", json=valid_request_body)
        
        assert response.status_code == 200
        assert "job_id" in response.json()
        assert response.json()["status"] == "Processing started"
        
        # Verify background task was called with correct parameters
        mock_process.assert_called_once()

def test_result_test_endpoint_pending(client, valid_request_body):
    # First get a job ID by calling the process-test endpoint
    response = client.post("/process-test/", json=valid_request_body)
    job_id = response.json()["job_id"]
    
    # Ensure job timestamp is set
    with patch.dict('src.app.job_timestamps', {job_id: time.time()}):
        # Test immediate response (should be pending)
        response = client.get(f"/result-test/{job_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "pending"
