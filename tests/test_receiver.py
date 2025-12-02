import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch, AsyncMock
import json


@pytest.fixture
def mock_kafka_bridge():
    """Mock the PyKafBridge for testing."""
    mock = MagicMock()
    mock.produce = MagicMock(return_value=True)
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def client(mock_kafka_bridge):
    """Create a test client with mocked Kafka bridge."""
    with patch("receiver.PyKafBridge", return_value=mock_kafka_bridge):
        from receiver import app
        with TestClient(app) as test_client:
            yield test_client


class TestReceiveEndpoint:
    """Tests for the /receive endpoint."""

    def test_receive_batch_data_success(self, client, mock_kafka_bridge):
        """Test receiving batch data with analyticsData structure."""
        payload = {
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5,
                        "mean_latency": 20.3,
                        "rsrp": -80,
                        "sinr": 15.5,
                        "rsrq": -10,
                        "direction": "downlink",
                        "network": "5G",
                        "cqi": 12,
                        "cell_index": 1,
                        "primary_bandwidth": 100,
                        "ul_bandwidth": 50,
                        "latitude": 40.7128,
                        "longitude": -74.0060,
                        "altitude": 10.5,
                        "velocity": 5.0
                    }
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert len(data["results"]) == 1
        assert data["results"][0]["status"] == "ok"
        assert data["results"][0]["data"]["timestamp"] == "2024-01-01T12:00:00Z"
        assert data["results"][0]["data"]["datarate"] == 100.5

        # Verify Kafka produce was called
        mock_kafka_bridge.produce.assert_called()

    def test_receive_batch_data_multiple_entries(self, client, mock_kafka_bridge):
        """Test receiving multiple entries in analyticsData."""
        payload = {
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {"datarate": 100.5, "rsrp": -80}
                },
                {
                    "timestamp": "2024-01-01T12:00:01Z",
                    "analyticsMetadata": {"datarate": 105.2, "rsrp": -78}
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2
        assert all(result["status"] == "ok" for result in data["results"])

        # Verify Kafka produce was called twice
        assert mock_kafka_bridge.produce.call_count >= 2

    def test_receive_fallback_single_packet(self, client, mock_kafka_bridge):
        """Test fallback case when data doesn't have analyticsData structure."""
        payload = {
            "timestamp": "2024-01-01T12:00:00Z",
            "datarate": 100.5,
            "mean_latency": 20.3,
            "rsrp": -80
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "data" in data
        assert data["data"]["timestamp"] == "2024-01-01T12:00:00Z"
        assert data["data"]["datarate"] == 100.5

    def test_receive_missing_fields_set_to_none(self, client, mock_kafka_bridge):
        """Test that missing required fields are set to None."""
        payload = {
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5
                        # All other fields missing
                    }
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        result_data = data["results"][0]["data"]

        # Check that missing fields are None
        assert result_data["mean_latency"] is None
        assert result_data["rsrp"] is None
        assert result_data["latitude"] is None

    def test_receive_kafka_produce_failure(self, client, mock_kafka_bridge):
        """Test handling of Kafka produce failure."""
        mock_kafka_bridge.produce.return_value = False

        payload = {
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {"datarate": 100.5}
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["results"][0]["status"] == "error"
        assert "Failed to send to Kafka" in data["results"][0]["message"]

    def test_receive_kafka_produce_exception(self, client, mock_kafka_bridge):
        """Test handling of Kafka produce exception."""
        mock_kafka_bridge.produce.side_effect = Exception("Kafka connection error")

        payload = {
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {"datarate": 100.5}
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["results"][0]["status"] == "error"

    def test_receive_no_kafka_bridge(self, client):
        """Test behavior when Kafka bridge is not available."""
        with patch("receiver.kafka_bridge", None):
            payload = {
                "analyticsData": [
                    {
                        "timestamp": "2024-01-01T12:00:00Z",
                        "analyticsMetadata": {"datarate": 100.5}
                    }
                ]
            }

            response = client.post("/receive", json=payload)

            assert response.status_code == 200
            data = response.json()
            assert data["results"][0]["status"] == "no-kafka"

    def test_receive_empty_analytics_data(self, client, mock_kafka_bridge):
        """Test receiving empty analyticsData array."""
        payload = {"analyticsData": []}

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["results"] == []

    def test_receive_all_required_fields_extracted(self, client, mock_kafka_bridge):
        """Test that all required fields are extracted correctly."""
        from receiver import REQUIRED_FIELDS

        # Create complete metadata with all required fields
        metadata = {
            "datarate": 100.5,
            "mean_latency": 20.3,
            "rsrp": -80,
            "sinr": 15.5,
            "rsrq": -10,
            "direction": "downlink",
            "network": "5G",
            "cqi": 12,
            "cell_index": 1,
            "primary_bandwidth": 100,
            "ul_bandwidth": 50,
            "latitude": 40.7128,
            "longitude": -74.0060,
            "altitude": 10.5,
            "velocity": 5.0
        }

        payload = {
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": metadata
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        result_data = data["results"][0]["data"]

        # Verify all required fields are present
        for field in REQUIRED_FIELDS:
            assert field in result_data

    def test_kafka_message_format(self, client, mock_kafka_bridge):
        """Test that the Kafka message is properly JSON formatted."""
        from receiver import TOPIC

        payload = {
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5,
                        "rsrp": -80
                    }
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200

        # Verify the message sent to Kafka is valid JSON
        call_args = mock_kafka_bridge.produce.call_args
        topic_arg, message_arg = call_args[0]

        assert topic_arg == TOPIC
        # Verify it's valid JSON
        parsed_message = json.loads(message_arg)
        assert "timestamp" in parsed_message
        assert "datarate" in parsed_message
