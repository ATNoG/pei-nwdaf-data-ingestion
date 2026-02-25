import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch, AsyncMock, Mock
import json
import requests


SUBSCRIPTION_ID = "random_id"

@pytest.fixture
def mock_kafka_bridge():
    """Mock the PyKafBridge for testing."""
    mock = MagicMock()
    mock.produce = MagicMock(return_value=True)
    mock.close = AsyncMock()
    return mock


@pytest.fixture
def mock_subscription_registry():
    """Mock the SubscriptionRegistry for testing."""
    mock = MagicMock()
    mock.add = MagicMock()
    mock.remove = MagicMock()
    mock.all_producers = MagicMock(return_value={})
    mock.get_url = MagicMock()
    mock.received_data = MagicMock()
    return mock


@pytest.fixture
def client(mock_kafka_bridge, mock_subscription_registry):
    """Create a test client with mocked Kafka bridge and subscription registry."""
    with patch("receiver.PyKafBridge", return_value=mock_kafka_bridge), \
         patch("receiver.subscription_registry", mock_subscription_registry), \
         patch("receiver.check_producers_thread") as mock_thread:
        mock_thread.start = MagicMock()
        from receiver import app

        with TestClient(app) as test_client:
            yield test_client


class TestReceiveEndpoint:
    """Tests for the /receive endpoint."""

    def test_receive_batch_data_success(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test receiving batch data with analyticsData structure."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        
        payload = {
            "subscription_id" : SUBSCRIPTION_ID,
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
                        "velocity": 5.0,
                    },
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

    def test_receive_batch_data_multiple_entries(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test receiving multiple entries in analyticsData."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        
        payload = {
            "subscription_id": SUBSCRIPTION_ID,
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5,
                        "rsrp": -80,
                        "cell_index": 1,
                    },
                },
                {
                    "timestamp": "2024-01-01T12:00:01Z",
                    "analyticsMetadata": {
                        "datarate": 105.2,
                        "rsrp": -78,
                        "cell_index": 1,
                    },
                },
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert len(data["results"]) == 2
        assert all(result["status"] == "ok" for result in data["results"])

        # Verify Kafka produce was called twice
        assert mock_kafka_bridge.produce.call_count >= 2

    def test_receive_fallback_single_packet(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test fallback case when data doesn't have analyticsData structure."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        
        payload = {
            "subscription_id": SUBSCRIPTION_ID,
            "timestamp": "2024-01-01T12:00:00Z",
            "datarate": 100.5,
            "mean_latency": 20.3,
            "rsrp": -80,
            "cell_index": 1,
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert "data" in data
        assert data["data"]["timestamp"] == "2024-01-01T12:00:00Z"
        assert data["data"]["datarate"] == 100.5

    def test_receive_missing_mandatory_fields(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test that missing cell_index or timestamp returns error."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        
        payload = {
            "subscription_id": SUBSCRIPTION_ID,
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5
                        # cell_index missing
                    },
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["results"][0]["status"] == "error"
        assert "cell_index" in data["results"][0]["message"]

    def test_receive_kafka_produce_failure(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test handling of Kafka produce failure."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        mock_kafka_bridge.produce.return_value = False

        payload = {
            "subscription_id": SUBSCRIPTION_ID,
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {"datarate": 100.5, "cell_index": 1},
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["results"][0]["status"] == "error"
        assert "Failed to send to Kafka" in data["results"][0]["message"]

    def test_receive_kafka_produce_exception(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test handling of Kafka produce exception."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        mock_kafka_bridge.produce.side_effect = Exception("Kafka connection error")

        payload = {
            "subscription_id": SUBSCRIPTION_ID,
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {"datarate": 100.5, "cell_index": 1},
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["results"][0]["status"] == "error"

    def test_receive_no_kafka_bridge(self, client, mock_subscription_registry):
        """Test behavior when Kafka bridge is not available."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        
        with patch("receiver.kafka_bridge", None):
            payload = {
                "subscription_id": SUBSCRIPTION_ID,
                "analyticsData": [
                    {
                        "timestamp": "2024-01-01T12:00:00Z",
                        "analyticsMetadata": {"datarate": 100.5, "cell_index": 1},
                    }
                ]
            }

            response = client.post("/receive", json=payload)

            assert response.status_code == 200
            data = response.json()
            assert data["results"][0]["status"] == "no-kafka"

    def test_receive_empty_analytics_data(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test receiving empty analyticsData array."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        
        payload = {
            "subscription_id": SUBSCRIPTION_ID,
            "analyticsData": []
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["results"] == []

    def test_receive_all_fields_passthrough(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test that all fields are passed through without filtering."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        
        from receiver import REQUIRED_FIELDS

        # Create complete metadata with various fields
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
            "velocity": 5.0,
            "custom_field": "custom_value",  # Extra field
        }

        payload = {
            "subscription_id": SUBSCRIPTION_ID,
            "analyticsData": [
                {"timestamp": "2024-01-01T12:00:00Z", "analyticsMetadata": metadata}
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        result_data = data["results"][0]["data"]

        # Verify required fields are present
        for field in REQUIRED_FIELDS:
            assert field in result_data

        # Verify custom field is also present (passthrough)
        assert "custom_field" in result_data
        assert result_data["custom_field"] == "custom_value"

    def test_kafka_message_format(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test that the Kafka message is properly JSON formatted."""
        mock_subscription_registry.all_producers.return_value = {SUBSCRIPTION_ID: "http://producer:8000"}
        
        from receiver import TOPIC

        payload = {
            "subscription_id": SUBSCRIPTION_ID,
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5,
                        "rsrp": -80,
                        "cell_index": 1,
                    },
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
        assert "cell_index" in parsed_message


class TestSubscriptionEndpoints:
    """Tests for subscription management endpoints."""

    @patch("receiver.requests.post")
    def test_subscribe_to_producer_success(self, mock_requests_post, client, mock_subscription_registry):
        """Test successfully subscribing to a producer."""
        # Mock the response from the producer
        mock_response = Mock()
        mock_response.text = json.dumps({"subscription_id": "test-sub-123"})
        mock_response.status_code = 200
        mock_requests_post.return_value = mock_response

        producer_url = "http://producer-service:8000/subscribe"
        payload = {"producer_url": producer_url}

        response = client.post("/subscriptions", json=payload)

        # Verify the response
        assert response.status_code == 201
        data = response.json()
        assert data["id"] == "test-sub-123"

        # Verify requests.post was called correctly
        from receiver import HOST, PORT
        mock_requests_post.assert_called_once_with(
            producer_url,
            json={"url": f"http://{HOST}:{PORT}/receive"},
            timeout=5
        )

        # Verify subscription was added to registry
        mock_subscription_registry.add.assert_called_once_with("test-sub-123", producer_url)

    @patch("receiver.requests.post")
    def test_subscribe_to_producer_timeout(self, mock_requests_post, client, mock_subscription_registry):
        """Test subscribing when producer times out."""
        mock_requests_post.side_effect = requests.exceptions.Timeout()

        producer_url = "http://producer-service:8000/subscribe"
        payload = {"producer_url": producer_url}

        response = client.post("/subscriptions", json=payload)

        assert response.status_code == 504
        assert "Producer didnt respond" in response.text

        # Verify subscription was not added
        mock_subscription_registry.add.assert_not_called()

    @patch("receiver.requests.post")
    def test_subscribe_to_producer_connection_error(self, mock_requests_post, client, mock_subscription_registry):
        """Test subscribing when cannot connect to producer."""
        mock_requests_post.side_effect = requests.exceptions.ConnectionError()

        producer_url = "http://producer-service:8000/subscribe"
        payload = {"producer_url": producer_url}

        response = client.post("/subscriptions", json=payload)

        assert response.status_code == 502
        data = response.json()
        assert "Cannot connect to producer" in data["detail"]

        # Verify subscription was not added
        mock_subscription_registry.add.assert_not_called()

    @patch("receiver.requests.post")
    def test_subscribe_to_producer_unexpected_response(self, mock_requests_post, client, mock_subscription_registry):
        """Test subscribing when producer gives unexpected response."""
        mock_response = Mock()
        mock_response.text = "invalid json"
        mock_response.status_code = 200
        mock_requests_post.return_value = mock_response

        producer_url = "http://producer-service:8000/subscribe"
        payload = {"producer_url": producer_url}

        response = client.post("/subscriptions", json=payload)

        assert response.status_code == 500
        data = response.json()
        assert "Producer gave unexpected answer" in data["detail"]

        # Verify subscription was not added
        mock_subscription_registry.add.assert_not_called()

    def test_get_subscriptions_empty(self, client, mock_subscription_registry):
        """Test getting subscriptions when none exist."""
        mock_subscription_registry.all_producers.return_value = {}

        response = client.get("/subscriptions")

        assert response.status_code == 200
        data = response.json()
        assert "producers" in data
        assert data["producers"] == []

    def test_get_subscriptions_with_producers(self, client, mock_subscription_registry):
        """Test getting subscriptions with existing producers."""
        mock_subscription_registry.all_producers.return_value = {
            "sub-1": "http://producer1:8000/subscribe",
            "sub-2": "http://producer2:8000/subscribe",
            "sub-3": "http://producer3:8000/subscribe"
        }

        response = client.get("/subscriptions")

        assert response.status_code == 200
        data = response.json()
        assert "producers" in data
        assert len(data["producers"]) == 3
        
        # Verify format
        producer_ids = [list(p.keys())[0] for p in data["producers"]]
        assert "sub-1" in producer_ids
        assert "sub-2" in producer_ids
        assert "sub-3" in producer_ids

    @patch("receiver.requests.delete")
    def test_unsubscribe_success(self, mock_requests_delete, client, mock_subscription_registry):
        """Test successfully unsubscribing from a producer."""
        subscription_id = "test-sub-123"
        producer_url = "http://producer-service:8000/subscribe"
        
        mock_subscription_registry.get_url.return_value = producer_url
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_delete.return_value = mock_response

        response = client.delete(f"/subscriptions/{subscription_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["subscription_id"] == subscription_id

        # Verify requests.delete was called correctly
        mock_requests_delete.assert_called_once_with(f"{producer_url}/{subscription_id}")

        # Verify subscription was removed from registry
        mock_subscription_registry.remove.assert_called_once_with(subscription_id)

    @patch("receiver.requests.delete")
    def test_unsubscribe_with_trailing_slash(self, mock_requests_delete, client, mock_subscription_registry):
        """Test unsubscribing when producer URL has trailing slash."""
        subscription_id = "test-sub-123"
        producer_url = "http://producer-service:8000/subscribe/"
        
        mock_subscription_registry.get_url.return_value = producer_url
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_delete.return_value = mock_response

        response = client.delete(f"/subscriptions/{subscription_id}")

        assert response.status_code == 200

        # Verify trailing slash was removed in the request
        mock_requests_delete.assert_called_once_with(f"http://producer-service:8000/subscribe/{subscription_id}")

    def test_unsubscribe_not_found(self, client, mock_subscription_registry):
        """Test unsubscribing from non-existent subscription."""
        subscription_id = "non-existent-sub"
        mock_subscription_registry.get_url.side_effect = KeyError()

        response = client.delete(f"/subscriptions/{subscription_id}")

        assert response.status_code == 404
        data = response.json()
        assert "Subscription not found" in data["detail"]

    @patch("receiver.requests.delete")
    def test_unsubscribe_producer_error(self, mock_requests_delete, client, mock_subscription_registry):
        """Test unsubscribing when producer returns error."""
        subscription_id = "test-sub-123"
        producer_url = "http://producer-service:8000/subscribe"
        
        mock_subscription_registry.get_url.return_value = producer_url
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Subscription not found on producer"
        mock_requests_delete.return_value = mock_response

        response = client.delete(f"/subscriptions/{subscription_id}")

        assert response.status_code == 404
        data = response.json()
        assert "Subscription not found on producer" in data["detail"]

        # Verify subscription was not removed from registry
        mock_subscription_registry.remove.assert_not_called()


class TestReceiveWithSubscription:
    """Tests for /receive endpoint with subscription validation."""

    def test_receive_data_from_subscribed_producer(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test receiving data from a subscribed producer."""
        subscription_id = "test-sub-123"
        mock_subscription_registry.all_producers.return_value = {subscription_id: "http://producer:8000"}

        payload = {
            "subscription_id": subscription_id,
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5,
                        "cell_index": 1,
                    },
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 200
        data = response.json()
        assert data["results"][0]["status"] == "ok"

        # Verify received_data was called
        mock_subscription_registry.received_data.assert_called_once_with(subscription_id)

    def test_receive_data_from_unsubscribed_producer(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test receiving data from an unsubscribed producer."""
        subscription_id = "unsubscribed-id"
        mock_subscription_registry.all_producers.return_value = {}

        payload = {
            "subscription_id": subscription_id,
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5,
                        "cell_index": 1,
                    },
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 403
        assert "Not subscribed" in response.text

        # Verify received_data was still called (before checking subscription)
        mock_subscription_registry.received_data.assert_called_once_with(subscription_id)

    def test_receive_data_without_subscription_id(self, client, mock_kafka_bridge, mock_subscription_registry):
        """Test receiving data without subscription_id field."""
        payload = {
            "analyticsData": [
                {
                    "timestamp": "2024-01-01T12:00:00Z",
                    "analyticsMetadata": {
                        "datarate": 100.5,
                        "cell_index": 1,
                    },
                }
            ]
        }

        response = client.post("/receive", json=payload)

        assert response.status_code == 400
        assert "Bad request" in response.text

        # Verify received_data was not called
        mock_subscription_registry.received_data.assert_not_called()
