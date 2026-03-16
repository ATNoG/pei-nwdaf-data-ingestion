"""Pytest configuration and fixtures."""
import sys
from unittest.mock import MagicMock


# Mock the utils.kmw module before importing receiver
class MockPyKafBridge:
    """Mock PyKafBridge for testing."""
    def __init__(self, topic, hostname=None, port=None):
        self.topic = topic
        self.hostname = hostname
        self.port = port
        self._closed = False

    def produce(self, topic, message):
        """Mock produce method."""
        return True

    async def close(self):
        """Mock close method."""
        self._closed = True


# Create a mock module
mock_kmw = MagicMock()
mock_kmw.PyKafBridge = MockPyKafBridge

# Inject the mock into sys.modules
sys.modules['utils'] = MagicMock()
sys.modules['utils.kmw'] = mock_kmw

# Mock policy_client so tests never make real network calls
class _AllowedResult:
    allowed = True
    reason = None

    def __init__(self, data):
        self.data = data

class MockPolicyClient:
    def __init__(self, *args, **kwargs):
        pass

    async def register_component(self, *args, **kwargs):
        pass

    async def start_heartbeat(self):
        pass

    async def stop_heartbeat(self):
        pass

    async def process_data(self, *args, data=None, **kwargs):
        return _AllowedResult(data)

mock_policy_module = MagicMock()
mock_policy_module.PolicyClient = MockPolicyClient
sys.modules['policy_client'] = mock_policy_module
