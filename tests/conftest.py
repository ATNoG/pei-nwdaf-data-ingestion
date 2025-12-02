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
