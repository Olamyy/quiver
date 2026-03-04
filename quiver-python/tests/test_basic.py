"""Basic tests for Quiver Python client."""

import pytest

from quiver import Client, FeatureTable
from quiver.exceptions import QuiverValidationError
from quiver.models import FeatureRequest


def test_client_creation():
    """Test basic client creation and validation."""
    client = Client("localhost:8815", validate_connection=False)
    assert client is not None
    client.close()

    # Test various address formats work
    valid_addresses = ["localhost:8815", "grpc://localhost:8815", "localhost"]
    for address in valid_addresses:
        client = Client(address, validate_connection=False)
        assert client is not None
        client.close()


def test_feature_request_validation():
    """Test FeatureRequest validation."""
    request = FeatureRequest(
        feature_view="user_features",
        entities=["user_1", "user_2"],
        features=["age", "tier"],
    )
    assert request.feature_view == "user_features"

    with pytest.raises(QuiverValidationError, match="feature_view cannot be empty"):
        FeatureRequest("", ["user_1"], ["age"])

    with pytest.raises(QuiverValidationError, match="entities cannot be empty"):
        FeatureRequest("user_features", [], ["age"])

    with pytest.raises(QuiverValidationError, match="features cannot be empty"):
        FeatureRequest("user_features", ["user_1"], [])


def test_basic_get_features():
    """Test basic get_features functionality."""
    client = Client("localhost:8815", validate_connection=False)

    result = client.get_features("user_features", ["user_1"], ["age"])
    assert isinstance(result, FeatureTable)
    assert len(result) == 1
    assert result.column_names == ["entity_id", "age"]

    client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
