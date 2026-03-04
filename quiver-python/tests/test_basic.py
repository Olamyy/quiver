"""Basic tests for Quiver Python client."""

import pytest
import sys

sys.path.insert(0, "src")

import quiver


def test_package_imports():
    """Test that package imports work correctly."""
    assert hasattr(quiver, "__version__")
    assert hasattr(quiver, "Client")
    assert hasattr(quiver, "FeatureTable")
    assert hasattr(quiver, "ValidationError")


def test_client_creation():
    """Test basic client creation and validation."""
    # Valid client creation
    client = quiver.Client("localhost:8815", validate_connection=False)
    assert client is not None
    client.close()

    # Invalid address
    with pytest.raises(quiver.ValidationError, match="address cannot be empty"):
        quiver.Client("")

    # Invalid timeout
    with pytest.raises(quiver.ValidationError, match="timeout must be positive"):
        quiver.Client("localhost:8815", timeout=-1)


def test_feature_request_validation():
    """Test FeatureRequest validation."""
    # Valid request
    request = quiver.FeatureRequest(
        feature_view="user_features",
        entities=["user_1", "user_2"],
        features=["age", "tier"],
    )
    assert request.feature_view == "user_features"

    # Empty feature_view
    with pytest.raises(quiver.ValidationError, match="feature_view cannot be empty"):
        quiver.FeatureRequest("", ["user_1"], ["age"])

    # Empty entities
    with pytest.raises(quiver.ValidationError, match="entities cannot be empty"):
        quiver.FeatureRequest("user_features", [], ["age"])

    # Empty features
    with pytest.raises(quiver.ValidationError, match="features cannot be empty"):
        quiver.FeatureRequest("user_features", ["user_1"], [])


def test_basic_get_features():
    """Test basic get_features functionality."""
    client = quiver.Client("localhost:8815", validate_connection=False)

    # Should return mock data with realistic values
    result = client.get_features("user_features", ["user_1"], ["age"])
    assert isinstance(result, quiver.FeatureTable)
    assert len(result) == 1  # One entity requested
    assert result.column_names == ["entity_id", "age"]

    client.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
