from quiver import Client
import pyarrow as pa

# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

client = Client("localhost:8815")

# Test with just one entity and feature
result = client.get_features("session_features", ["session:101"], ["active_user_id"])

print("Result:")
print(result._table)
