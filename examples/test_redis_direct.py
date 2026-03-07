import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Check what's in Redis for session:101
key = "sessions:session:101"
data = r.hgetall(key)
print(f"Direct query for {key}:")
print(f"  {data}")

# Try quiver client
from quiver import Client

client = Client("localhost:8815")
result = client.get_features("session_features", ["session:101"], ["active_user_id"])

print("\nQuiver client result:")
print(result.to_pandas())

print("\nRaw Arrow:")
print(result._table)
