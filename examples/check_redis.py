import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Get all keys
keys = r.keys("*")
print(f"Total keys in Redis: {len(keys)}")
print("\nFirst 10 keys:")
for key in keys[:10]:
    print(f"  {key}")

# Check session keys specifically
session_keys = r.keys("sessions:*")
print(f"\nSession keys: {len(session_keys)}")
if session_keys:
    print(f"First session key: {session_keys[0]}")
    print(f"  Value: {r.hgetall(session_keys[0])}")

# Check for session:101 format
key_101 = "sessions:session:101"
if r.exists(key_101):
    print(f"\nFound {key_101}: {r.hgetall(key_101)}")
else:
    print(f"\n{key_101} not found")
