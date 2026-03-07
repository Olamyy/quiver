import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
session_keys = r.keys("sessions:*")
print("Session entities in Redis:")
for key in sorted(session_keys):
    entity_part = key.split(":", 1)[1]  # Get part after "sessions:"
    print(f"  {entity_part}")
