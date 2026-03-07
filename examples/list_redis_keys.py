import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)
keys = r.keys("sessions:*")
print(f"Keys matching 'sessions:*': {len(keys)}")
for key in keys:
    print(f"  {key}: {r.hgetall(key)}")
