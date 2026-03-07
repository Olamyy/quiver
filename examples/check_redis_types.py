import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

key = "sessions:session:101"
print(f"Key: {key}")
print(f"Type: {r.type(key)}")
print(f"Value (HGETALL): {r.hgetall(key)}")
print(f"Value (GET): {r.get(key)}")
