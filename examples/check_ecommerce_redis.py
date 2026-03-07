import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

# Check ecommerce data
keys = r.keys("ecommerce:user:*")
if keys:
    key = keys[0]
    print(f"Key: {key}")
    print(f"Type: {r.type(key)}")
    print(f"Value: {r.hgetall(key)}")
