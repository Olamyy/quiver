import redis

def seed_data():
    # Connect to local Redis
    r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)

    # Feature view: user_metrics
    # Entities: u101, u102
    # Features: clicks, views
    
    test_data = [
        {"entity_id": "u101", "clicks": 42.0, "views": 105.0},
        {"entity_id": "u102", "clicks": 15.5, "views": 80.0},
    ]

    # Pattern: quiver:f:{feature}:e:{entity}
    for row in test_data:
        entity_id = row["entity_id"]
        for feature, value in row.items():
            if feature == "entity_id":
                continue
            
            key = f"quiver:f:{feature}:e:{entity_id}"
            r.set(key, value)
            print(f"Set {key} -> {value}")

    print("\nRedis Seeding Complete.")

if __name__ == "__main__":
    try:
        seed_data()
    except redis.exceptions.QuiverConnectionError:
        print("Error: Could not connect to Redis. Ensure it is running on 127.0.0.1:6379")
