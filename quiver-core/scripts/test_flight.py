import pyarrow.flight as flight

def test_quiver_flight():
    client = flight.connect("grpc://localhost:8815")
    
    # Test GetSchema
    print("Testing GetSchema...")
    try:
        descriptor = flight.FlightDescriptor.for_path("user_features")
        schema = client.get_schema(descriptor)
        print(f"Schema: {schema.schema}")
    except Exception as e:
        print(f"GetSchema failed: {e}")

if __name__ == "__main__":
    test_quiver_flight()
