import pyarrow.flight as flight
import pyarrow as pa

# Connect to Quiver
client = flight.FlightClient("grpc://localhost:8815")

# 1. Define the FeatureRequest (proto-encoded Ticket)
# In a real scenario, you'd use the generated proto.
# For a quick test, we can use a raw ticket if we know the encoding.

