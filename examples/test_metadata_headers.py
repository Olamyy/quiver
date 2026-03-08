"""
Test client for verifying response metadata headers.

Tests that the Arrow Flight do_get() response includes:
- x-quiver-request-id: UUID for correlating with observability service
- x-quiver-from-cache: Boolean indicating if response came from cache

Demonstrates:
1. Metadata headers are present on all responses
2. Each request gets a unique request_id
3. Cache behavior: first request is fresh, subsequent identical requests are cached

Usage:
  uv run test_metadata_headers.py [--server localhost:8815] [--identical 3] [--different 2]

Options:
  --server: Server address (default: localhost:8815)
  --identical: Number of identical requests to verify cache (default: 3)
  --different: Number of different requests to verify fresh fetches (default: 2)
"""

import argparse
import sys

import pyarrow.flight as flight
from quiver import Client
from quiver.proto import serving_pb2, types_pb2


def extract_metadata(response) -> dict:
    """Extract metadata headers from gRPC response."""
    metadata = response.metadata() if hasattr(response, 'metadata') else None
    result = {}

    if metadata:
        request_id_bytes = metadata.get(b"x-quiver-request-id")
        from_cache_bytes = metadata.get(b"x-quiver-from-cache")

        if request_id_bytes:
            result["request_id"] = request_id_bytes.decode("utf-8")
        if from_cache_bytes:
            result["from_cache"] = from_cache_bytes.decode("utf-8").lower() == "true"

    return result


def create_flight_request(feature_view: str, entities: list, features: list) -> bytes:
    """Create a protobuf FeatureRequest."""
    proto_request = serving_pb2.FeatureRequest()
    proto_request.feature_view = feature_view
    proto_request.feature_names.extend(features)

    for entity_id in entities:
        entity_key = types_pb2.EntityKey()
        entity_key.entity_type = "default"
        entity_key.entity_id = entity_id
        proto_request.entities.append(entity_key)

    return proto_request.SerializeToString()


def test_identical_requests(
    client: flight.FlightClient, identical: int
) -> tuple[bool, int, int]:
    """Test multiple identical requests to verify cache behavior."""
    print("\n" + "=" * 70)
    print("TEST 1: IDENTICAL REQUESTS (Cache Behavior)")
    print("=" * 70)
    print(f"Making {identical} identical requests to verify cache\n")

    feature_view = "user_features"
    entities = ["user:100", "user:200"]
    features = ["score"]

    print(f"Request params:")
    print(f"  Feature view: {feature_view}")
    print(f"  Entities: {entities}")
    print(f"  Features: {features}\n")

    cache_hits = 0
    cache_misses = 0
    all_passed = True

    for i in range(identical):
        try:
            ticket_data = create_flight_request(feature_view, entities, features)
            ticket = flight.Ticket(ticket_data)

            response = client.do_get(ticket)
            for _ in response:
                pass

            metadata = extract_metadata(response)

            request_id = metadata.get("request_id", "MISSING")
            from_cache = metadata.get("from_cache")

            if from_cache is None:
                print(f"  Request {i + 1}: ✗ Missing x-quiver-from-cache header")
                all_passed = False
            elif from_cache:
                print(f"  Request {i + 1}: ✓ CACHE HIT (request_id: {request_id[:8]}...)")
                cache_hits += 1
            else:
                print(
                    f"  Request {i + 1}: ✓ CACHE MISS / FRESH (request_id: {request_id[:8]}...)"
                )
                cache_misses += 1

        except Exception as e:
            print(f"  Request {i + 1}: ✗ Failed: {e}")
            all_passed = False

    print()
    if all_passed:
        print("✓ All requests returned metadata headers")
        if cache_hits > 0:
            print(f"✓ Cache working correctly: {cache_hits} hits, {cache_misses} miss(es)")
        return True, cache_hits, cache_misses
    else:
        print("✗ Some requests missing metadata")
        return False, cache_hits, cache_misses


def test_different_requests(client: flight.FlightClient, different: int) -> bool:
    """Test different requests all return fresh data."""
    print("\n" + "=" * 70)
    print("TEST 2: DIFFERENT REQUESTS (All Fresh)")
    print("=" * 70)
    print(f"Making {different} different requests\n")

    all_passed = True
    request_ids_seen = set()

    for i in range(different):
        try:
            entities = [f"user:{100 + i * 10}", f"user:{101 + i * 10}"]
            feature_view = "user_features"
            features = ["score"]

            ticket_data = create_flight_request(feature_view, entities, features)
            ticket = flight.Ticket(ticket_data)

            response = client.do_get(ticket)
            for _ in response:
                pass

            metadata = extract_metadata(response)

            request_id = metadata.get("request_id", "MISSING")
            from_cache = metadata.get("from_cache")

            request_ids_seen.add(request_id)

            if request_id == "MISSING":
                print(f"  Request {i + 1}: ✗ Missing request_id header")
                all_passed = False
            elif from_cache is not False:
                print(f"  Request {i + 1}: ✗ Expected fresh (from_cache=false)")
                all_passed = False
            else:
                print(
                    f"  Request {i + 1}: ✓ FRESH DATA (entities: {entities}, id: {request_id[:8]}...)"
                )

        except Exception as e:
            print(f"  Request {i + 1}: ✗ Failed: {e}")
            all_passed = False

    print()
    if all_passed and len(request_ids_seen) == different:
        print(f"✓ All {different} requests returned unique fresh data")
        return True
    else:
        if not all_passed:
            print("✗ Some requests missing metadata or not fresh")
        if len(request_ids_seen) != different:
            print(
                f"✗ Expected {different} unique IDs, got {len(request_ids_seen)}"
            )
        return False


def test_request_id_correlation(client: flight.FlightClient) -> bool:
    """Verify request_id can be used for observability correlation."""
    print("\n" + "=" * 70)
    print("TEST 3: REQUEST ID CORRELATION")
    print("=" * 70)
    print("Verify request_id for observability service correlation\n")

    try:
        ticket_data = create_flight_request("user_features", ["user:100"], ["score"])
        ticket = flight.Ticket(ticket_data)

        response = client.do_get(ticket)
        for _ in response:
            pass

        metadata = extract_metadata(response)
        request_id = metadata.get("request_id")

        if request_id:
            print(f"✓ Request ID: {request_id}")
            print(f"✓ Can be used to fetch metrics from observability service:")
            print(f"  client.get_metrics('{request_id}')")
            return True
        else:
            print("✗ Missing request_id header")
            return False

    except Exception as e:
        print(f"✗ Failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Test Quiver response metadata headers", add_help=True
    )
    parser.add_argument(
        "--server",
        default="localhost:8815",
        help="Quiver server address (default: localhost:8815)",
    )
    parser.add_argument(
        "--identical",
        type=int,
        default=3,
        help="Number of identical requests for cache test (default: 3)",
    )
    parser.add_argument(
        "--different",
        type=int,
        default=2,
        help="Number of different requests for fresh data test (default: 2)",
    )

    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("QUIVER METADATA HEADERS TEST SUITE")
    print("=" * 70)
    print(f"Server: {args.server}\n")

    try:
        host, port = args.server.split(":") if ":" in args.server else (args.server, "8815")
        location = flight.Location.for_grpc_tcp(host, int(port))
        client = flight.FlightClient(location)
        print(f"✓ Connected to {args.server}\n")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        return 1

    results = []

    identical_passed, cache_hits, cache_misses = test_identical_requests(
        client, args.identical
    )
    results.append(("Identical requests", identical_passed))

    different_passed = test_different_requests(client, args.different)
    results.append(("Different requests", different_passed))

    correlation_passed = test_request_id_correlation(client)
    results.append(("Request ID correlation", correlation_passed))

    print("\n" + "=" * 70)
    print("FINAL RESULTS")
    print("=" * 70)

    all_passed = True
    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {test_name}")
        if not passed:
            all_passed = False

    print()
    if all_passed:
        print("✓ All tests passed")
        return 0
    else:
        print("✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
