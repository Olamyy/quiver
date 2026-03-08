"""
Quiver end-to-end client example.

Demonstrates querying features from any adapter by reading available features
from server configuration and querying with standard test entities.

Usage:
  uv run read.py [--server localhost:8815]
"""

import argparse
import json
from pathlib import Path
from quiver import Client


def load_fixtures():
    """Load fixture definitions to get standard test entities."""
    fixtures_path = Path(__file__).parent / "data" / "fixtures.json"
    with open(fixtures_path) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Quiver end-to-end client example")
    parser.add_argument(
        "--server",
        default="localhost:8815",
        help="Quiver server address (default: localhost:8815)",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=3,
        help="Number of rows to fetch per feature view (default: 3)",
    )

    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("QUIVER CLIENT END-TO-END EXAMPLE")
    print("=" * 70)

    try:
        client = Client(args.server, observability_host="localhost", observability_port=8816)
        print(f"\n✓ Connected to Quiver server at {args.server}")
        print(f"✓ Observability service available at localhost:8816\n")

        print("=" * 70)
        print("SERVER CONFIGURATION")
        print("=" * 70)
        server_info = client.get_server_info()

        if "registry" in server_info and "views" in server_info["registry"]:
            views = server_info["registry"]["views"]
            print(f"\nAvailable feature views: {len(views)}")

            for view in views:
                view_name = view.get("name", "unknown")
                entity_type = view.get("entity_type", "default")
                entity_key = view.get("entity_key", "id")
                columns = view.get("columns", [])

                print(f"\n  View: {view_name}")
                print(f"    Entity type: {entity_type}")
                print(f"    Entity key: {entity_key}")
                print(f"    Features: {len(columns)}")
                for col in columns:
                    col_name = col.get("name", "?")
                    col_type = col.get("arrow_type", "?")
                    col_source = col.get("source", "?")
                    print(f"      - {col_name} ({col_type}) from {col_source}")

        print("\n" + "=" * 70)
        print("FEATURE QUERIES")
        print("=" * 70)

        fixtures = load_fixtures()

        if "registry" in server_info and "views" in server_info["registry"]:
            for view in server_info["registry"]["views"]:
                view_name = view.get("name", "")
                entity_type = view.get("entity_type", "")
                columns = view.get("columns", [])

                if not columns:
                    print(f"\n  View {view_name}: no features to query")
                    continue

                feature_names = [col["name"] for col in columns]

                scenario = None
                for fixture_name, fixture_def in fixtures.items():
                    if fixture_def.get("entity_type") == entity_type:
                        scenario = fixture_def
                        break

                sample_entities = []
                if scenario:
                    fixture_entities = [record["entity"] for record in scenario["records"]]
                    sample_entities.extend(fixture_entities[:args.rows])

                # Generate additional entities if needed
                if len(sample_entities) < args.rows:
                    base_id = 1000 + len(fixture_entities) if scenario else 1000
                    for i in range(len(sample_entities), args.rows):
                        sample_entities.append(f"{entity_type}:{base_id + i - len(fixture_entities)}")

                if not sample_entities:
                    sample_entities = [
                        f"{entity_type}:{1000 + i}"
                        for i in range(args.rows)
                    ]

                print(f"\n  View: {view_name}")
                print(f"    Querying {len(sample_entities)} entities for {len(feature_names)} features")

                try:
                    result = client.get_features(
                        view_name,
                        sample_entities,
                        feature_names,
                    )

                    print(f"    Retrieved {len(result)} rows")

                    request_id = client.get_last_request_id()
                    if request_id:
                        try:
                            metrics = client.get_metrics(request_id)
                            print(f"    Request ID: {request_id}")
                            print(f"      Latency Breakdown (11 Instrumentation Points):")
                            print(f"      Registry lookup:    {metrics['registry_lookup_ms']:7.2f}ms")
                            print(f"      Cache lookup:       {metrics['cache_lookup_ms']:7.2f}ms")
                            print(f"      Partition:          {metrics['partition_ms']:7.2f}ms")
                            print(f"      Dispatch:           {metrics['dispatch_ms']:7.2f}ms")
                            print(f"      Backend Redis:      {metrics['backend_redis_ms']:7.2f}ms")
                            print(f"      Backend PostgreSQL: {metrics['backend_postgres_ms']:7.2f}ms")
                            print(f"      Backend Max:        {metrics['backend_max_ms']:7.2f}ms")
                            print(f"      Alignment:          {metrics['alignment_ms']:7.2f}ms")
                            print(f"      Merge:              {metrics['merge_ms']:7.2f}ms")
                            print(f"      Validation:         {metrics['validation_ms']:7.2f}ms")
                            print(f"      Serialization:      {metrics['serialization_ms']:7.2f}ms")
                            print(f"      " + "-" * 45)
                            print(f"      TOTAL:              {metrics['total_ms']:7.2f}ms")
                        except Exception as e:
                            print(f"    Metrics unavailable: {type(e).__name__}")
                    else:
                        print(f"    Request ID not available")

                    print()
                    df = result.to_pandas()
                    print("    Data:")
                    for line in str(df).split("\n"):
                        print(f"      {line}")

                except Exception as e:
                    print(f"    Error: {type(e).__name__}: {e}\n")

        if "adapters" in server_info:
            print("\n" + "=" * 70)
            print("ADAPTER CONFIGURATION")
            print("=" * 70)
            adapters = server_info["adapters"]
            for adapter_name, adapter_config in adapters.items():
                print(f"\n  {adapter_name}:")
                for key, value in adapter_config.items():
                    if key not in ["connection_string", "auth", "password"]:
                        print(f"    {key}: {value}")

    except Exception as e:
        print(f"\n✗ Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    print("\n" + "=" * 70 + "\n")
    return 0


if __name__ == "__main__":
    exit(main())
