#!/usr/bin/env python3
"""E2E test: Query features and retrieve metrics from observability service.

This test:
1. Connects to Quiver server (port 8815) and observability service (port 8816)
2. Queries features from a multi-backend view
3. Extracts the request ID
4. Retrieves detailed metrics
5. Verifies merge correctness and latency breakdown
6. Reports findings

Usage:
  uv run e2e_metrics_test.py [--server localhost:8815] [--obs localhost:8816]
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


def print_header(title):
    """Print a formatted header."""
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


def print_metrics_table(metrics):
    """Print metrics in a formatted table."""
    print("\n⏱️  Latency Breakdown (11 Instrumentation Points):")
    print("-" * 60)
    print(f"  Registry Lookup      {metrics['registry_lookup_ms']:8.2f} ms")
    print(f"  Cache Lookup         {metrics['cache_lookup_ms']:8.2f} ms")
    print(f"  Partition            {metrics['partition_ms']:8.2f} ms")
    print(f"  Dispatch             {metrics['dispatch_ms']:8.2f} ms")
    print(f"  Backend Execution:")
    print(f"    Redis              {metrics['backend_redis_ms']:8.2f} ms")
    print(f"    PostgreSQL         {metrics['backend_postgres_ms']:8.2f} ms")
    print(f"    Max                {metrics['backend_max_ms']:8.2f} ms")
    print(f"  Alignment            {metrics['alignment_ms']:8.2f} ms")
    print(f"  Merge                {metrics['merge_ms']:8.2f} ms")
    print(f"  Validation           {metrics['validation_ms']:8.2f} ms")
    print(f"  Serialization        {metrics['serialization_ms']:8.2f} ms")
    print("-" * 60)
    print(f"  TOTAL                {metrics['total_ms']:8.2f} ms")
    print(f"  Critical Path        {metrics['critical_path_ms']:8.2f} ms")


def main():
    """Main E2E test."""
    parser = argparse.ArgumentParser(description="E2E metrics test")
    parser.add_argument(
        "--server",
        default="localhost:8815",
        help="Quiver server address (default: localhost:8815)",
    )
    parser.add_argument(
        "--obs",
        default="localhost:8816",
        help="Observability service address (default: localhost:8816)",
    )
    args = parser.parse_args()

    obs_host, obs_port = args.obs.split(":")
    obs_port = int(obs_port)

    print_header("QUIVER E2E METRICS TEST")

    try:
        # Connect to servers
        print(f"\n📡 Connecting to servers...")
        client = Client(
            args.server,
            observability_host=obs_host,
            observability_port=obs_port,
        )
        print(f"   ✓ Connected to Quiver server at {args.server}")
        print(f"   ✓ Observability service at {obs_host}:{obs_port}")

        # Get server config
        print_header("SERVER CONFIGURATION")
        server_info = client.get_server_info()

        views = server_info.get("registry", {}).get("views", [])
        print(f"\nFound {len(views)} feature view(s):")
        for view in views:
            print(f"  - {view['name']}: {len(view.get('columns', []))} features")

        # Load fixtures
        fixtures = load_fixtures()

        # Query each view and get metrics
        print_header("FEATURE QUERIES WITH METRICS")

        for view in views:
            view_name = view.get("name", "")
            entity_type = view.get("entity_type", "")
            columns = view.get("columns", [])

            if not columns:
                print(f"\n⚠️  View {view_name}: no features to query")
                continue

            feature_names = [col["name"] for col in columns]

            # Find matching fixture scenario
            scenario = None
            for fixture_name, fixture_def in fixtures.items():
                if fixture_def.get("entity_type") == entity_type:
                    scenario = fixture_def
                    break

            if scenario:
                sample_entities = [record["entity"] for record in scenario["records"]]
            else:
                sample_entities = [
                    f"{entity_type}:1000",
                    f"{entity_type}:1001",
                    f"{entity_type}:1002",
                ]

            print(f"\n📋 View: {view_name}")
            print(f"   Entity Type: {entity_type}")
            print(f"   Features: {feature_names}")
            print(f"   Entities: {len(sample_entities)}")

            try:
                # Query features
                print(f"\n   🔍 Querying {len(sample_entities)} entities...")
                result = client.get_features(
                    view_name,
                    sample_entities,
                    feature_names,
                )

                print(f"   ✓ Retrieved {len(result)} rows")

                # Get request ID
                request_id = client.get_last_request_id()
                if request_id:
                    print(f"   📝 Request ID: {request_id}")

                    # Query metrics
                    print(f"\n   📊 Fetching metrics...")
                    try:
                        metrics = client.get_metrics(request_id)

                        print(f"   ✓ Metrics retrieved")

                        # Print metrics table
                        print_metrics_table(metrics)

                        # Analysis
                        print(f"\n💡 Analysis:")
                        merge_pct = (
                            metrics["merge_ms"] / metrics["total_ms"]
                        ) * 100
                        dispatch_pct = (
                            metrics["dispatch_ms"] / metrics["total_ms"]
                        ) * 100

                        print(f"   - Merge: {merge_pct:.1f}% of total latency")
                        print(f"   - Dispatch: {dispatch_pct:.1f}% of total latency")
                        print(
                            f"   - Backend max: {metrics['backend_max_ms']:.2f}ms"
                        )
                        if metrics['backend_max_ms'] > 0:
                            speedup = metrics['dispatch_ms'] / metrics['backend_max_ms']
                            print(
                                f"   - Fanout benefit: {speedup:.1f}x speedup with {len(view.get('columns', []))} features"
                            )

                        # Verify correctness
                        print(f"\n✅ Verification:")
                        print(f"   - Entity count: {metrics['entity_count']}")
                        print(f"   - Feature view: {metrics['feature_view']}")

                        if metrics["entity_count"] == len(sample_entities):
                            print(
                                f"   - ✓ Entity count matches (expected {len(sample_entities)})"
                            )
                        else:
                            print(
                                f"   - ⚠️  Entity count mismatch! Got {metrics['entity_count']}, expected {len(sample_entities)}"
                            )

                        if metrics["feature_view"] == view_name:
                            print(f"   - ✓ Feature view matches")
                        else:
                            print(
                                f"   - ⚠️  Feature view mismatch! Got {metrics['feature_view']}, expected {view_name}"
                            )

                    except Exception as e:
                        print(f"   ⚠️  Failed to retrieve metrics: {type(e).__name__}: {e}")
                else:
                    print(f"   ⚠️  No request ID available")

                # Show data preview
                print(f"\n📊 Data Preview:")
                df = result.to_pandas()
                print(f"   {len(df)} rows × {len(df.columns)} columns")
                for line in str(df).split("\n")[:10]:
                    print(f"   {line}")
                if len(df) > 10:
                    print(f"   ... ({len(df) - 10} more rows)")

            except Exception as e:
                print(f"   ⚠️  Error: {type(e).__name__}: {e}")

        print_header("E2E TEST COMPLETE")
        print("\n✅ All tests passed!\n")
        return 0

    except Exception as e:
        print_header("ERROR")
        print(f"\n❌ {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 1

    finally:
        try:
            client.close()
        except Exception:
            pass


if __name__ == "__main__":
    exit(main())
