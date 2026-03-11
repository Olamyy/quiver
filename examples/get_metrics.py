#!/usr/bin/env python
"""Example: Retrieve and display request metrics.

This example shows how to:
1. Get features from Quiver server
2. Extract the request ID from the response
3. Query detailed metrics from the observability service
4. Display and analyze the latency breakdown
"""

from datetime import datetime
import quiver


def main():
    """Main entry point."""

    client = quiver.Client(
        address="localhost:8815",
        timeout=30.0,
        observability_host="localhost",
        observability_port=8816,
    )

    try:
        feature_view = "user_profile"
        entities = ["user:1", "user:2", "user:3"]
        features = ["spend_30d", "purchase_count", "active_session_id"]

        print(f"\n📊 Fetching features: {features}")
        print(f"   From view: {feature_view}")
        print(f"   For entities: {entities}\n")

        table = client.get_features(
            feature_view=feature_view,
            entities=entities,
            features=features,
        )

        print(f"✅ Received {len(table)} rows\n")
        print(table)

        request_id = client.get_last_request_id()
        if request_id:
            print(f"\n📝 Request ID: {request_id}\n")

            try:
                metrics = client.get_metrics(request_id)

                print("⏱️  Latency Breakdown (11 Instrumentation Points):\n")
                print(f"  Registry Lookup:    {metrics['registry_lookup_ms']:7.2f} ms")
                print(f"  Cache Lookup:       {metrics['cache_lookup_ms']:7.2f} ms")
                print(f"  Partition:          {metrics['partition_ms']:7.2f} ms")
                print(f"  Dispatch:           {metrics['dispatch_ms']:7.2f} ms")
                print(f"  Backend Execution:")
                print(
                    f"    - Redis:          {metrics['backend_redis_ms']:7.2f} ms"
                )
                print(
                    f"    - PostgreSQL:     {metrics['backend_postgres_ms']:7.2f} ms"
                )
                print(f"    - Max:            {metrics['backend_max_ms']:7.2f} ms")
                print(f"  Alignment:          {metrics['alignment_ms']:7.2f} ms")
                print(f"  Merge:              {metrics['merge_ms']:7.2f} ms")
                print(f"  Validation:         {metrics['validation_ms']:7.2f} ms")
                print(f"  Serialization:      {metrics['serialization_ms']:7.2f} ms")
                print("-" * 45)
                print(f"  Total:              {metrics['total_ms']:7.2f} ms")
                print(f"  Critical Path:      {metrics['critical_path_ms']:7.2f} ms")

                print(f"\n📋 Request Metadata:")
                print(f"  Feature View: {metrics['feature_view']}")
                print(f"  Entity Count: {metrics['entity_count']}")
                print(f"  Timestamp:    {metrics['request_timestamp']}")

                print(f"\n💡 Analysis:")
                merge_pct = (metrics["merge_ms"] / metrics["total_ms"]) * 100
                dispatch_pct = (metrics["dispatch_ms"] / metrics["total_ms"]) * 100
                print(f"  - Merge represents {merge_pct:.1f}% of total latency")
                print(f"  - Dispatch represents {dispatch_pct:.1f}% of total latency")
                print(
                    f"  - Backend max latency: {metrics['backend_max_ms']:.2f}ms"
                )

            except quiver.QuiverServerError as e:
                print(f"⚠️  Could not retrieve metrics: {str(e)}")
                print("   (Metrics may have expired or observability service is unavailable)")
        else:
            print("⚠️  No request ID available")

    finally:
        client.close()
        print("\n✅ Connection closed\n")


if __name__ == "__main__":
    main()
