import sys
import argparse
from typing import Dict, Any, List, Optional
import pyarrow.flight as flight
from quiver import Client
from quiver.exceptions import (
    QuiverConnectionError,
    QuiverFeatureViewNotFound,
    QuiverFeatureNotFound,
    QuiverServerError,
)
from s3_parquet_examples import S3ParquetExamples


def get_test_data_for_adapter_type(
    adapter_type: str, feature_view_name: str, num_entities: int = 3
) -> Dict[str, Any]:
    """Generate appropriate test data based on adapter type and feature view."""

    base_entities = [f"user_{i}" for i in range(1, num_entities + 1)]

    adapter_patterns = {
        "postgres": {
            "entities": base_entities,
            "feature_patterns": {
                "user_demographics": ["age", "country", "registration_date"],
                "user_scores": ["credit_score", "engagement_score", "last_updated"],
            },
        },
        "redis": {
            "entities": base_entities,
            "feature_patterns": {
                "realtime_features": [
                    "current_session_score",
                    "last_action_timestamp",
                    "is_online",
                ],
                "recommendation_features": [
                    "trending_category",
                    "recommendation_score",
                ],
            },
        },
        "memory": {
            "entities": base_entities,
            "feature_patterns": {
                "user_features": ["age", "score", "is_premium"],
            },
        },
    }

    pattern = adapter_patterns.get(adapter_type.lower(), {})

    features = pattern.get("feature_patterns", {}).get(feature_view_name, [])

    return {"entities": pattern.get("entities", base_entities), "features": features}


def get_test_data_from_server_config(
    server_info: Dict[str, Any],
    feature_view_name: str,
    num_entities: int = 3,
    max_features: Optional[int] = None,
) -> Dict[str, Any]:
    """Extract test data based on server configuration."""

    registry = server_info.get("registry", {})
    views = registry.get("views", [])

    feature_view = None
    for view in views:
        if view.get("name") == feature_view_name:
            feature_view = view
            break

    if not feature_view:
        return {
            "entities": [f"user_{i}" for i in range(1, num_entities + 1)],
            "features": [],
        }

    entity_key = feature_view.get("entity_key", "user_id")
    entity_type = feature_view.get("entity_type", "user")

    if "user" in entity_type.lower():
        test_entities = [f"user_{i}" for i in range(1, num_entities + 1)]
    else:
        test_entities = [f"{entity_type}_{i}" for i in range(1, num_entities + 1)]

    columns = feature_view.get("columns", [])
    features = []
    for col in columns:
        col_name = col.get("name", "")
        if col_name != entity_key:
            features.append(col_name)

    adapters = server_info.get("adapters", {})

    for adapter_name, adapter_config in adapters.items():
        adapter_type = adapter_config.get("type", "")
        if adapter_type:
            adapter_data = get_test_data_for_adapter_type(
                adapter_type, feature_view_name, num_entities
            )
            if adapter_data.get("entities"):
                test_entities = adapter_data["entities"]

    if max_features and len(features) > max_features:
        features = features[:max_features]

    return {
        "entities": test_entities,
        "features": features,
    }


def test_server_connection(endpoint: str, timeout: float = 30.0):
    """Test connection to Quiver server."""
    print(f"Connecting to Quiver server at {endpoint}...")

    try:
        client = Client(endpoint, validate_connection=True)
        print("Successfully connected to Quiver server")
        return client
    except QuiverConnectionError as e:
        print(f"Connection failed: {e}")
        print(f"  Make sure Quiver server is running on {endpoint}")
        return None
    except Exception as e:
        print(f"Unexpected connection error: {e}")
        return None


def discover_feature_views(client):
    """Discover available feature views."""
    print("\nDiscovering feature views...")

    try:
        feature_views = client.list_feature_views()
        if not feature_views:
            print("No feature views found")
            return []

        print(f"Found {len(feature_views)} feature view(s):")
        for view in feature_views:
            view_name = (
                view.name.decode("utf-8") if isinstance(view.name, bytes) else view.name
            )
            print(f"  - {view_name}")

        return feature_views
    except Exception as e:
        print(f"Failed to discover feature views: {e}")
        return []


def show_schema(client, feature_view_name: str, verbose: bool = True):
    """Show schema for a feature view using direct schema request."""
    if isinstance(feature_view_name, bytes):
        feature_view_name = feature_view_name.decode("utf-8")

    if verbose:
        print(f"\nSchema for {feature_view_name}:")

    try:
        schema = client.get_schema(feature_view=feature_view_name)

        if verbose:
            print(f"  Columns ({len(schema)}):")
            for field in schema:
                nullable = "nullable" if field.nullable else "not null"
                print(f"    {field.name}: {field.type} ({nullable})")
        return schema

    except Exception as e:
        if verbose:
            print(f"Failed to get schema directly: {e}")
            print("  Attempting schema discovery via feature request...")
        try:
            test_entities = ["user_1"]
            test_features = ["age", "score", "name"]

            feature_table = client.get_features(
                feature_view=feature_view_name,
                entities=test_entities,
                features=test_features,
            )

            schema = feature_table.to_arrow().schema
            if verbose:
                print(f"  Discovered schema with {len(schema)} columns:")
                for field in schema:
                    nullable = "nullable" if field.nullable else "not null"
                    print(f"    {field.name}: {field.type} ({nullable})")
            return schema

        except Exception as e2:
            if verbose:
                print(f"  Schema discovery also failed: {e2}")
            return None


def test_feature_retrieval(
    client,
    feature_view_name: str,
    entities: List[str],
    features: List[str],
    verbose: bool = True,
    show_data: bool = True,
):
    """Test feature retrieval."""
    if isinstance(feature_view_name, bytes):
        feature_view_name = feature_view_name.decode("utf-8")

    if verbose:
        print(f"\nTesting feature retrieval from {feature_view_name}...")
        print(f"  Entities: {entities}")
        print(f"  Features: {features}")

    try:
        feature_table = client.get_features(
            feature_view=feature_view_name, entities=entities, features=features
        )

        if verbose:
            print(f"Successfully retrieved {len(feature_table)} rows")

        if show_data:
            df = feature_table.to_pandas()
            if verbose:
                print(f"\nData from {feature_view_name}:")
            if not df.empty:
                print(df.to_string(index=False))
            else:
                if verbose:
                    print("  (no data returned - this is expected for test entities)")

        return True

    except QuiverFeatureViewNotFound:
        if verbose:
            print(f"Feature view {feature_view_name} not found")
        return False
    except QuiverFeatureNotFound as e:
        if verbose:
            print(f"Features not found: {e}")
            print("   This is normal if no test data is loaded in the backend")

        if len(features) > 1:
            if verbose:
                print(f"   Trying with just the first feature: {features[0]}")
            try:
                feature_table = client.get_features(
                    feature_view=feature_view_name,
                    entities=entities[:1],
                    features=[features[0]],
                )
                if verbose:
                    print(f"Partial success: retrieved {len(feature_table)} rows")
                if show_data:
                    df = feature_table.to_pandas()
                    if not df.empty:
                        if verbose:
                            print("  Sample data:")
                        print(df.to_string(index=False))
                    else:
                        if verbose:
                            print("  (no data, but schema validation successful)")
                return True
            except Exception as e2:
                if verbose:
                    print(f"   Single feature test also failed: {e2}")

        if verbose:
            print("Schema validation successful (no test data needed)")
        return True

    except Exception as e:
        if verbose:
            print(f"Feature retrieval failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Test Quiver server with configurable parameters",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - test all feature views
  python example.py

  # Test specific endpoint with more entities
  python example.py --endpoint localhost:8815 --entities 10

  # Test specific feature views only
  python example.py --feature-views user_demographics user_scores

  # Limit number of features per view
  python example.py --max-features 3

  # Quiet output showing only data
  python example.py --quiet --show-data-only

  # S3/Parquet examples (requires S3/Parquet server config)
  python example.py --s3-example all
  python example.py --s3-example user_features
  python example.py --s3-example daily_metrics

  # Comprehensive test with all parameters
  python example.py --entities 5 --max-features 5 --verbose --timeout 60
        """,
    )

    parser.add_argument(
        "--endpoint",
        "-e",
        default="localhost:8815",
        help="Quiver server endpoint (default: localhost:8815)",
    )
    parser.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=30.0,
        help="Connection timeout in seconds (default: 30.0)",
    )

    parser.add_argument(
        "--entities",
        "-n",
        type=int,
        default=3,
        help="Number of test entities to generate (default: 3)",
    )
    parser.add_argument(
        "--max-features",
        "-f",
        type=int,
        help="Maximum number of features to test per feature view",
    )

    parser.add_argument(
        "--feature-views",
        nargs="+",
        help="Specific feature views to test (default: test all discovered views)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Quiet output (minimal logging)",
    )
    parser.add_argument(
        "--show-data-only",
        action="store_true",
        help="Only show retrieved data, no metadata or status messages",
    )
    parser.add_argument(
        "--no-data",
        action="store_true",
        help="Don't show retrieved data, only test connectivity and schemas",
    )

    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Only test schema discovery, skip feature retrieval",
    )

    parser.add_argument(
        "--s3-example",
        choices=["user_features", "daily_metrics", "products", "all"],
        help="Run S3/Parquet adapter examples (requires compatible server config)",
    )

    args = parser.parse_args()

    verbose = args.verbose and not args.quiet and not args.show_data_only
    show_data = not args.no_data
    quiet = args.quiet or args.show_data_only

    if args.show_data_only:
        verbose = False
        quiet = True

    client = test_server_connection(args.endpoint, args.timeout)
    if not client:
        sys.exit(1)

    if args.s3_example:
        try:
            s3_examples = S3ParquetExamples(client)

            print("=" * 60)
            print("S3/Parquet Adapter Examples")
            print("=" * 60)

            if args.s3_example in ["all", "user_features"]:
                s3_examples.query_user_features()

            if args.s3_example in ["all", "daily_metrics"]:
                s3_examples.query_daily_metrics_timetravel()

            if args.s3_example in ["all", "products"]:
                s3_examples.query_products()

            print("\n" + "=" * 60)
            print("✓ S3/Parquet examples completed!")
            print("=" * 60)
        except Exception as e:
            print(f"Error running S3/Parquet examples: {e}")
            sys.exit(1)
        finally:
            client.close()
        return

    try:
        if not quiet:
            print("Getting server configuration...")
        server_info = client.get_server_info()
        if verbose:
            print(f"Retrieved server configuration")

        adapters = server_info.get("adapters", {})
        if adapters and not quiet:
            print(f"Server adapters: {list(adapters.keys())}")
            if verbose:
                for name, config in adapters.items():
                    adapter_type = config.get("type", "unknown")
                    print(f"{name}: {adapter_type}")

        feature_views = discover_feature_views(client)
        if not feature_views:
            if not quiet:
                print("\nNo feature views to test")
            sys.exit(1)

        if args.feature_views:
            filtered_views = []
            requested_names = set(args.feature_views)
            for view in feature_views:
                view_name = (
                    view.name.decode("utf-8")
                    if isinstance(view.name, bytes)
                    else view.name
                )
                if view_name in requested_names:
                    filtered_views.append(view)
            feature_views = filtered_views

            if not feature_views and not quiet:
                print(
                    f"None of the requested feature views found: {args.feature_views}"
                )
                sys.exit(1)

        total_tests = 0
        passed_tests = 0

        for view in feature_views:
            view_name = (
                view.name.decode("utf-8") if isinstance(view.name, bytes) else view.name
            )

            schema = show_schema(client, view_name, verbose=verbose and not quiet)
            if not schema:
                continue

            if args.schema_only:
                continue

            test_data = get_test_data_from_server_config(
                server_info, view_name, args.entities, args.max_features
            )
            entities = test_data.get(
                "entities", [f"user_{i}" for i in range(1, args.entities + 1)]
            )
            features = test_data.get("features", [])

            if not features:
                for field in schema:
                    if not field.name.endswith("_id") and field.name != "user_id":
                        features.append(field.name)
                        if args.max_features and len(features) >= args.max_features:
                            break

            if features:
                total_tests += 1
                success = test_feature_retrieval(
                    client,
                    view_name,
                    entities,
                    features,
                    verbose=verbose and not quiet,
                    show_data=show_data,
                )
                if success:
                    passed_tests += 1
            else:
                if verbose and not quiet:
                    print(f"  (no features to test for {view_name})")

    except KeyboardInterrupt:
        if not quiet:
            print("\nTest interrupted by user")
        sys.exit(1)
    except Exception as e:
        if not quiet:
            print(f"\nUnexpected error: {e}")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()
