"""S3/Parquet adapter examples for Quiver."""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from quiver import Client
from quiver.exceptions import (
    QuiverConnectionError,
    QuiverFeatureViewNotFound,
    QuiverFeatureNotFound,
    QuiverServerError,
)


class S3ParquetExamples:
    """Example queries for S3/Parquet-backed features."""

    def __init__(self, client: Client):
        self.client = client

    def query_user_features(self) -> Optional[Dict[str, Any]]:
        """Example: Query user demographic features (basic)."""
        try:
            print("\n=== User Features (Basic Query) ===")

            feature_view = "batch_user_features"
            entity_ids = ["user_1", "user_2", "user_3"]
            features = ["age", "lifetime_spend", "is_vip"]

            print(f"Feature view: {feature_view}")
            print(f"Entities: {entity_ids}")
            print(f"Features: {features}")

            result = self.client.get_features(
                feature_view=feature_view,
                entities=entity_ids,
                features=features,
            )

            df = result.to_pandas()
            print(f"\nResult shape: {df.shape}")
            print("\nFeatures:")
            print(df.to_string())
            return {"dataframe": df}

        except QuiverFeatureViewNotFound as e:
            print(f"Error: Feature view not found: {e}")
            return None
        except QuiverServerError as e:
            print(f"Error: Server error: {e}")
            return None

    def query_daily_metrics_timetravel(self) -> Optional[Dict[str, Any]]:
        """Example: Query daily metrics with time-travel (as_of)."""
        try:
            print("\n=== Daily Metrics (Time-Travel Query) ===")

            feature_view = "daily_user_features"
            entity_ids = ["user_1", "user_2"]
            features = ["daily_active_users", "session_count", "avg_session_duration"]

            as_of = datetime.now() - timedelta(days=3)

            print(f"Feature view: {feature_view}")
            print(f"Entities: {entity_ids}")
            print(f"Features: {features}")
            print(f"As of: {as_of.date()}")

            result = self.client.get_features(
                feature_view=feature_view,
                entities=entity_ids,
                features=features,
                as_of=as_of,
            )

            df = result.to_pandas()
            print(f"\nResult shape: {df.shape}")
            print("\nFeatures:")
            print(df.to_string())
            print(f"\nNote: Queried latest partition on or before {as_of.date()}")
            return {"dataframe": df}

        except QuiverServerError as e:
            print(f"Error: Server error: {e}")
            return None

    def query_products(self) -> Optional[Dict[str, Any]]:
        """Example: Query product catalog."""
        try:
            print("\n=== Product Catalog ===")

            feature_view = "product_features"
            entity_ids = ["prod_00001", "prod_00002", "prod_00005"]
            features = ["product_name", "price", "inventory_count", "is_available"]

            print(f"Feature view: {feature_view}")
            print(f"Entities (products): {entity_ids}")
            print(f"Features: {features}")

            result = self.client.get_features(
                feature_view=feature_view,
                entities=entity_ids,
                features=features,
            )

            df = result.to_pandas()
            print(f"\nResult shape: {df.shape}")
            print("\nFeatures:")
            print(df.to_string())
            return {"dataframe": df}

        except QuiverFeatureViewNotFound as e:
            print(f"Error: Feature view not found: {e}")
            return None
        except QuiverServerError as e:
            print(f"Error: Server error: {e}")
            return None

    def list_feature_views(self):
        """List available feature views."""
        try:
            info = self.client.get_server_info()
            views = info.get("registry", {}).get("views", [])

            print("\n=== Available Feature Views ===")
            for view in views:
                print(f"  - {view.get('name')}")
                print(f"    Entity type: {view.get('entity_type')}")
                cols = [col.get("name", "") for col in view.get("columns", [])]
                print(f"    Columns: {', '.join(cols)}")
        except Exception as e:
            print(f"Error listing feature views: {e}")
