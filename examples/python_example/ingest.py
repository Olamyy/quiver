import argparse
import json
import random
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
import redis

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PARQUET = True
except ImportError:
    HAS_PARQUET = False

try:
    import boto3
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False


class TestDataGenerator:
    """Generate realistic test data for different scenarios."""

    @staticmethod
    def generate_users(count: int = 100) -> List[Dict[str, Any]]:
        """Generate user demographic data."""
        countries = ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "SG"]
        users = []

        for i in range(1, count + 1):
            user = {
                "user_id": f"user_{i}",
                "age": random.randint(18, 80),
                "country": random.choice(countries),
                "registration_date": datetime.now()
                - timedelta(days=random.randint(1, 1095)),
            }
            users.append(user)

        return users

    @staticmethod
    def generate_scores(user_ids: List[str]) -> List[Dict[str, Any]]:
        """Generate user score data."""
        scores = []

        for user_id in user_ids:
            num_scores = random.randint(1, 5)

            for i in range(num_scores):
                score = {
                    "user_id": user_id,
                    "credit_score": round(random.uniform(300, 850), 1),
                    "engagement_score": round(random.uniform(0, 100), 1),
                    "last_updated": datetime.now()
                    - timedelta(hours=random.randint(1, 24 * 30)),
                }
                scores.append(score)

        return scores

    @staticmethod
    def generate_realtime_features(user_ids: List[str]) -> List[Dict[str, Any]]:
        """Generate real-time feature data for Redis."""
        features = []

        for user_id in user_ids:
            feature = {
                "user_id": user_id,
                "current_session_score": round(random.uniform(0, 100), 2),
                "last_action_timestamp": datetime.now()
                - timedelta(minutes=random.randint(1, 60)),
                "is_online": random.choice([True, False]),
                "recent_clicks": random.randint(0, 50),
                "current_session_duration": random.randint(60, 3600),
            }
            features.append(feature)

        return features

    @staticmethod
    def generate_recommendations(user_ids: List[str]) -> List[Dict[str, Any]]:
        """Generate recommendation feature data for Redis."""
        categories = ["electronics", "books", "clothing", "home", "sports", "toys", "music", "movies"]
        recommendations = []

        for user_id in user_ids:
            rec = {
                "user_id": user_id,
                "trending_category": random.choice(categories),
                "recommendation_score": round(random.uniform(0.1, 1.0), 2),
            }
            recommendations.append(rec)

        return recommendations

    @staticmethod
    def generate_user_features(user_ids: List[str], num_records: int = 100) -> List[Dict[str, Any]]:
        """Generate user feature data for S3/Parquet."""
        records = []
        for i in range(num_records):
            user_id = random.choice(user_ids)
            records.append({
                "user_id": user_id,
                "age": random.randint(18, 80),
                "lifetime_spend": round(random.uniform(10, 10000), 2),
                "account_created": datetime.now() - timedelta(days=random.randint(1, 1095)),
                "is_vip": random.choice([True, False]),
            })
        return records

    @staticmethod
    def generate_daily_metrics(
        user_ids: List[str],
        date: datetime,
        num_records: int = 50,
    ) -> List[Dict[str, Any]]:
        """Generate daily user metrics for S3/Parquet."""
        records = []
        for i in range(num_records):
            user_id = random.choice(user_ids)
            records.append({
                "user_id": user_id,
                "daily_active_users": random.randint(0, 1),
                "session_count": random.randint(0, 10),
                "avg_session_duration": round(random.uniform(0, 3600), 2),
                "date": date.strftime("%Y-%m-%d"),
            })
        return records

    @staticmethod
    def generate_products(num_products: int = 50) -> List[Dict[str, Any]]:
        """Generate product catalog for S3/Parquet."""
        records = []
        for i in range(num_products):
            records.append({
                "product_id": f"prod_{i:05d}",
                "product_name": f"Product {i}",
                "price": round(random.uniform(10, 500), 2),
                "inventory_count": random.randint(0, 1000),
                "last_restocked": datetime.now() - timedelta(days=random.randint(1, 30)),
                "is_available": random.choice([True, False]),
            })
        return records


class PostgreSQLIngester:
    """Ingest test data into PostgreSQL tables."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def ingest_demographics(self, users: List[Dict[str, Any]]):
        """Ingest user demographics data into per-feature tables."""
        print(f"Ingesting {len(users)} user demographics records...")

        with psycopg2.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                for user in users:
                    cur.execute(
                        """
                        INSERT INTO user_features_age 
                        (user_id, age, feature_ts)
                        VALUES (%(user_id)s, %(age)s, NOW())
                        ON CONFLICT (user_id, feature_ts) DO UPDATE SET age = EXCLUDED.age
                    """,
                        user,
                    )
                    cur.execute(
                        """
                        INSERT INTO user_features_country 
                        (user_id, country, feature_ts)
                        VALUES (%(user_id)s, %(country)s, NOW())
                        ON CONFLICT (user_id, feature_ts) DO UPDATE SET country = EXCLUDED.country
                    """,
                        user,
                    )
                    cur.execute(
                        """
                        INSERT INTO user_features_registration_date 
                        (user_id, registration_date, feature_ts)
                        VALUES (%(user_id)s, %(registration_date)s, NOW())
                        ON CONFLICT (user_id, feature_ts) DO UPDATE SET registration_date = EXCLUDED.registration_date
                    """,
                        user,
                    )
                conn.commit()

        print("✓ Demographics data ingested")

    def ingest_scores(self, scores: List[Dict[str, Any]]):
        """Ingest user scores data into per-feature tables."""
        print(f"Ingesting {len(scores)} user scores records...")

        with psycopg2.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                for score in scores:
                    cur.execute(
                        """
                        INSERT INTO user_features_credit_score 
                        (user_id, credit_score, feature_ts)
                        VALUES (%(user_id)s, %(credit_score)s, NOW())
                        ON CONFLICT (user_id, feature_ts) DO UPDATE SET credit_score = EXCLUDED.credit_score
                    """,
                        score,
                    )
                    cur.execute(
                        """
                        INSERT INTO user_features_engagement_score 
                        (user_id, engagement_score, feature_ts)
                        VALUES (%(user_id)s, %(engagement_score)s, NOW())
                        ON CONFLICT (user_id, feature_ts) DO UPDATE SET engagement_score = EXCLUDED.engagement_score
                    """,
                        score,
                    )
                    cur.execute(
                        """
                        INSERT INTO user_features_last_updated 
                        (user_id, last_updated, feature_ts)
                        VALUES (%(user_id)s, %(last_updated)s, NOW())
                        ON CONFLICT (user_id, feature_ts) DO UPDATE SET last_updated = EXCLUDED.last_updated
                    """,
                        score,
                    )
                conn.commit()

        print("✓ Scores data ingested")

    def clear_data(self):
        """Clear existing test data from per-feature tables."""
        print("Clearing existing PostgreSQL data...")

        with psycopg2.connect(self.connection_string) as conn:
            with conn.cursor() as cur:
                tables_to_clear = [
                    "user_features_age",
                    "user_features_country",
                    "user_features_registration_date",
                    "user_features_credit_score",
                    "user_features_engagement_score",
                    "user_features_last_updated",
                ]

                for table in tables_to_clear:
                    try:
                        cur.execute(f"TRUNCATE {table} CASCADE")
                        print(f"  Cleared {table}")
                    except psycopg2.Error as e:
                        print(f"  Warning: Could not clear {table}: {e}")

                conn.commit()

        print("✓ PostgreSQL data cleared")


class S3ParquetIngester:
    """Ingest test data to S3/Parquet files."""

    def __init__(self, storage_path: str, use_s3: bool = False):
        if not HAS_PARQUET:
            raise ImportError("pyarrow is required for S3/Parquet ingestion. Install with: pip install pyarrow")
        self.storage_path = storage_path
        self.use_s3 = use_s3
        self.s3_client = None
        if use_s3 and HAS_BOTO3:
            import os
            region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
            self.s3_client = boto3.client("s3", region_name=region)

    def ingest_user_features(self, data: List[Dict[str, Any]]):
        """Ingest user features to Parquet."""
        print(f"Ingesting {len(data)} user feature records to Parquet...")
        table = pa.Table.from_pylist(data)
        self._write_parquet(table, "/features/user_features.parquet")
        print("✓ User features ingested")

    def ingest_daily_metrics(self, data_by_date: Dict[str, List[Dict[str, Any]]]):
        """Ingest daily metrics partitioned by date."""
        print(f"Ingesting daily metrics across {len(data_by_date)} partitions...")
        for date_str, records in data_by_date.items():
            if records:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                year = date_obj.strftime("%Y")
                month = date_obj.strftime("%m")
                day = date_obj.strftime("%d")
                table = pa.Table.from_pylist(records)
                path = f"/year={year}/month={month}/day={day}/data.parquet"
                self._write_parquet(table, path)
        print("✓ Daily metrics ingested")

    def ingest_products(self, data: List[Dict[str, Any]]):
        """Ingest product catalog to Parquet."""
        print(f"Ingesting {len(data)} product records to Parquet...")
        table = pa.Table.from_pylist(data)
        self._write_parquet(table, "/products/catalog.parquet")
        print("✓ Products ingested")

    def _write_parquet(self, table: pa.Table, path: str):
        """Write Parquet file locally or to S3."""
        if self.use_s3 and self.s3_client:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                tmp_path = tmp.name
            pq.write_table(table, tmp_path)
            bucket = self.storage_path.replace("s3://", "").split("/")[0]
            key = path.lstrip("/")
            print(f"DEBUG _write: storage_path={self.storage_path}, bucket={bucket}, key={key}")
            self.s3_client.upload_file(tmp_path, bucket, key)
            Path(tmp_path).unlink()
            print(f"  ✓ {path}")
        else:
            full_path = Path(self.storage_path) / path.lstrip("/")
            full_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, full_path)
            print(f"  ✓ {path}")


class RedisIngester:
    """Ingest test data into Redis."""

    def __init__(self, redis_url: str = "redis://:quiver_dev_redis_password@127.0.0.1:6379"):
        self.redis_client = redis.from_url(redis_url)

    def ingest_realtime_features(self, features: List[Dict[str, Any]]):
        """Ingest real-time features into Redis."""
        print(f"Ingesting {len(features)} real-time feature records...")

        pipe = self.redis_client.pipeline()

        for feature in features:
            user_id = feature["user_id"]

            pipe.set(
                f"features:user:{user_id}:feature:current_session_score",
                feature["current_session_score"],
            )
            pipe.set(
                f"features:user:{user_id}:feature:last_action_timestamp",
                feature["last_action_timestamp"].isoformat(),
            )
            pipe.set(
                f"features:user:{user_id}:feature:is_online",
                json.dumps(feature["is_online"]),
            )
            pipe.set(
                f"features:user:{user_id}:feature:recent_clicks",
                feature["recent_clicks"],
            )
            pipe.set(
                f"features:user:{user_id}:feature:current_session_duration",
                feature["current_session_duration"],
            )

        pipe.execute()
        print("✓ Real-time features ingested")

    def ingest_recommendations(self, recommendations: List[Dict[str, Any]]):
        """Ingest recommendation features into Redis."""
        print(f"Ingesting {len(recommendations)} recommendation records...")

        pipe = self.redis_client.pipeline()

        for rec in recommendations:
            user_id = rec["user_id"]

            pipe.set(
                f"ml:recommendations:{user_id}:trending_category",
                rec["trending_category"],
            )
            pipe.set(
                f"ml:recommendations:{user_id}:recommendation_score",
                rec["recommendation_score"],
            )

        pipe.execute()
        print("✓ Recommendation features ingested")

    def clear_data(self):
        """Clear existing test data."""
        print("Clearing existing Redis data...")

        keys = self.redis_client.keys("features:user:*")
        if keys:
            self.redis_client.delete(*keys)

        print("✓ Redis data cleared")


def main():
    parser = argparse.ArgumentParser(description="Ingest test data for Quiver adapters")
    parser.add_argument(
        "--adapter",
        choices=["postgres", "redis", "s3_parquet", "all"],
        default="all",
        help="Which adapter to populate",
    )
    parser.add_argument(
        "--scenario",
        choices=["basic", "comprehensive"],
        default="basic",
        help="Data scenario to generate",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=20,
        help="Number of users to generate (default: 20)",
    )
    parser.add_argument(
        "--clear", action="store_true", help="Clear existing data before ingesting"
    )
    parser.add_argument(
        "--postgres-url",
        default="postgresql://quiver_user:quiver_dev_password@127.0.0.1:5432/quiver_features",
        help="PostgreSQL connection string",
    )
    parser.add_argument(
        "--redis-url", default="redis://:quiver_dev_redis_password@127.0.0.1:6379", help="Redis connection string"
    )
    parser.add_argument(
        "--parquet-dir",
        default="./parquet_data",
        help="Local directory for Parquet files (default: ./parquet_data)",
    )
    parser.add_argument(
        "--use-s3",
        action="store_true",
        help="Upload to S3 instead of local filesystem",
    )
    parser.add_argument(
        "--s3-bucket",
        help="S3 bucket name (required if --use-s3 is set)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days of historical data for partitioned Parquet (default: 7)",
    )

    args = parser.parse_args()

    if args.use_s3 and not args.s3_bucket:
        print("Error: --s3-bucket is required when using --use-s3")
        exit(1)

    if not args.use_s3:
        Path(args.parquet_dir).mkdir(parents=True, exist_ok=True)

    print("Quiver Test Data Ingestion")
    print(f"Adapter: {args.adapter}")
    print(f"Scenario: {args.scenario}")
    print(f"Users: {args.users}")
    print("=" * 40)

    generator = TestDataGenerator()
    users = generator.generate_users(args.users)
    user_ids = [user["user_id"] for user in users]

    if args.scenario == "comprehensive":
        scores = generator.generate_scores(user_ids)
        realtime_features = generator.generate_realtime_features(user_ids)
        recommendations = generator.generate_recommendations(user_ids)
        user_features = generator.generate_user_features(user_ids, num_records=len(user_ids) * 2)
        products = generator.generate_products(num_products=50)
    else:
        scores = generator.generate_scores(user_ids[:10])
        realtime_features = generator.generate_realtime_features(user_ids[:10])
        recommendations = generator.generate_recommendations(user_ids[:10])
        user_features = generator.generate_user_features(user_ids, num_records=len(user_ids))
        products = generator.generate_products(num_products=20)

    if args.adapter in ["postgres", "all"]:
        try:
            pg_ingester = PostgreSQLIngester(args.postgres_url)

            if args.clear:
                pg_ingester.clear_data()

            pg_ingester.ingest_demographics(users)
            pg_ingester.ingest_scores(scores)

        except Exception as e:
            print(f"PostgreSQL ingestion failed: {e}")

    if args.adapter in ["redis", "all"]:
        try:
            redis_ingester = RedisIngester(args.redis_url)

            if args.clear:
                redis_ingester.clear_data()

            redis_ingester.ingest_realtime_features(realtime_features)
            redis_ingester.ingest_recommendations(recommendations)

        except Exception as e:
            print(f"Redis ingestion failed: {e}")

    if args.adapter in ["s3_parquet", "all"]:
        try:
            storage_path = f"s3://{args.s3_bucket}" if args.use_s3 and args.s3_bucket else args.parquet_dir
            print(f"DEBUG: storage_path={storage_path}, use_s3={args.use_s3}, s3_bucket={args.s3_bucket}")
            s3_ingester = S3ParquetIngester(storage_path, use_s3=args.use_s3)

            s3_ingester.ingest_user_features(user_features)
            s3_ingester.ingest_products(products)

            data_by_date = {}
            for i in range(args.days):
                date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
                data_by_date[date] = generator.generate_daily_metrics(
                    user_ids,
                    datetime.strptime(date, "%Y-%m-%d"),
                    num_records=len(user_ids),
                )
            s3_ingester.ingest_daily_metrics(data_by_date)

        except Exception as e:
            import traceback
            print(f"S3/Parquet ingestion failed: {e}")
            traceback.print_exc()

    print(f"\nData ingestion completed!")
    print(f"Summary:")
    print(f"   Users: {len(users)}")
    if args.adapter in ["postgres", "all"]:
        print(f"   PostgreSQL demographics: {len(users)} records")
        print(f"   PostgreSQL scores: {len(scores)} records")
    if args.adapter in ["redis", "all"]:
        print(f"   Redis realtime features: {len(realtime_features)} records")
        print(f"   Redis recommendations: {len(recommendations)} records")
    if args.adapter in ["s3_parquet", "all"]:
        print(f"   S3/Parquet user features: {len(user_features)} records")
        print(f"   S3/Parquet products: {len(products)} records")
        print(f"   S3/Parquet daily metrics: {args.days} partitions")


if __name__ == "__main__":
    main()
