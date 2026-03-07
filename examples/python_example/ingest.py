import argparse
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any
import psycopg2
from psycopg2.extras import RealDictCursor
import redis


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


class RedisIngester:
    """Ingest test data into Redis."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
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
        choices=["postgres", "redis", "all"],
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
        "--redis-url", default="redis://127.0.0.1:6379", help="Redis connection string"
    )

    args = parser.parse_args()

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
    else:
        scores = generator.generate_scores(user_ids[:10])
        realtime_features = generator.generate_realtime_features(user_ids[:10])

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

        except Exception as e:
            print(f"Redis ingestion failed: {e}")

    print(f"\nData ingestion completed!")
    print(f"Summary:")
    print(f"   Users: {len(users)}")
    if args.adapter in ["postgres", "all"]:
        print(f"   PostgreSQL demographics: {len(users)} records")
        print(f"   PostgreSQL scores: {len(scores)} records")
    if args.adapter in ["redis", "all"]:
        print(f"   Redis features: {len(realtime_features)} records")


if __name__ == "__main__":
    main()
