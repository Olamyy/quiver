import argparse
import csv
import io
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2 import pool
import redis
import boto3
import pandas as pd

from quiver import Client
from scenarios import (
    get_scenario_config,
    MetricsCollector,
)


class BackendConnector:
    """Handles connections to Redis and PostgreSQL backends with connection pooling."""

    def __init__(self,
                 redis_url: str = "redis://localhost:6379",
                 db_config: Optional[Dict] = None,
                 view_config: Optional[Dict] = None,
                 s3_bucket: str = "quiver-benchmark",
                 s3_region: str = "eu-central-1"):
        self.redis_url = redis_url
        self.redis_client = None
        self.db_config = db_config or {
            "host": "127.0.0.1",
            "user": "quiver",
            "password": "quiver_test",
            "database": "quiver_test",
        }
        self.db_pool = None
        self.view_config = view_config
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.s3_client = None
        self.s3_cache = {}
        self.s3_cache_size = 0
        self.postgres_views = {}
        self.redis_views = {}
        self.initialized = False
        self.MAX_CACHE_SIZE_MB = 500



    def connect_redis(self):
        self.redis_client = redis.from_url(self.redis_url, decode_responses=True)

    def connect_postgres(self):
        """Establish database connection pool."""
        try:
            self.db_pool = pool.SimpleConnectionPool(5, 20, **self.db_config)
        except psycopg2.OperationalError as e:
            print(f"Failed to connect to PostgreSQL: {e}")
            raise

    def connect_s3(self):
        self.s3_client = boto3.client("s3", region_name=self.s3_region)

    def close(self):
        """Close connections."""
        if self.db_pool:
            self.db_pool.closeall()
        if self.redis_client:
            self.redis_client.close()

    def get_features_for_view(
        self,
        entity_ids: List[str],
        feature_view: str,
        feature_names: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch all features for a view by routing to appropriate backends.

        Inspects feature definitions to determine which backend handles each feature,
        then fetches in parallel matching Quiver's routing logic.
        """
        results_by_backend = {}
        view_config = self.view_config.get(feature_view, {})
        columns = view_config.get("columns", {})

        features_by_source = {}
        for feature_name in feature_names:
            col_config = columns.get(feature_name, {})
            source = col_config.get("source", "")
            if source not in features_by_source:
                features_by_source[source] = []
            features_by_source[source].append(feature_name)

        with ThreadPoolExecutor(max_workers=len(features_by_source)) as executor:
            futures = {}

            for source, features in features_by_source.items():
                if "redis" in source:
                    futures[source] = executor.submit(
                        self._get_redis_features,
                        entity_ids,
                        features,
                    )
                elif "postgres" in source:
                    table_name = f"{feature_view.replace('_view', '')}_data"
                    futures[source] = executor.submit(
                        self._get_postgres_features,
                        entity_ids,
                        table_name,
                        features,
                    )
                elif "s3" in source:
                    parquet_key = f"features/{source}.parquet"
                    futures[source] = executor.submit(
                        self._get_s3_features,
                        entity_ids,
                        features,
                        parquet_key,
                    )

            for source, future in futures.items():
                results_by_backend[source] = future.result()

        merged = {}
        for entity_id in entity_ids:
            merged[entity_id] = {}
            for backend_results in results_by_backend.values():
                if entity_id in backend_results:
                    merged[entity_id].update(backend_results[entity_id])

        return merged

    def _get_redis_features(
        self,
        entity_ids: List[str],
        feature_names: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch Redis-backed features using pipelining and HMGET."""
        results = {}

        if not feature_names:
            for entity_id in entity_ids:
                results[entity_id] = {}
            return results

        try:
            pipeline = self.redis_client.pipeline()
            for entity_id in entity_ids:
                pipeline.hmget(entity_id, feature_names)

            responses = pipeline.execute()
            for entity_id, field_values in zip(entity_ids, responses):
                data = {}
                for field_name, value in zip(feature_names, field_values):
                    if value is not None:
                        data[field_name] = value
                results[entity_id] = data
        except Exception: # noqa
            for entity_id in entity_ids:
                results[entity_id] = {}

        return results

    def _get_postgres_features(
        self,
        entity_ids: List[str],
        table_name: str,
        feature_names: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch PostgreSQL-backed features."""
        results = {}

        if not self.db_pool:
            for entity_id in entity_ids:
                results[entity_id] = {}
            return results

        conn = self.db_pool.getconn()
        try:
            cursor = conn.cursor()
            entity_tuple = tuple(entity_ids)
            placeholders = ",".join(["%s"] * len(entity_ids))

            selected_cols = ["entity"] + feature_names
            query = f"SELECT {', '.join(selected_cols)} FROM {table_name} WHERE entity IN ({placeholders}) ORDER BY timestamp DESC"
            cursor.execute(query, entity_tuple)

            rows = cursor.fetchall()
            col_names = [desc[0] for desc in cursor.description]

            for row in rows:
                entity_id = row[col_names.index("entity")]
                if entity_id not in results:
                    results[entity_id] = {col_names[i]: row[i] for i in range(len(col_names)) if col_names[i] != "entity"}

            cursor.close()
        except Exception as e:
            print(f"PostgreSQL query error: {e}")
        finally:
            self.db_pool.putconn(conn)

        for entity_id in entity_ids:
            if entity_id not in results:
                results[entity_id] = {}

        return results

    def _get_s3_features(
        self,
        entity_ids: List[str],
        feature_names: List[str],
        parquet_key: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch S3/Parquet features."""
        results = {entity_id: {} for entity_id in entity_ids}

        if not self.s3_client or not feature_names:
            return results

        if parquet_key not in self.s3_cache:
            response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=parquet_key)
            body = response["Body"].read()
            df = pd.read_parquet(io.BytesIO(body))
            size_mb = df.memory_usage(deep=True).sum() / 1024 / 1024

            # Evict if cache would exceed limit
            if self.s3_cache_size + size_mb > self.MAX_CACHE_SIZE_MB:
                self.s3_cache.clear()
                self.s3_cache_size = 0

            self.s3_cache[parquet_key] = df
            self.s3_cache_size += size_mb
        else:
            df = self.s3_cache[parquet_key]

        entity_set = set(entity_ids)
        for _, row in df.iterrows():
            entity = row.get("entity")
            if entity in entity_set:
                for feature in feature_names:
                    if feature in row:
                        results[entity][feature] = row[feature]

        return results

def benchmark_scenario(
    entity_count: int = 100,
    duration_secs: int = 10,
) -> tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict]]:
    """Run benchmark for a scenario with parallel backend calls.

    Returns (metrics, feature_samples, views) where feature_samples is a list of
    dicts containing extracted features and views is the config from server.
    """

    try:
        quiver_client = Client("localhost:8815")
        server_info = quiver_client.get_server_info()
    except Exception as e:
        print(f"Warning: Could not get server info: {e}")
        server_info = {}

    view_config = {}
    views = server_info.get("registry", {}).get("views", [])
    adapters = server_info.get("adapters", {})

    for view in views:
        view_name = view.get("name", "")
        columns = {}
        for col in view.get("columns", []):
            columns[col.get("name")] = col
        view_config[view_name] = {"columns": columns, "adapters": adapters}

    connector = BackendConnector(
        view_config=view_config
    )

    if not connector.initialized:
        connector.connect_redis()
        connector.connect_postgres()
        connector.connect_s3()
        connector.initialized = True

    prefix_map = {
        "ecommerce_view": "user",
        "timeseries_view": "sensor",
    }

    entity_ids_by_view = {}
    feature_names_by_view = {}
    for view in views:
        view_name = view.get("name", "")
        prefix = prefix_map.get(view_name, "entity")
        entity_ids_by_view[view_name] = [f"{prefix}:{1000 + i}" for i in range(entity_count)]
        feature_names_by_view[view_name] = [col.get("name") for col in view.get("columns", [])]

    metrics = MetricsCollector()
    metrics.start()

    feature_samples = []
    first_request = True

    try:
        end_time = time.time() + duration_secs
        request_count = 0

        with ThreadPoolExecutor(max_workers=10) as executor:
            while time.time() < end_time:
                start_latency = time.perf_counter()

                futures = []

                for view in views:
                    view_name = view.get("name", "")
                    entity_ids = entity_ids_by_view[view_name]
                    feature_names = feature_names_by_view[view_name]

                    future = executor.submit(
                        connector.get_features_for_view,
                        entity_ids,
                        view_name,
                        feature_names,
                    )
                    futures.append((view_name, feature_names, future))

                # Collect samples only on first request
                if first_request:
                    for view_name, feature_names, future in futures:
                        results = future.result()
                        for entity_id, features in results.items():
                            row = {"entity_id": entity_id, **features}
                            feature_samples.append(row)
                    first_request = False
                else:
                    for view_name, feature_names, future in futures:
                        future.result()

                latency_ms = (time.perf_counter() - start_latency) * 1000
                metrics.record_latency(latency_ms)

                # Sample resource usage every 10th request
                if request_count % 10 == 0:
                    metrics.update_resource_usage()

                request_count += 1

    finally:
        metrics.end()
        connector.close()

    return metrics.to_dict(), feature_samples, views


def main():
    parser = argparse.ArgumentParser(
        description="Direct backend access benchmark (application fan-out)"
    )
    parser.add_argument(
        "--scenario",
        default="redis-postgres",
        choices=["redis-postgres", "redis-postgres-s3"],
        help="Benchmark scenario",
    )
    parser.add_argument(
        "--entity-count",
        type=int,
        default=100,
        help="Number of entities per request",
    )
    parser.add_argument(
        "--duration-secs",
        type=int,
        default=10,
        help="Benchmark duration in seconds",
    )

    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("BASELINE: DIRECT BACKEND ACCESS (Application Fan-Out)")
    print("=" * 70)
    print(f"Scenario: {args.scenario}")
    print(f"Entity count: {args.entity_count}")
    print(f"Duration: {args.duration_secs}s")
    print("\nRunning benchmark...\n")

    metrics, feature_samples, views = benchmark_scenario(
        entity_count=args.entity_count,
        duration_secs=args.duration_secs,
    )

    print("Benchmark Results:")
    print(f"  P50 Latency:   {metrics['latency_p50_ms']:.2f} ms")
    print(f"  P95 Latency:   {metrics['latency_p95_ms']:.2f} ms")
    print(f"  P99 Latency:   {metrics['latency_p99_ms']:.2f} ms")
    print(f"  Throughput:    {metrics['throughput_rps']:.0f} req/s")
    print(f"  CPU Peak:      {metrics['peak_cpu_percent']:.1f}%")
    print(f"  Memory Peak:   {metrics['peak_memory_mb']:.0f} MB")
    print(f"  Requests:      {metrics['request_count']}")

    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(exist_ok=True)

    timestamp = int(time.time())

    json_file = results_dir / f"baseline_app_{args.scenario}_{timestamp}.json"
    with open(json_file, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"JSON saved to: {json_file}")

    if feature_samples:
        csv_file = results_dir / f"baseline_app_{args.scenario}.csv"

        # Collect all actual fields from samples (to handle timestamp, etc)
        all_fields = set()
        for sample in feature_samples:
            all_fields.update(sample.keys())

        # Add source tracking by determining which backend provided each feature
        for sample in feature_samples:
            sources = set()
            for view in views:
                view_name = view.get("name", "")
                for col in view.get("columns", []):
                    feature_name = col.get("name")
                    if feature_name in sample:
                        source = col.get("source", "unknown")
                        sources.add(source)
            sample["source"] = ",".join(sorted(sources)) if sources else "none"

        # Build fieldnames: entity_id first, then source, then all other fields
        fieldnames = ["entity_id", "source"] + sorted([f for f in all_fields if f not in ("entity_id", "source")])
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, restval="")
            writer.writeheader()
            writer.writerows(feature_samples)
        print(f"CSV saved to: {csv_file} ({len(feature_samples)} entities)")


if __name__ == "__main__":
    main()
