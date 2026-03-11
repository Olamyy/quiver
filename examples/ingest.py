#!/usr/bin/env python3
"""
Ingest test data into feature store engines.

Usage:
  uv run ingest.py postgres
  uv run ingest.py postgres --count 100
  uv run ingest.py redis --count 1000
  uv run ingest.py clickhouse
  uv run ingest.py s3

Options:
  --count N    Number of entities to generate (default: 3)
               Used for benchmarking: --count 100 or --count 10000
"""

import argparse
import json
from pathlib import Path
import psycopg2
import redis
from clickhouse_driver import Client as ClickHouseClient
import pandas as pd
import pyarrow.parquet as pq
import boto3


def load_fixtures():
    """Load fixture definitions."""
    fixtures_path = Path(__file__).parent / "data" / "fixtures.json"
    with open(fixtures_path) as f:
        return json.load(f)


def ingest_postgres(fixtures):
    """Ingest data into PostgreSQL."""
    conn = psycopg2.connect(
        host="127.0.0.1",
        user="quiver",
        password="quiver_test",
        database="quiver_test"
    )
    cursor = conn.cursor()
    total = 0

    for scenario_name, scenario_def in fixtures.items():
        table_name = scenario_def["table"]
        schema = scenario_def["schema"]

        # Drop and recreate table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        col_defs = ["entity TEXT", "timestamp TEXT", "feature_ts TIMESTAMP DEFAULT NOW()"]
        col_defs += [f"{col} TEXT" for col in schema.keys()]
        cursor.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")

        # Insert records
        for record in scenario_def["records"]:
            values = [record["entity"], record["timestamp"]] + [str(record.get(col, "")) for col in schema.keys()]
            placeholders = ", ".join(["%s"] * len(values))
            cursor.execute(f"INSERT INTO {table_name} (entity, timestamp, {', '.join(schema.keys())}) VALUES ({placeholders})", values)
            total += 1

    conn.commit()

    # Verify insertion
    cursor.execute("SELECT COUNT(*) FROM ecommerce_data UNION ALL SELECT COUNT(*) FROM timeseries_data UNION ALL SELECT COUNT(*) FROM sessions_data")
    counts = cursor.fetchall()
    actual_total = sum(c[0] for c in counts)
    conn.close()

    print(f"PostgreSQL: {total} records inserted")
    print(f"PostgreSQL: {actual_total} entities in store")


def ingest_redis(fixtures):
    """Ingest data into Redis."""
    r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
    r.flushdb()
    total = 0

    for scenario_name, scenario_def in fixtures.items():
        for record in scenario_def["records"]:
            entity = record["entity"]
            for col in scenario_def["schema"].keys():
                r.hset(entity, col, str(record.get(col, "")))
            total += 1

    print(f"Redis: {total} records inserted")

    # Verify insertion
    count = r.dbsize()
    print(f"Redis: {count} entities in store")


def ingest_clickhouse(fixtures):
    """Ingest data into ClickHouse."""
    client = ClickHouseClient("localhost", port=9000, database="quiver_test")
    total = 0

    for scenario_name, scenario_def in fixtures.items():
        table_name = scenario_def["table"]
        schema = scenario_def["schema"]

        # Drop and recreate table
        col_defs = "entity String, timestamp String, " + ", ".join([f"{col} String" for col in schema.keys()])
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
        client.execute(f"CREATE TABLE {table_name} ({col_defs}) ENGINE = MergeTree() ORDER BY (entity, timestamp)")

        # Insert records
        rows = []
        for record in scenario_def["records"]:
            values = [record["entity"], record["timestamp"]] + [str(record.get(col, "")) for col in schema.keys()]
            rows.append(values)
            total += 1

        if rows:
            client.execute(f"INSERT INTO {table_name} VALUES", rows)

    print(f"ClickHouse: {total} records inserted")


def ingest_s3(fixtures):
    """Ingest data into S3 or local filesystem as Parquet files."""
    import os

    use_s3 = os.getenv("USE_S3", "false").lower() == "true"

    if use_s3:
        bucket = os.getenv("AWS_BUCKET", "airflow-ml-platform-test")
        region = os.getenv("AWS_REGION", "eu-central-1")
        s3 = boto3.client("s3", region_name=region)
        storage_uri = f"s3://{bucket}/features"
    else:
        local_path = "/tmp/quiver_test_data/features"
        os.makedirs(local_path, exist_ok=True)
        storage_uri = local_path

    total = 0

    for scenario_name, scenario_def in fixtures.items():
        schema = scenario_def["schema"]
        records = scenario_def["records"]

        df_data = {
            "entity": [r["entity"] for r in records],
            "timestamp": [r["timestamp"] for r in records],
        }
        for col in schema.keys():
            df_data[col] = [r.get(col, "") for r in records]

        df = pd.DataFrame(df_data)

        for col in schema.keys():
            col_df = df[["entity", "timestamp", col]].copy()
            col_df.columns = ["entity", "timestamp", "value"]

            if use_s3:
                s3_key = f"features/{col}.parquet"
                parquet_bytes = col_df.to_parquet(index=False)
                s3.put_object(Bucket=bucket, Key=s3_key, Body=parquet_bytes)
            else:
                file_path = os.path.join(storage_uri, f"{col}.parquet")
                col_df.to_parquet(file_path, index=False)

            total += 1
            print(f"  Written {col}.parquet")

    print(f"S3/Parquet: {total} feature files written to {storage_uri}/")


def generate_fixtures(count):
    """Generate fixtures with N entities per scenario."""
    base_fixtures = load_fixtures()
    generated = {}

    for scenario_name, scenario_def in base_fixtures.items():
        generated[scenario_name] = {
            "table": scenario_def["table"],
            "entity_type": scenario_def["entity_type"],
            "entity_key": scenario_def["entity_key"],
            "schema": scenario_def["schema"],
            "records": []
        }

        # Generate N records, cycling through entity IDs
        for i in range(count):
            entity_type = scenario_def["entity_type"]
            base_record = scenario_def["records"][i % len(scenario_def["records"])]

            new_record = {
                "entity": f"{entity_type}:{1000 + i}",
                "timestamp": base_record["timestamp"]
            }
            for col in scenario_def["schema"].keys():
                new_record[col] = base_record.get(col, "")

            generated[scenario_name]["records"].append(new_record)

    return generated


def main():
    parser = argparse.ArgumentParser(description="Ingest test data into feature store")
    parser.add_argument("engine", choices=["postgres", "redis", "clickhouse", "s3"], help="Target adapter")
    parser.add_argument("--count", type=int, default=3, help="Number of entities per scenario to generate (default: 3)")
    args = parser.parse_args()

    fixtures = generate_fixtures(args.count)

    try:
        if args.engine == "postgres":
            ingest_postgres(fixtures)
        elif args.engine == "redis":
            ingest_redis(fixtures)
        elif args.engine == "clickhouse":
            ingest_clickhouse(fixtures)
        elif args.engine == "s3":
            ingest_s3(fixtures)
    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
