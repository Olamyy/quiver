#!/usr/bin/env python3
"""
Ingest test data for benchmarks.

Generates and injects test data into Redis and PostgreSQL backends.
Supports configurable entity count and feature count.

Usage:
  uv run ingest.py postgres --entity-count 100 --feature-count 4
  uv run ingest.py redis --entity-count 1000
  uv run ingest.py both --entity-count 100 --feature-count 10

Options:
  --entity-count N    Number of entities per scenario (default: 3)
  --feature-count N   Number of features per view (default: 4)
  --scenario NAME     Only ingest specific scenario (default: all)
"""

import argparse
import io
import json
import os
from pathlib import Path
from typing import Dict, Any, List

import boto3
import psycopg2
import redis
import pandas as pd


def load_fixtures() -> Dict[str, Any]:
    """Load base fixture definitions from local directory."""
    # fixtures.json is in benchmarks/ directory (same directory as ingest.py)
    fixtures_path = Path(__file__).parent / "fixtures.json"

    if not fixtures_path.exists():
        raise FileNotFoundError(
            f"Fixtures file not found at {fixtures_path}\n"
            f"Make sure fixtures.json exists in benchmarks/ directory"
        )

    with open(fixtures_path) as f:
        return json.load(f)


def select_features(
    schema: Dict[str, str],
    feature_count: int,
) -> Dict[str, str]:
    """
    Select N features from schema.

    If feature_count < len(schema), select first N.
    If feature_count > len(schema), extend with synthetic features.
    """
    feature_names = list(schema.keys())

    if feature_count <= len(feature_names):
        return {k: schema[k] for k in feature_names[:feature_count]}

    selected = dict(schema)
    col_num = len(feature_names) + 1

    while len(selected) < feature_count:
        feature_type = ["float64", "int64", "bool", "string"][col_num % 4]
        selected[f"feature_{col_num}"] = feature_type
        col_num += 1

    return selected


def generate_fixtures(
    entity_count: int,
    feature_count: int,
) -> Dict[str, Any]:
    """
    Generate test fixtures with N entities and M features.

    Returns dict mapping scenario_name -> fixture with:
    - table name
    - entity type
    - schema (with selected features)
    - records (N entities x M features)
    """
    base_fixtures = load_fixtures()
    generated = {}

    for scenario_name, scenario_def in base_fixtures.items():
        selected_schema = select_features(scenario_def["schema"], feature_count)

        generated[scenario_name] = {
            "table": scenario_def["table"],
            "entity_type": scenario_def["entity_type"],
            "entity_key": scenario_def["entity_key"],
            "schema": selected_schema,
            "records": [],
        }

        for i in range(entity_count):
            entity_type = scenario_def["entity_type"]
            base_record = scenario_def["records"][i % len(scenario_def["records"])]

            new_record = {
                "entity": f"{entity_type}:{1000 + i}",
                "timestamp": base_record["timestamp"],
            }

            for col in selected_schema.keys():
                if col in base_record:
                    new_record[col] = base_record[col]
                else:
                    col_type = selected_schema[col]
                    if col_type == "float64":
                        new_record[col] = 42.0 + i * 0.1
                    elif col_type == "int64":
                        new_record[col] = 100 + i
                    elif col_type == "bool":
                        new_record[col] = i % 2 == 0
                    else:
                        new_record[col] = f"value_{i}"

            generated[scenario_name]["records"].append(new_record)

    return generated


def ingest_postgres(
    fixtures: Dict[str, Any],
    host: str = "127.0.0.1",
    user: str = "quiver",
    password: str = "quiver_test",
    database: str = "quiver_test",
):
    """Ingest data into PostgreSQL."""
    conn = psycopg2.connect(host=host, user=user, password=password, database=database)
    cursor = conn.cursor()
    total_records = 0
    total_tables = 0

    postgres_fixtures = {k: v for k, v in fixtures.items() if k.startswith("postgres")}

    for scenario_name, scenario_def in postgres_fixtures.items():
        table_name = scenario_def["table"]
        schema = scenario_def["schema"]

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        col_defs = ["entity TEXT", "timestamp TEXT", "feature_ts TIMESTAMP DEFAULT NOW()"]
        col_defs += [f"{col} TEXT" for col in schema.keys()]
        cursor.execute(f"CREATE TABLE {table_name} ({', '.join(col_defs)})")

        for record in scenario_def["records"]:
            values = [record["entity"], record["timestamp"]] + [
                str(record.get(col, "")) for col in schema.keys()
            ]
            placeholders = ", ".join(["%s"] * len(values))
            cursor.execute(
                f"INSERT INTO {table_name} (entity, timestamp, {', '.join(schema.keys())}) VALUES ({placeholders})",
                values,
            )
            total_records += 1

        total_tables += 1

    conn.commit()
    conn.close()

    print(f"✓ PostgreSQL: {total_records} records across {total_tables} tables")


def ingest_s3(
    fixtures: Dict[str, Any],
    bucket: str = "quiver-benchmark",
):
    """Ingest data into S3 as Parquet files."""
    s3 = boto3.client("s3", region_name="eu-central-1")
    total_records = 0

    s3_fixtures = {k: v for k, v in fixtures.items() if k == "s3" or k.startswith("s3_")}

    for scenario_name, scenario_def in s3_fixtures.items():
        records = []

        for record in scenario_def["records"]:
            new_record = {"entity": record["entity"], "timestamp": record["timestamp"]}
            for col in scenario_def["schema"].keys():
                new_record[col] = record.get(col)
            records.append(new_record)

        df = pd.DataFrame(records)

        parquet_key = f"features/{scenario_name}.parquet"
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)


        try:
            s3.put_object(
                Bucket=bucket,
                Key=parquet_key,
                Body=parquet_buffer.getvalue(),
                ContentType="application/octet-stream",
            )
            total_records += len(df)
        except Exception as e:
            print(f"Failed to upload {parquet_key} to S3: {e}")
            raise

    print(f"✓ S3: {total_records} records across {len(s3_fixtures)} scenarios")

def ingest_redis(
    fixtures: Dict[str, Any],
    host: str = "127.0.0.1",
    port: int = 6379,
):
    """Ingest data into Redis."""
    r = redis.Redis(host=host, port=port, decode_responses=True)
    r.flushdb()
    total_records = 0

    redis_fixtures = {k: v for k, v in fixtures.items() if k == "redis" or k.startswith("redis_")}

    for scenario_name, scenario_def in redis_fixtures.items():
        for record in scenario_def["records"]:
            entity = record["entity"]
            for col in scenario_def["schema"].keys():
                r.hset(entity, col, str(record.get(col, "")))
            total_records += 1

    print(f"✓ Redis: {total_records} entities")


def main():
    parser = argparse.ArgumentParser(
        description="Ingest test data for benchmarks"
    )
    parser.add_argument(
        "engine",
        choices=["postgres", "redis", "s3", "all"],
        help="Target backend(s)",
    )
    parser.add_argument(
        "--entity-count",
        type=int,
        default=3,
        help="Number of entities per scenario (default: 3)",
    )
    parser.add_argument(
        "--feature-count",
        type=int,
        default=4,
        help="Number of features per view (default: 4, all available)",
    )
    parser.add_argument(
        "--scenario",
        help="Only ingest specific scenario (optional)",
    )

    args = parser.parse_args()

    print(
        f"\nIngesting test data: {args.entity_count} entities per scenario"
    )
    print("-" * 70)

    base_fixtures = load_fixtures()
    fixtures = {}

    for scenario_name, scenario_def in base_fixtures.items():
        fixtures[scenario_name] = {
            "table": scenario_def["table"],
            "entity_type": scenario_def["entity_type"],
            "entity_key": scenario_def["entity_key"],
            "schema": scenario_def["schema"],
            "records": [],
        }

        for i in range(args.entity_count):
            entity_type = scenario_def["entity_type"]
            base_record = scenario_def["records"][i % len(scenario_def["records"])]

            new_record = {
                "entity": f"{entity_type}:{1000 + i}",
                "timestamp": base_record["timestamp"],
            }

            for col in scenario_def["schema"].keys():
                if col in base_record:
                    new_record[col] = base_record[col]
                else:
                    col_type = scenario_def["schema"][col]
                    if col_type == "float64":
                        new_record[col] = 42.0 + i * 0.1
                    elif col_type == "int64":
                        new_record[col] = 100 + i
                    elif col_type == "bool":
                        new_record[col] = i % 2 == 0
                    else:
                        new_record[col] = f"value_{i}"

            fixtures[scenario_name]["records"].append(new_record)

    if args.scenario:
        if args.scenario in fixtures:
            fixtures = {args.scenario: fixtures[args.scenario]}
        else:
            print(f"Unknown scenario: {args.scenario}")
            return

    try:
        if args.engine in ["postgres", "all"]:
            ingest_postgres(fixtures)

        if args.engine in ["redis", "all"]:
            ingest_redis(fixtures)

        if args.engine in ["s3", "all"]:
            ingest_s3(fixtures)

        print("-" * 70)
        print(
            f"✓ Data ingestion complete: {len(fixtures)} scenarios, "
            f"{args.entity_count} entities each"
        )

    except Exception as e:
        print(f"✗ Error during ingestion: {e}")
        raise


if __name__ == "__main__":
    main()
