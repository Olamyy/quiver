#!/usr/bin/env python3
"""
Ingest test data into feature store engines.

Usage:
  uv run ingest.py postgres
  uv run ingest.py redis
  uv run ingest.py clickhouse
"""

import argparse
import json
from pathlib import Path
import psycopg2
import redis
from clickhouse_driver import Client as ClickHouseClient


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
    conn.close()
    print(f"PostgreSQL: {total} records inserted")


def ingest_redis(fixtures):
    """Ingest data into Redis."""
    r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)
    r.flushdb()
    total = 0

    for scenario_name, scenario_def in fixtures.items():
        for record in scenario_def["records"]:
            entity = record["entity"]
            for col in scenario_def["schema"].keys():
                key = f"features:{entity}:feature:{col}"
                r.set(key, str(record.get(col, "")))
                total += 1

    print(f"Redis: {total} records inserted")


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


def main():
    parser = argparse.ArgumentParser(description="Ingest test data into feature store")
    parser.add_argument("engine", choices=["postgres", "redis", "clickhouse"], help="Target adapter")
    args = parser.parse_args()

    fixtures = load_fixtures()

    try:
        if args.engine == "postgres":
            ingest_postgres(fixtures)
        elif args.engine == "redis":
            ingest_redis(fixtures)
        elif args.engine == "clickhouse":
            ingest_clickhouse(fixtures)
    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
