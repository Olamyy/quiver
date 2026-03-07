#!/usr/bin/env python3
"""
Ingest test data into feature store engines.

Usage:
  python ingest.py --engine postgres [--seed] [--generate 1000] [--scenario ecommerce]
  python ingest.py --engine redis --seed
  python ingest.py --engine clickhouse --generate 500 --scenario timeseries
"""

import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path
import psycopg2
import redis
from clickhouse_driver import Client as ClickHouseClient


def load_fixtures():
    """Load fixture definitions from fixtures.json."""
    fixtures_path = Path(__file__).parent / "data" / "fixtures.json"
    with open(fixtures_path) as f:
        return json.load(f)


def generate_data(scenario, fixtures, count, seed=42):
    """Generate synthetic data for a scenario."""
    random.seed(seed)

    generated = []

    if scenario == "ecommerce":
        for i in range(count):
            user_id = f"user:{1000 + i}"
            timestamp = datetime.utcnow() - timedelta(days=random.randint(0, 30))
            record = {
                "entity": user_id,
                "timestamp": timestamp.isoformat() + "Z",
                "spend_30d": round(random.uniform(0, 5000), 2),
                "purchase_count": random.randint(0, 100),
                "last_purchase_date": (timestamp - timedelta(days=random.randint(1, 30))).isoformat() + "Z",
                "is_premium": random.choice([True, False])
            }
            generated.append(record)

    elif scenario == "timeseries":
        for i in range(count):
            sensor_id = f"sensor:{100 + (i % 100):03d}"
            timestamp = datetime.utcnow() - timedelta(hours=random.randint(0, 168))
            record = {
                "entity": sensor_id,
                "timestamp": timestamp.isoformat() + "Z",
                "temperature": round(random.uniform(15, 35), 1),
                "humidity": round(random.uniform(20, 80), 1),
                "alert_triggered": random.choice([True, False])
            }
            generated.append(record)

    elif scenario == "sessions":
        for i in range(count):
            session_id = f"session:s{1000 + i:04d}"
            user_id = f"user:{random.randint(1, 1000)}"
            timestamp = datetime.utcnow() - timedelta(minutes=random.randint(0, 60))
            record = {
                "entity": session_id,
                "timestamp": timestamp.isoformat() + "Z",
                "active_user_id": user_id,
                "request_count": random.randint(1, 500),
                "last_activity": timestamp.isoformat() + "Z"
            }
            generated.append(record)

    return generated


def ingest_postgres(fixtures, seed=False, generate=0, scenario=None):
    """Ingest data into PostgreSQL."""
    conn = psycopg2.connect(
        host="127.0.0.1",
        user="quiver",
        password="quiver_test",
        database="quiver_test"
    )
    cursor = conn.cursor()

    scenarios = fixtures["scenarios"]
    if scenario:
        scenarios = {scenario: scenarios[scenario]}

    total_inserted = 0

    for scenario_name, scenario_def in scenarios.items():
        table_name = f"{scenario_name}_data"
        columns = list(scenario_def["schema"].keys())

        col_defs_list = ["entity TEXT", "timestamp TEXT", "feature_ts TIMESTAMP"]
        col_defs_list += [f"{col} TEXT" for col in columns]
        col_defs = ", ".join(col_defs_list)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        cursor.execute(f"CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, {col_defs})")

        if seed:
            for row in scenario_def["seed_data"]:
                values = [row["entity"], row["timestamp"]] + [row.get(col) for col in columns]
                placeholders = ", ".join(["%s"] * len(values))
                # Insert with feature_ts converted from ISO string
                cursor.execute(f"INSERT INTO {table_name} (entity, timestamp, feature_ts, {', '.join(columns)}) VALUES (%s, %s, %s::timestamp, {', '.join(['%s'] * len(columns))})", [row["entity"], row["timestamp"], row["timestamp"]] + [row.get(col) for col in columns])
                total_inserted += 1

        if generate > 0:
            generated = generate_data(scenario_name, fixtures, generate)
            for row in generated:
                # Insert with feature_ts converted from ISO string
                cursor.execute(f"INSERT INTO {table_name} (entity, timestamp, feature_ts, {', '.join(columns)}) VALUES (%s, %s, %s::timestamp, {', '.join(['%s'] * len(columns))})", [row["entity"], row["timestamp"], row["timestamp"]] + [row.get(col) for col in columns])
                total_inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"PostgreSQL: {total_inserted} records inserted")


def ingest_redis(fixtures, seed=False, generate=0, scenario=None):
    """Ingest data into Redis."""
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    scenarios = fixtures["scenarios"]
    if scenario:
        scenarios = {scenario: scenarios[scenario]}

    total_inserted = 0

    for scenario_name, scenario_def in scenarios.items():
        if seed:
            for row in scenario_def["seed_data"]:
                key = f"{scenario_name}:{row['entity']}"
                r.hset(key, mapping={k: str(v) for k, v in row.items() if k != "entity"})
                total_inserted += 1

        if generate > 0:
            generated = generate_data(scenario_name, fixtures, generate)
            for row in generated:
                key = f"{scenario_name}:{row['entity']}"
                r.hset(key, mapping={k: str(v) for k, v in row.items() if k != "entity"})
                total_inserted += 1

    print(f"Redis: {total_inserted} records inserted")


def ingest_clickhouse(fixtures, seed=False, generate=0, scenario=None):
    """Ingest data into ClickHouse."""
    client = ClickHouseClient("localhost", port=9000, database="quiver_test")

    scenarios = fixtures["scenarios"]
    if scenario:
        scenarios = {scenario: scenarios[scenario]}

    total_inserted = 0

    for scenario_name, scenario_def in scenarios.items():
        table_name = f"{scenario_name}_data"
        schema = scenario_def["schema"]

        col_defs = "entity String, timestamp String, " + ", ".join([f"{col} String" for col in schema.keys()])
        client.execute(f"DROP TABLE IF EXISTS {table_name}")
        client.execute(f"CREATE TABLE {table_name} ({col_defs}) ENGINE = MergeTree() ORDER BY (entity, timestamp)")

        if seed:
            rows = []
            for row in scenario_def["seed_data"]:
                values = [row["entity"], row["timestamp"]] + [str(row.get(col, "")) for col in schema.keys()]
                rows.append(values)
            if rows:
                client.execute(f"INSERT INTO {table_name} VALUES", rows)
                total_inserted += len(rows)

        if generate > 0:
            generated = generate_data(scenario_name, fixtures, generate)
            rows = []
            for row in generated:
                values = [row["entity"], row["timestamp"]] + [str(row.get(col, "")) for col in schema.keys()]
                rows.append(values)
            if rows:
                client.execute(f"INSERT INTO {table_name} VALUES", rows)
                total_inserted += len(rows)

    print(f"ClickHouse: {total_inserted} records inserted")


def main():
    parser = argparse.ArgumentParser(description="Ingest test data into feature store")
    parser.add_argument("--engine", required=True, choices=["postgres", "redis", "clickhouse"])
    parser.add_argument("--seed", action="store_true", help="Load seed data from fixtures.json")
    parser.add_argument("--generate", type=int, default=0, help="Generate N random records per scenario")
    parser.add_argument("--scenario", choices=["ecommerce", "timeseries", "sessions"], help="Load specific scenario (default: all)")

    args = parser.parse_args()

    if not args.seed and args.generate == 0:
        parser.error("Must specify --seed and/or --generate")

    fixtures = load_fixtures()

    try:
        if args.engine == "postgres":
            ingest_postgres(fixtures, args.seed, args.generate, args.scenario)
        elif args.engine == "redis":
            ingest_redis(fixtures, args.seed, args.generate, args.scenario)
        elif args.engine == "clickhouse":
            ingest_clickhouse(fixtures, args.seed, args.generate, args.scenario)
    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
