import argparse
import csv
import json
import time
from pathlib import Path
from typing import Dict, List, Any
import sys

from quiver import Client
from scenarios import (
    get_scenario_config,
    MetricsCollector,
)


class QuiverBenchmark:
    """Benchmark using Quiver client."""

    def __init__(self, server_addr: str = "localhost:8815"):
        try:
            self.client = Client(server_addr)
            print(f"✓ Connected to Quiver server at {server_addr}")
        except Exception as e:
            print(f"Failed to connect to Quiver server: {e}")
            raise

    def get_server_config(self) -> Dict[str, Any]:
        """Get server configuration."""
        try:
            return self.client.get_server_info()
        except Exception as e:
            print(f"Failed to get server info: {e}")
            return {}

    def get_features(
        self,
        feature_view: str,
        entity_ids: List[str],
        feature_names: List[str],
    ) -> Dict[str, Any]:
        """Fetch features via Quiver."""
        try:
            result = self.client.get_features(
                feature_view=feature_view,
                entities=entity_ids,
                features=feature_names,
            )
            return result
        except Exception as e:
            print(f"Failed to get features from Quiver: {e}")
            return {}


async def benchmark_scenario(
    scenario_name: str,
    entity_count: int = 100,
    duration_secs: int = 10,
    server_addr: str = "localhost:8815",
) -> tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict]]:
    """Run benchmark for a scenario.

    Returns (metrics, feature_samples, views) where feature_samples is a list of
    dicts containing extracted features and views is the config from server.
    """

    config = get_scenario_config(scenario_name, entity_count)

    benchmark = QuiverBenchmark(server_addr)

    server_info = benchmark.get_server_config()
    if not server_info:
        raise RuntimeError("Could not connect to Quiver server")

    views = server_info.get("registry", {}).get("views", [])
    if not views:
        raise RuntimeError("No feature views configured on server")

    view_configs = {}
    for view_name in config.feature_views:
        view_config = next((v for v in views if v.get("name") == view_name), None)
        if not view_config:
            raise RuntimeError(f"View {view_name} not found on server")
        feature_names = [col.get("name") for col in view_config.get("columns", [])]
        if not feature_names:
            raise RuntimeError(f"No features defined for view {view_name}")
        view_configs[view_name] = {
            "entity_key": view_config.get("entity_key", "entity"),
            "features": feature_names,
        }

    entity_prefix_map = {
        "ecommerce_view": "user",
        "timeseries_view": "sensor",
    }

    print(f"  Testing views: {config.feature_views}")
    for view_name, view_info in view_configs.items():
        print(f"    - {view_name}: {view_info['features']}")
    print(f"  Entities: {entity_count}")

    entity_ids_by_view = {}
    for view in views:
        view_name = view.get("name", "")
        prefix = entity_prefix_map.get(view_name, "entity")
        entity_ids_by_view[view_name] = [f"{prefix}:{1000 + i}" for i in range(entity_count)]

    feature_names_by_view = {}
    for view in views:
        view_name = view.get("name", "")
        feature_names_by_view[view_name] = [col.get("name") for col in view.get("columns", [])]

    metrics = MetricsCollector()
    metrics.start()

    feature_samples = []
    first_request = True

    try:
        request_count = 0
        end_time = time.time() + duration_secs

        while time.time() < end_time:
            start_latency = time.perf_counter()

            if first_request:
                for view in views:
                    view_name = view.get("name", "")
                    entity_ids = entity_ids_by_view[view_name]
                    feature_names = feature_names_by_view[view_name]

                    result_table = benchmark.client.get_features(
                        feature_view=view_name,
                        entities=entity_ids,
                        features=feature_names,
                    )

                    df = result_table.to_pandas()
                    if "entity" in df.columns:
                        for _, row in df.iterrows():
                            row_dict = {"entity_id": row["entity"], **{k: v for k, v in row.items() if k != "entity"}}
                            feature_samples.append(row_dict)
                    else:
                        for idx, row in df.iterrows():
                            entity_ids = entity_ids_by_view[view_name]
                            row_dict = {"entity_id": entity_ids[idx], **row.to_dict()}
                            feature_samples.append(row_dict)

                first_request = False
            else:
                for view in views:
                    view_name = view.get("name", "")
                    entity_ids = entity_ids_by_view[view_name]
                    feature_names = feature_names_by_view[view_name]

                    benchmark.client.get_features(
                        feature_view=view_name,
                        entities=entity_ids,
                        features=feature_names,
                    )

            latency_ms = (time.perf_counter() - start_latency) * 1000
            metrics.record_latency(latency_ms)

            if request_count % 10 == 0:
                metrics.update_resource_usage()

            request_count += 1

    except Exception as e:
        print(f"Benchmark error: {e}")
    finally:
        metrics.end()

    return metrics.to_dict(), feature_samples, views


def main():
    parser = argparse.ArgumentParser(description="Quiver client benchmark")
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
    parser.add_argument(
        "--server",
        default="localhost:8815",
        help="Quiver server address (default: localhost:8815)",
    )

    args = parser.parse_args()

    print("\n" + "=" * 70)
    print("QUIVER SERVER BENCHMARK")
    print("=" * 70)
    print(f"Scenario: {args.scenario}")
    print(f"Entity count: {args.entity_count}")
    print(f"Duration: {args.duration_secs}s")
    print(f"Server: {args.server}")
    print("\nRunning benchmark...\n")

    try:
        import asyncio

        metrics, feature_samples, views = asyncio.run(
            benchmark_scenario(
                args.scenario,
                entity_count=args.entity_count,
                duration_secs=args.duration_secs,
                server_addr=args.server,
            )
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

        json_file = results_dir / f"baseline_quiver_{args.scenario}_{timestamp}.json"
        with open(json_file, "w") as f:
            json.dump(metrics, f, indent=2)
        print(f"JSON saved to: {json_file}")

        csv_file = results_dir / f"baseline_quiver_{args.scenario}.csv"

        all_fields = set()
        for sample in feature_samples:
            all_fields.update(sample.keys())

        for sample in feature_samples:
            sources = set()
            for view in views:
                for col in view.get("columns", []):
                    feature_name = col.get("name")
                    if feature_name in sample:
                        source = col.get("source", "unknown")
                        sources.add(source)
            sample["source"] = ",".join(sorted(sources)) if sources else "none"

        fieldnames = ["entity_id", "source"] + sorted([f for f in all_fields if f not in ("entity_id", "source")])
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, restval="")
            writer.writeheader()
            writer.writerows(feature_samples)
        print(f"CSV saved to: {csv_file} ({len(feature_samples)} entities)")

    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        print("\nMake sure:")
        print("  1. Quiver server is running")
        print("  2. Backends (Redis, PostgreSQL) are running")
        print("  3. Data has been ingested: cd examples && uv run ingest.py postgres")
        sys.exit(1)


if __name__ == "__main__":
    main()
