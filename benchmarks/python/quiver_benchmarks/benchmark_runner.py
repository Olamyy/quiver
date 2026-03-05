"""Main benchmark runner and orchestrator"""

import asyncio
import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from .config import BenchmarkConfig, BenchmarkMethod, SCENARIOS
from .environment import detect_environment, print_environment_info
from .models import BenchmarkResults, PerformanceMetrics, DataPoint
from .clients.direct_redis import (
    DirectRedisBinaryClient,
    DirectRedisJSONClient,
    benchmark_redis_client,
)
from .clients.quiver_client import (
    QuiverBenchmarkClient,
    benchmark_quiver_client,
    QUIVER_AVAILABLE,
)
from .clients.http_client import HTTPBenchmarkClient, benchmark_http_client


app = typer.Typer(help="Quiver Python Benchmark Suite")
console = Console()


class BenchmarkRunner:
    """Main benchmark orchestration class"""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.environment = detect_environment(config.redis_host, config.redis_port)

    async def validate_infrastructure(self) -> None:
        """Validate all required infrastructure is available before running benchmarks"""
        console.print("Validating infrastructure...", style="blue")

        failures = []
        methods = self.config.methods or []

        redis_methods = {
            BenchmarkMethod.DIRECT_REDIS_BINARY,
            BenchmarkMethod.DIRECT_REDIS_JSON,
            BenchmarkMethod.QUIVER_CLIENT,
        }

        if any(method in redis_methods for method in methods):
            try:
                import redis.asyncio as redis

                client = redis.Redis(
                    host=self.config.redis_host,
                    port=self.config.redis_port,
                    socket_connect_timeout=2,
                    socket_timeout=2,
                    decode_responses=False,
                )
                _ = await client.ping()
                await client.aclose()
                console.print(
                    f"✓ Redis connection successful at {self.config.redis_host}:{self.config.redis_port}",
                    style="green",
                )
            except Exception as e:
                failures.append(
                    f"Redis connection failed at {self.config.redis_host}:{self.config.redis_port}: {e}"
                )

        if BenchmarkMethod.QUIVER_CLIENT in methods:
            try:
                if not QUIVER_AVAILABLE:
                    raise Exception("Quiver client package not available")

                import grpc

                channel = grpc.aio.insecure_channel(
                    f"{self.config.quiver_host}:{self.config.quiver_port}"
                )
                try:
                    await asyncio.wait_for(channel.channel_ready(), timeout=3.0)
                    await channel.close()
                    console.print(
                        f"✓ Quiver server connection successful at {self.config.quiver_host}:{self.config.quiver_port}",
                        style="green",
                    )
                except asyncio.TimeoutError:
                    raise Exception("Connection timeout - server not responding")
                except Exception as e:
                    raise Exception(f"gRPC connection failed: {e}")

            except Exception as e:
                failures.append(
                    f"Quiver server connection failed at {self.config.quiver_host}:{self.config.quiver_port}: {e}"
                )

        if BenchmarkMethod.HTTP_API in methods:
            try:
                import httpx

                async with httpx.AsyncClient(timeout=3.0) as client:
                    response = await client.get(
                        f"http://{self.config.http_host}:{self.config.http_port}/health"
                    )
                    if response.status_code != 200:
                        raise Exception(
                            f"Health check failed with status {response.status_code}"
                        )
                console.print(
                    f"✓ HTTP server connection successful at {self.config.http_host}:{self.config.http_port}",
                    style="green",
                )
            except Exception as e:
                failures.append(
                    f"HTTP server connection failed at {self.config.http_host}:{self.config.http_port}: {e}"
                )

        if failures:
            console.print("\nInfrastructure validation failed:", style="red bold")
            for failure in failures:
                console.print(f"  • {failure}", style="red")
            console.print("\nPlease start required services:", style="yellow")
            console.print("  • Redis: brew services start redis", style="yellow")
            console.print("  • Quiver: make run", style="yellow")
            console.print("  • HTTP API: make bench-py-http", style="yellow")
            raise Exception("Required infrastructure not available")

        console.print("✓ All infrastructure validation passed", style="green bold")
        console.print()

    async def setup_test_data(self) -> float:
        """Setup test data in Redis for benchmarking"""
        console.print("Setting up test data...", style="blue")
        start_time = time.time()

        try:
            import redis.asyncio as redis

            client = redis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                decode_responses=False,
            )

            pipe = client.pipeline()

            feature_names = [
                f"feature_{i}" for i in range(self.config.features_per_request)
            ]
            entity_ids = [f"entity_{i}" for i in range(self.config.num_entities)]

            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
            ) as progress:
                task = progress.add_task(
                    "Loading data",
                    total=self.config.num_entities * self.config.features_per_request,
                )

                for entity_id in entity_ids:
                    for feature_name in feature_names:
                        value = random.uniform(-10.0, 10.0)
                        binary_key = f"quiver:f:{feature_name}:e:{entity_id}"
                        pipe.set(binary_key, str(value))
                        json_key = f"feature:{feature_name}:entity:{entity_id}"
                        pipe.set(json_key, json.dumps(value))

                        progress.advance(task, 1)

                        if len(pipe.command_stack) >= 1000:
                            await pipe.execute()
                            pipe = client.pipeline()

                if len(pipe.command_stack) > 0:
                    await pipe.execute()

            await client.aclose()

            setup_time = time.time() - start_time
            console.print(
                f"Test data setup completed in {setup_time:.2f} seconds", style="green"
            )
            return setup_time

        except Exception as e:
            console.print(f"Failed to setup test data: {e}", style="red")
            raise

    def generate_request_entities(self) -> List[str]:
        """Generate entity IDs for a single request"""
        entities_per_request = min(10, self.config.num_entities)
        max_entity_id = self.config.num_entities - 1

        # Use random.sample to ensure unique entities (no duplicates)
        entity_indices = random.sample(range(max_entity_id + 1), entities_per_request)
        return [f"entity_{i}" for i in entity_indices]

    def generate_request_features(self) -> List[str]:
        """Generate feature names for a single request"""
        return [f"feature_{i}" for i in range(self.config.features_per_request)]

    async def run_method_benchmark(self, method: BenchmarkMethod) -> PerformanceMetrics:
        """Run benchmark for a specific method"""
        console.print(f"Benchmarking {method.value}...", style="blue")

        entity_ids = self.generate_request_entities()
        feature_names = self.generate_request_features()

        client = None
        data_points = []

        try:
            if method == BenchmarkMethod.DIRECT_REDIS_BINARY:
                client = DirectRedisBinaryClient(
                    self.config.redis_host, self.config.redis_port
                )
                if not await client.ping():
                    raise Exception("Failed to connect to Redis")
                data_points = await benchmark_redis_client(
                    client, entity_ids, feature_names, self.config.duration_seconds
                )

            elif method == BenchmarkMethod.DIRECT_REDIS_JSON:
                client = DirectRedisJSONClient(
                    self.config.redis_host, self.config.redis_port
                )
                if not await client.ping():
                    raise Exception("Failed to connect to Redis")
                data_points = await benchmark_redis_client(
                    client, entity_ids, feature_names, self.config.duration_seconds
                )

            elif method == BenchmarkMethod.QUIVER_CLIENT:
                if not QUIVER_AVAILABLE:
                    raise Exception("Quiver client not available")
                client = QuiverBenchmarkClient(
                    self.config.quiver_host, self.config.quiver_port
                )
                if not await client.connect():
                    raise Exception("Failed to connect to Quiver server")
                data_points = await benchmark_quiver_client(
                    client, entity_ids, feature_names, self.config.duration_seconds
                )

            elif method == BenchmarkMethod.HTTP_API:
                client = HTTPBenchmarkClient(
                    f"http://{self.config.http_host}:{self.config.http_port}"
                )
                if not await client.ping():
                    raise Exception("Failed to connect to HTTP server")
                data_points = await benchmark_http_client(
                    client, entity_ids, feature_names, self.config.duration_seconds
                )

            return self.calculate_metrics(method, data_points)

        finally:
            if client and hasattr(client, "close"):
                await client.close()

    @staticmethod
    def calculate_metrics(
        method: BenchmarkMethod, data_points: List[DataPoint]
    ) -> PerformanceMetrics:
        """Calculate performance metrics from data points"""
        if not data_points:
            return PerformanceMetrics(
                method=method,
                total_requests=0,
                successful_requests=0,
                failed_requests=0,
                avg_latency_ms=0.0,
                p50_latency_ms=0.0,
                p95_latency_ms=0.0,
                p99_latency_ms=0.0,
                min_latency_ms=0.0,
                max_latency_ms=0.0,
                throughput_rps=0.0,
                success_rate=0.0,
                avg_cpu_percent=0.0,
                max_cpu_percent=0.0,
                avg_memory_mb=0.0,
                max_memory_mb=0.0,
                actual_duration_seconds=0.0,
                error_types={},
            )

        total_requests = len(data_points)
        successful_requests = sum(1 for dp in data_points if dp.success)
        failed_requests = total_requests - successful_requests

        latencies = [dp.latency_ms for dp in data_points]
        latencies.sort()

        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)

        def percentile(data: List[float], p: float) -> float:
            if not data:
                return 0.0
            idx = int(len(data) * p / 100)
            return data[min(idx, len(data) - 1)]

        p50_latency = percentile(latencies, 50)
        p95_latency = percentile(latencies, 95)
        p99_latency = percentile(latencies, 99)

        if data_points:
            duration = data_points[-1].timestamp - data_points[0].timestamp
            throughput = successful_requests / duration if duration > 0 else 0
        else:
            duration = 0
            throughput = 0

        success_rate = (
            (successful_requests / total_requests) * 100 if total_requests > 0 else 0
        )

        error_types = {}
        for dp in data_points:
            if not dp.success and dp.error_type:
                error_types[dp.error_type] = error_types.get(dp.error_type, 0) + 1

        return PerformanceMetrics(
            method=method,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            avg_latency_ms=avg_latency,
            p50_latency_ms=p50_latency,
            p95_latency_ms=p95_latency,
            p99_latency_ms=p99_latency,
            min_latency_ms=min_latency,
            max_latency_ms=max_latency,
            throughput_rps=throughput,
            success_rate=success_rate,
            avg_cpu_percent=0.0,  # TODO: Implement resource monitoring
            max_cpu_percent=0.0,
            avg_memory_mb=0.0,
            max_memory_mb=0.0,
            actual_duration_seconds=duration,
            error_types=error_types,
        )

    async def run_all_benchmarks(self) -> BenchmarkResults:
        """Run benchmarks for all configured methods"""
        console.print("Starting Quiver Python Benchmark Suite", style="bold blue")
        console.print()

        print_environment_info(self.environment)

        start_time = time.time()

        await self.validate_infrastructure()

        data_setup_time = await self.setup_test_data()

        metrics = {}

        for method in self.config.methods or []:
            try:
                method_metrics = await self.run_method_benchmark(method)
                metrics[method] = method_metrics
                self.print_method_results(method_metrics)
            except Exception as e:
                console.print(f"Failed to benchmark {method.value}: {e}", style="red")
                continue

        total_duration = time.time() - start_time

        results = BenchmarkResults(
            config=self.config,
            environment=self.environment,
            timestamp=datetime.now(),
            metrics=metrics,
            total_test_duration_seconds=total_duration,
            data_setup_time_seconds=data_setup_time,
        )

        return results

    @staticmethod
    def print_method_results(metrics: PerformanceMetrics):
        """Print results for a single method"""
        table = Table(title=f"Results: {metrics.method.value}")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")

        table.add_row("Total Requests", str(metrics.total_requests))
        table.add_row("Successful", str(metrics.successful_requests))
        table.add_row("Failed", str(metrics.failed_requests))
        table.add_row("Success Rate", f"{metrics.success_rate:.1f}%")
        table.add_row("Avg Latency", f"{metrics.avg_latency_ms:.2f} ms")
        table.add_row("P50 Latency", f"{metrics.p50_latency_ms:.2f} ms")
        table.add_row("P95 Latency", f"{metrics.p95_latency_ms:.2f} ms")
        table.add_row("P99 Latency", f"{metrics.p99_latency_ms:.2f} ms")
        table.add_row("Throughput", f"{metrics.throughput_rps:.1f} rps")

        if metrics.error_types:
            table.add_row("Errors", str(metrics.error_types))

        console.print(table)
        console.print()


@app.command()
def run(
    scenario: str = typer.Option("standard", help="Benchmark scenario to run"),
    duration: Optional[int] = typer.Option(
        None, help="Override test duration in seconds"
    ),
    methods: Optional[str] = typer.Option(
        None, help="Comma-separated list of methods to test"
    ),
    output: Optional[str] = typer.Option(None, help="Output file for results (JSON)"),
):
    """Run benchmark suite"""

    if scenario in SCENARIOS:
        config = SCENARIOS[scenario]
    else:
        console.print(f"Unknown scenario: {scenario}", style="red")
        console.print(f"Available scenarios: {list(SCENARIOS.keys())}")
        return

    if duration:
        config.duration_seconds = duration

    if methods:
        try:
            method_list = [BenchmarkMethod(m.strip()) for m in methods.split(",")]
            config.methods = method_list
        except ValueError as e:
            console.print(f"Invalid method: {e}", style="red")
            return

    async def run_benchmarks():
        runner = BenchmarkRunner(config)
        results = await runner.run_all_benchmarks()

        if len(results.metrics) > 1:
            print_comparison_table(results)

        if output:
            save_results(results, output)

        return results

    asyncio.run(run_benchmarks())


def print_comparison_table(results: BenchmarkResults):
    """Print comparison table for all methods"""
    table = Table(title="Performance Comparison")
    table.add_column("Method", style="cyan")
    table.add_column("Avg Latency (ms)", style="green")
    table.add_column("P95 Latency (ms)", style="green")
    table.add_column("Throughput (rps)", style="green")
    table.add_column("Success Rate (%)", style="green")

    for method, metrics in results.metrics.items():
        table.add_row(
            method.value,
            f"{metrics.avg_latency_ms:.2f}",
            f"{metrics.p95_latency_ms:.2f}",
            f"{metrics.throughput_rps:.1f}",
            f"{metrics.success_rate:.1f}",
        )

    console.print(table)


def save_results(results: BenchmarkResults, output_path: str):
    """Save benchmark results to file"""
    try:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        data = {
            "timestamp": results.timestamp.isoformat(),
            "config": {
                "duration_seconds": results.config.duration_seconds,
                "concurrent_requests": results.config.concurrent_requests,
                "num_entities": results.config.num_entities,
                "features_per_request": results.config.features_per_request,
            },
            "environment": {
                "os": results.environment.os,
                "cpu": results.environment.cpu,
                "cpu_cores": results.environment.cpu_cores,
                "ram_gb": results.environment.ram_gb,
                "python_version": results.environment.python_version,
                "redis_version": results.environment.redis_version,
                "quiver_version": results.environment.quiver_version,
            },
            "metrics": {
                method.value: {
                    "avg_latency_ms": metrics.avg_latency_ms,
                    "p95_latency_ms": metrics.p95_latency_ms,
                    "throughput_rps": metrics.throughput_rps,
                    "success_rate": metrics.success_rate,
                    "total_requests": metrics.total_requests,
                    "error_types": metrics.error_types,
                }
                for method, metrics in results.metrics.items()
            },
        }

        with open(output_path, "w") as f:
            json.dump(data, f, indent=2)

        console.print(f"Results saved to {output_path}", style="green")

    except Exception as e:
        console.print(f"Failed to save results: {e}", style="red")


@app.command()
def list_scenarios():
    """List available benchmark scenarios"""
    table = Table(title="Available Scenarios")
    table.add_column("Name", style="cyan")
    table.add_column("Duration", style="green")
    table.add_column("Requests", style="green")
    table.add_column("Entities", style="green")
    table.add_column("Features", style="green")

    for name, config in SCENARIOS.items():
        table.add_row(
            name,
            f"{config.duration_seconds}s",
            str(config.concurrent_requests),
            str(config.num_entities),
            str(config.features_per_request),
        )

    console.print(table)


if __name__ == "__main__":
    app()
