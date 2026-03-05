"""Environment detection for benchmark reporting"""

import platform
import psutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Optional


@dataclass
class EnvironmentInfo:
    """System environment information for benchmark reports"""

    os: str
    cpu: str
    cpu_cores: int
    ram_gb: int
    python_version: str
    redis_version: Optional[str] = None
    quiver_version: Optional[str] = None


def get_cpu_info() -> str:
    """Get detailed CPU information"""
    try:
        if platform.system() == "Darwin":
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        elif platform.system() == "Linux":
            try:
                with open("/proc/cpuinfo", "r") as f:
                    for line in f:
                        if line.startswith("model name"):
                            return line.split(":")[1].strip()
            except FileNotFoundError:
                pass

        processor = platform.processor()
        if processor:
            return processor

        return f"{platform.machine()} processor"

    except (subprocess.SubprocessError, subprocess.TimeoutExpired):
        return f"{platform.machine()} processor"


def get_redis_version(host: str = "localhost", port: int = 6379) -> Optional[str]:
    """Get Redis server version"""
    try:
        import redis  # noqa: F401

        client = redis.Redis(host=host, port=port, socket_connect_timeout=2)
        info = client.info()
        return info.get("redis_version")
    except Exception:  # noqa
        return None


def get_quiver_version() -> Optional[str]:
    """Get Quiver version if available"""
    try:
        import quiver

        return getattr(quiver, "__version__", "0.1.0")
    except ImportError:
        return None


def detect_environment(
    redis_host: str = "localhost", redis_port: int = 6379
) -> EnvironmentInfo:
    """Detect and return system environment information"""

    cpu_info = get_cpu_info()
    physical_cores = psutil.cpu_count(logical=False)
    logical_cores = psutil.cpu_count()
    cpu_cores = physical_cores if physical_cores is not None else (logical_cores or 1)
    ram_gb = round(psutil.virtual_memory().total / (1024**3))

    python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    redis_version = get_redis_version(redis_host, redis_port)
    quiver_version = get_quiver_version()

    return EnvironmentInfo(
        os=f"{platform.system()} {platform.release()}",
        cpu=cpu_info,
        cpu_cores=cpu_cores,
        ram_gb=ram_gb,
        python_version=python_version,
        redis_version=redis_version,
        quiver_version=quiver_version,
    )


def print_environment_info(env: EnvironmentInfo) -> None:
    """Print environment information in a readable format"""
    print("Environment Information:")
    print(f"  OS: {env.os}")
    print(f"  CPU: {env.cpu}")
    print(f"  CPU Cores: {env.cpu_cores}")
    print(f"  RAM: {env.ram_gb}GB")
    print(f"  Python: {env.python_version}")
    if env.redis_version:
        print(f"  Redis: {env.redis_version}")
    if env.quiver_version:
        print(f"  Quiver: {env.quiver_version}")
    print()
