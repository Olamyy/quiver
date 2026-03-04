"""Type definitions and aliases for Quiver Python client."""

from typing import Literal
import sys

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

EntityId: TypeAlias = str
FeatureName: TypeAlias = str
FeatureViewName: TypeAlias = str
NullStrategy: TypeAlias = Literal["error", "fill_null", "skip_row"]
CompressionType: TypeAlias = Literal["gzip", "lz4", "zstd"]

__all__ = [
    "EntityId",
    "FeatureName",
    "FeatureViewName",
    "NullStrategy",
    "CompressionType",
]
