"""Type definitions and aliases for Quiver Python client."""

from typing import Literal
import sys

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

# Core type aliases for clarity
EntityId: TypeAlias = str
FeatureName: TypeAlias = str
FeatureViewName: TypeAlias = str
NullStrategy: TypeAlias = Literal["error", "fill_null", "skip_row"]

# Export common types
__all__ = [
    "EntityId",
    "FeatureName", 
    "FeatureViewName",
    "NullStrategy",
]
