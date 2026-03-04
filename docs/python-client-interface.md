# Quiver Python Client Interface Proposal

## Design Principles

1. **Pythonic**: Feels natural to Python/ML engineers
2. **Simple**: Common cases should be one-liners
3. **Powerful**: Advanced cases should be possible
4. **Arrow-native**: Leverage PyArrow for zero-copy performance
5. **ML-friendly**: Integrate well with pandas, numpy, sklearn workflows

## Core Interface

```python
import quiver
import pandas as pd
import pyarrow as pa

# Connect to Quiver server
client = quiver.Client("localhost:8815")

# Simple feature lookup
features = client.get_features(
    feature_view="user_features",
    entities=["user_123", "user_456"],
    features=["age", "tier", "total_orders"]
)
# Returns: pyarrow.Table

# Convert to pandas for ML workflows
df = features.to_pandas()
```

## API Classes

```python
class Client:
    def __init__(self, address: str, timeout: float = 30.0, max_retries: int = 3, 
                 compression: Optional[str] = None, default_null_strategy: str = "fill_null")
    def get_features(self, feature_view: str, entities: List[str], features: List[str]) -> pa.Table
    def get_features_batch(self, requests: List[FeatureRequest]) -> List[pa.Table]
    def list_feature_views(self) -> List[FeatureViewInfo]
    def close(self)

@dataclass 
class FeatureRequest:
    feature_view: str
    entities: List[str] 
    features: List[str]
    as_of: Optional[datetime] = None

@dataclass
class FeatureViewInfo:
    name: str
    schema: pa.Schema
    backend: str
    entity_types: List[str]
```

## Error Handling

```python
try:
    features = client.get_features("user_features", ["user_123"], ["age"])
except quiver.FeatureViewNotFound:
    # Handle missing feature view
except quiver.FeatureNotFound as e:
    # Handle missing features: e.missing_features 
except quiver.ConnectionError:
    # Handle server connection issues
```

## Key Design Decisions

1. **Arrow-first**: Return `pyarrow.Table` by default for zero-copy performance
2. **Entity lists**: Most common case is list of string entity IDs
3. **Simple defaults**: Common parameters have sensible defaults  
4. **No global config**: All configuration through Client constructor

This interface focuses on the 80% use case (simple feature lookup) while supporting advanced scenarios ML teams need in production.
