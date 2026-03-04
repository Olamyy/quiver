# Quiver Python Client

High-performance Python client for the [Quiver](https://github.com/quiver-ai/quiver) feature serving system. Provides zero-copy Arrow integration for ML inference pipelines that need fast, reliable feature retrieval.

**Key Features:**
- Zero-copy data transfer with Apache Arrow
- Automatic retries and connection management  
- ML framework exports (pandas, PyTorch, TensorFlow, Polars)
- Batch requests with parallel execution
- Point-in-time feature queries


## Prerequisites

- **You need a running Quiver server to use this client.** See the [Quiver documentation](https://github.com/quiver-ai/quiver) for installation and setup instructions.
- Python >=3.10

## Installation

### Core Installation
```bash
uv add quiver-client
# Includes: grpcio-tools, pyarrow
```

### Optional ML Framework Support
```bash
# Individual frameworks
uv add quiver-client[pandas]      # pandas>=1.5.0
uv add quiver-client[numpy]       # numpy>=1.21.0  
uv add quiver-client[torch]       # torch>=1.12.0
uv add quiver-client[tensorflow]  # tensorflow>=2.9.0
uv add quiver-client[polars]      # polars>=0.18.0

# Multiple frameworks
uv add quiver-client[pandas,torch]
```

## Usage Examples

*Note: All examples assume you have a Quiver server running on `localhost:8815`. Adjust the address as needed for your setup.*

### Basic Feature Retrieval

```python
import quiver

# Connect to Quiver server
client = quiver.Client("localhost:8815")

# Get features for entities
features = client.get_features(
    feature_view="user_features",
    entities=["user_123", "user_456"], 
    features=["age", "tier", "total_orders"]
)

# Convert to pandas for ML workflows
df = features.to_pandas()
print(df)
#    entity_id  age tier  total_orders
# 0    user_123   25  gold           42
# 1    user_456   31  silver         18

client.close()
```


### Batch Requests

```python
# Multiple requests in parallel
requests = [
    quiver.FeatureRequest(
        feature_view="user_features", 
        entities=["user_123", "user_456"],
        features=["age", "tier"]
    ),
    quiver.FeatureRequest(
        feature_view="item_features",
        entities=["item_789"], 
        features=["category", "price"]
    )
]

results = client.get_features_batch(requests)
for i, result in enumerate(results):
    print(f"Request {i}: {len(result)} rows")
```

### Exporting to other formats

```python
client = ...
features = client.get_features(
    feature_view="user_features",
    entities=["user_123"],
    features=["age", "tier", "total_orders"]
)

# pandas DataFrame
df = features.to_pandas()

# PyTorch tensor
tensor = features.to_torch(columns=["age", "total_orders"])

# TensorFlow tensor  
tf_tensor = features.to_tensorflow(columns=["age", "total_orders"])

# Polars DataFrame
polars_df = features.to_polars()

# NumPy array
array = features.to_numpy(columns=["age", "total_orders"])
```

### Point-in-Time Queries

```python
from datetime import datetime

client = ...
# Get features as they existed at a specific time
features = client.get_features(
    feature_view="user_features",
    entities=["user_123"],
    features=["age", "tier"],
    as_of=datetime(2024, 1, 15, 10, 30, 0)
)
```

### Context Manager Usage

```python
# Automatic connection cleanup
import quiver
with quiver.Client("localhost:8815") as client:
    features = client.get_features(
        feature_view="user_features",
        entities=["user_123"],
        features=["age", "tier"]
    )
    df = features.to_pandas()
    # Client automatically closed
```

## Configuration

### Client Options

```python
import quiver
client = quiver.Client(
    address="localhost:8815",
    timeout=30.0,                    # Request timeout in seconds
    max_retries=3,                   # Number of retry attempts
    compression="gzip",              # Optional compression (gzip, zstd)
    default_null_strategy="fill_null", # How to handle missing values
    validate_connection=True         # Test connection on startup
)
```

### Advanced Request Options

```python
import quiver
features = client.get_features(
    feature_view="user_features",
    entities=["user_123"],
    features=["age", "tier"],
    include_timestamps=True,         # Include feature timestamps
    include_freshness=True,          # Include freshness metadata  
    null_strategy="error",           # Override default null handling
    context=quiver.RequestContext(   # Request tracing context
        request_id="req-123",
        caller="ml-service",
        environment="production"
    ),
    timeout=60.0                     # Override default timeout
)
```

### List Available Feature Views

```python
# Discover available feature views
client = ...
views = client.list_feature_views()

for view in views:
    print(f"View: {view.name}")
    print(f"Backend: {view.backend}")
    print(f"Schema: {view.schema}")
    print(f"Entity types: {view.entity_types}")
```

## E2E Integration Examples

### Inference Pipeline

```python
import quiver
import torch.nn as nn

class MLModel:
    def __init__(self, quiver_client):
        self.client = quiver_client
        self.model = self.load_model()
    
    def predict(self, entity_ids):
        # Fetch features from Quiver
        features = self.client.get_features(
            feature_view="user_features",
            entities=entity_ids,
            features=["age", "tier_encoded", "total_orders"]
        )
        
        # Convert to PyTorch tensor
        input_tensor = features.to_torch(
            columns=["age", "tier_encoded", "total_orders"]
        )
        
        # Run inference
        with torch.no_grad():
            predictions = self.model(input_tensor)
            
        return predictions.numpy()

# Usage
with quiver.Client("localhost:8815") as client:
    model = MLModel(client)
    predictions = model.predict(["user_123", "user_456"])
```

### Feature Store Enrichment

```python
import quiver
import pandas as pd

def enrich_dataset(entity_ids, feature_views):
    """Enrich a dataset with features from multiple views."""
    
    with quiver.Client("localhost:8815") as client:
        requests = []
        for view_name, features in feature_views.items():
            requests.append(quiver.FeatureRequest(
                feature_view=view_name,
                entities=entity_ids,
                features=features
            ))
        
        results = client.get_features_batch(requests)
        
        combined_df = None
        for result in results:
            df = result.to_pandas()
            if combined_df is None:
                combined_df = df
            else:
                combined_df = combined_df.merge(df, on="entity_id", how="outer")
                
        return combined_df

enriched_data = enrich_dataset(
    entity_ids=["user_123", "user_456", "user_789"],
    feature_views={
        "user_features": ["age", "tier", "total_orders"],
        "user_preferences": ["category_affinity", "price_sensitivity"],
        "user_behavior": ["last_login", "session_count"]
    }
)
```

## Error Handling

### Exception Types

```python
import quiver

try:
    client = quiver.Client("localhost:8815")
    features = client.get_features("user_features", ["user_123"], ["age"])
    
except quiver.QuiverConnectionError:
    print("Cannot connect to Quiver server")
    
except quiver.QuiverFeatureViewNotFound:
    print("Feature view 'user_features' does not exist")
    
except quiver.QuiverFeatureNotFound as e:
    print(f"Features not found: {e.missing_features}")
    
except quiver.QuiverTimeoutError:
    print("Request timed out")
    
finally:
    client.close()
```

### Common Issues

| Exception | Cause | Solution |
|-----------|-------|----------|
| `QuiverConnectionError` | Cannot reach server | Check server address and network |
| `QuiverFeatureViewNotFound` | Feature view doesn't exist | Verify feature view name |
| `QuiverFeatureNotFound` | One or more features missing | Check available features |
| `QuiverValidationError` | Invalid request parameters | Validate input data |
| `QuiverTimeoutError` | Request took too long | Increase timeout or reduce batch size |
| `QuiverServerError` | Server-side error | Check server logs |

### Troubleshooting

```python
# Test basic connectivity
try:
    client = quiver.Client("localhost:8815", validate_connection=True)
    print("Connection successful")
except quiver.QuiverConnectionError as e:
    print(f"Connection failed: {e}")

# Check available features
views = client.list_feature_views()
for view in views:
    print(f"{view.name}: {view.schema.names}")

# Performance tuning for high-throughput
client = quiver.Client(
    address="localhost:8815",
    timeout=120.0,        # Longer timeout for large batches
    max_retries=5,        # More retries for reliability
    compression="zstd"    # Better compression for large responses
)
```