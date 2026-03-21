# Quiver

[![Rust Build](https://img.shields.io/github/actions/workflow/status/Olamyy/quiver/rust-ci.yml?branch=main&label=build)](https://github.com/Olamyy/quiver/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](https://github.com/Olamyy/quiver/blob/main/LICENSE)
[![GitHub Release](https://img.shields.io/github/v/release/Olamyy/quiver?style=flat&sort=semver&color=blue)](https://github.com/Olamyy/quiver/releases)

**Quiver** is a feature serving system for machine learning inference. It resolves feature requests across different backends in a single gRPC call and returns the results as columnar data via [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) for direct consumption by model inference pipelines.

```python
from quiver import Client

client = Client("localhost:8815")

table = client.get_features(
    feature_view="user_features",
    entities=["user:1000", "user:1001", "user:1002"],
    features=["score", "country", "last_active"],
)

df = table.to_pandas()
```

## Install

```bash
# Python client
pip install quiver-python

# Server (Docker)
docker run -p 8815:8815 -p 8816:8816 \
  -v $(pwd)/config.yaml:/etc/quiver/config.yaml \
  ghcr.io/olamyy/quiver-server:latest
```

## Documentation

See the [docs](/docs) for more details on how to get started, architecture, configuration, and the Python client API.

| | |
|---|---|
| [Quickstart](https://olamyy.github.io/quiver/getting-started/quickstart/) | Step-by-step tutorial with PostgreSQL and Redis |
| [Architecture](https://olamyy.github.io/quiver/concepts/architecture/) | How the serving path works end to end |
| [Configuration](https://olamyy.github.io/quiver/concepts/configuration/) | Full YAML reference with auth and adapter options |
| [Python Client](https://olamyy.github.io/quiver/reference/python-client/) | SDK reference for `Client` and `FeatureTable` |
