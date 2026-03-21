# Kubernetes Sidecar

Quiver is designed to run as a sidecar alongside your model server or inference API. This gives you sub-millisecond feature retrieval over localhost gRPC with no network hop.

## Pod configuration

```yaml
spec:
  containers:
    - name: model-server
      image: your-model-server:latest
      # model server talks to localhost:8815

    - name: quiver
      image: ghcr.io/olamyy/quiver:latest
      env:
        - name: QUIVER_CONFIG
          value: /etc/quiver/config.yaml
        - name: RUST_LOG
          value: info
      ports:
        - containerPort: 8815
          name: arrow-flight
      readinessProbe:
        tcpSocket:
          port: 8815
        initialDelaySeconds: 5
        periodSeconds: 10
      resources:
        requests:
          cpu: 100m
          memory: 256Mi
        limits:
          memory: 256Mi
      volumeMounts:
        - name: quiver-config
          mountPath: /etc/quiver
          readOnly: true

  volumes:
    - name: quiver-config
      configMap:
        name: quiver-config
```

## ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: quiver-config
data:
  config.yaml: |
    server:
      host: "[::]"    # dual-stack — required on IPv6-only clusters
      port: 8815
      timeout_seconds: 30

    registry:
      type: static
      views:
        - name: user_features
          entity_type: user
          entity_key: user_id
          columns:
            - name: score
              arrow_type: float64
              nullable: true
              source: redis

    adapters:
      redis:
        type: redis
        connection: redis://redis-service:6379
        source_path: "features:{feature}"
```

!!! warning "IPv6 clusters"
    Set `host: "[::]"` (not `0.0.0.0`) if your cluster assigns IPv6 pod IPs. Using `0.0.0.0` binds IPv4 only and the readiness probe will fail.

## Calling Quiver from the sidecar

From within the same pod, connect to `localhost:8815`:

```python
from quiver import Client

client = Client("localhost:8815")
table = client.get_features("user_features", ["user:1000"], ["score"])
```

No service discovery or DNS needed — sidecar containers share the same network namespace.

## Resources

Quiver is lightweight. Starting points:

| Scenario | CPU request | Memory limit |
|----------|-------------|--------------|
| Memory adapter only | 50m | 128Mi |
| Redis / Postgres | 100m | 256Mi |
| S3 / heavy fanout | 200m | 512Mi |
