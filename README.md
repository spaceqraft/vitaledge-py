# vitaledge-py

Python client library for [VitalEdge](https://github.com/paegun/vitaledge), a graph database with a Cypher-compatible query interface over gRPC.

## Requirements

- Python 3.10+
- A running VitalEdge server (default: `localhost:7443`)

## Installation

```bash
# Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install the library
pip install -e .
```

## Quick Example

```python
from vitaledge import VitalEdgeClient

with VitalEdgeClient(host="localhost", port=7443, tenant="default") as client:
    # Check what the server supports
    caps = client.get_capabilities()
    print(caps.protocol_version)

    # Run a Cypher query
    result = client.execute("MATCH (n) RETURN n LIMIT 10", include_stats=True)
    print(result.columns)
    for row in result.rows:
        print(row)
    print(result.stats)

    # Get a query execution plan without running it
    plan = client.explain("MATCH (n)-[r]->(m) RETURN n, r, m")
    import json
    print(json.loads(plan.explain_json))
```

See [examples/basic_usage.py](examples/basic_usage.py) for a runnable version.

## Advanced Example: VitalEdge Cyber Threat Detection

An advanced threat-detection example is provided in
[examples/advanced_cyber_threat_detection.py](examples/advanced_cyber_threat_detection.py).

This example performs detection with VitalEdge graph analytics (Cypher queries)
using traffic features only. The dataset columns `Attack_Type` and `Label` are
held out from detection and used only for post-hoc evaluation and comparison.

Dataset:
- https://www.kaggle.com/datasets/hussainsheikh03/cyber-threat-detection

Why this example is useful:
- Models network traffic as a graph (`Host` -> `Flow` -> `Host`) and runs graph-native hunting queries
- Computes protocol-relative anomaly scores directly in Cypher
- Uses held-out labels only for post-hoc evaluation (prevents label leakage)
- Demonstrates parameterized queries and consolidated analytics calls

Run:

```bash
python examples/advanced_cyber_threat_detection.py \
    --csv /path/to/cyberfeddefender_dataset.csv \
    --host localhost --port 7443 --tenant default
```

In order to obtain the dataset file, refer to the corresponding kaggle:
https://www.kaggle.com/datasets/hussainsheikh03/cyber-threat-detection

```bash
python examples/advanced_cyber_threat_detection.py \
    --csv examples/cyberfeddefender_dataset.csv \
    --host localhost --port 7443 --tenant default
```

Optional controls:
- `--threshold` adjusts sensitivity for `detected_malicious` classification
- `--batch-size` controls ingest write size
- `--limit` controls printed result rows per query

Expected output sections:
- `Top Suspicious Sources`
- `Possible Lateral Movement`
- `Destination Concentration`
- `Evaluation vs Held-Out Labels`
- `Attack-Type Comparison (Post-Hoc Only)`

## Parameterized Query Example

```python
from vitaledge import VitalEdgeClient

query = """
MATCH (:Movie {title: $movieTitle})<-[r:ACTED_IN]-(p:Person)
WHERE r.role CONTAINS $actorRole
RETURN p.name AS actor, r.role AS role
"""

parameters = {
    "movieTitle": "Wall Street",
    "actorRole": "Fox",
}

with VitalEdgeClient(host="localhost", port=7443, tenant="default") as client:
    result = client.execute(query, parameters=parameters, include_stats=True)
    for row in result.rows:
        print(row["actor"], row["role"])
```

## API

### `VitalEdgeClient(host, port, *, tenant, tls, tls_credentials, channel_options)`

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `"localhost"` | VitalEdge server hostname |
| `port` | `7443` | gRPC port |
| `tenant` | `"default"` | Default tenant for all requests |
| `tls` | `False` | Enable TLS; uses system CA bundle unless `tls_credentials` is provided |
| `tls_credentials` | `None` | Custom `grpc.ChannelCredentials` |
| `channel_options` | `[]` | Extra gRPC channel options |

Can be used as a context manager (`with` statement) or manually via `.connect()` / `.close()`.

### Methods

#### `execute(cypher, *, parameters, tenant, read_only, include_stats, include_warnings, timeout) → QueryResult`

Run a Cypher query. Returns a `QueryResult` with:
- `.columns` — list of column name strings
- `.rows` — list of dicts mapping column name → Python value
- `.stats` — `{"rows_returned": int, "duration_ms": int}`
- `.warnings` — list of `{"code": str, "message": str}` dicts

`parameters` is an optional dict of query parameters. Parameter placeholders in Cypher
must use the `$name` form.

#### `explain(cypher, *, tenant, timeout) → ExplainResult`

Retrieve the query execution plan without running the query. Returns an `ExplainResult` with:
- `.explain_json` — raw JSON bytes from the server
- `.stats`, `.warnings`

#### `get_capabilities(*, timeout) → Capabilities`

Query server capabilities. Returns a `Capabilities` object with:
- `.protocol_version`
- `.parser_versions`
- `.ir_versions`
- `.prepared_query_supported`
- `.parameter_binding`

## Regenerating Proto Stubs

If the VitalEdge proto definitions change, regenerate the Python stubs:

```bash
bash scripts/gen_proto.sh
```

This requires `grpcio-tools` (included in the `dev` extras):

```bash
pip install -e ".[dev]"
```

## Development

```bash
pip install -e ".[dev]"
pytest
```
