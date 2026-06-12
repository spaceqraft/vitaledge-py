# vitaledge-py

Python client library for [VitalEdge](https://github.com/spaceqraft/vitaledge), a graph database with a Cypher-compatible query interface over gRPC.

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

## Intermediate Example: Graph-Powered Movie Recommendations

An intermediate recommendation example is provided in
[examples/intermediate_movie_recommendation.py](examples/intermediate_movie_recommendation.py).

Demonstrates how a graph naturally models the user → movie → genre relationship
and enables collaborative filtering through graph traversal — no ML framework
required.

Dataset:
- https://www.kaggle.com/datasets/parasharmanas/movie-recommendation-system
- Expected files: `movies.csv` (movieId, title, genres) and `ratings.csv` (userId, movieId, rating, timestamp)
- Title format: `"Movie Title (YYYY)"` — year is parsed automatically

Why this example is useful:
- Models users, movies, and genres as a graph and finds similar users via shared RATED edges
- Applies a **decade-affinity boost**: users who historically rate 1980s films highly see more 1980s candidates — not because those films are old or new, but because that era resonates
- Scores candidates with a Bayesian-weighted popularity score to suppress single-review noise
- Stores top-N recommendations as `RECOMMENDED` relationships so they can be traversed and re-queried like any other graph relationship

Run:

```bash
python examples/intermediate_movie_recommendation.py \
    --movies /path/to/movies.csv \
    --ratings /path/to/ratings.csv \
    --host localhost --port 7443 --tenant default
```

Optional controls:
- `--ratings-limit` caps ratings loaded (useful for development with large datasets)
- `--user-sample` controls how many most-active users receive individual recommendations
- `--batch-size` controls ingest write size
- `--limit` controls Top-N for all output tables

Expected output sections:
- `Top N Overall Movies (Bayesian score)`
- `Top N <Genre> Movies` (one table per genre)
- `Top N Movies from <most recent year>`
- `Top N Recommendations for User <id>` (sample of users)

### Troubleshooting Slow UNWIND + MERGE Ingest

The intermediate example includes two EXPLAIN checks before ingest:
- `EXPLAIN Ingest Plan Check` for the three ingest queries
- `EXPLAIN Lookup Plan Check` for simple property lookups

Use the `EXPLAIN Diagnosis` output to route investigation quickly:

1. Lookup shows index, ingest shows scan:
    planner/index integration gap on MERGE write plans.

2. Lookup shows scan:
    index not used for equality lookup path yet.

3. Both show index:
    slowdown likely in write execution (MERGE bookkeeping, locking, storage).

This is intended as a developer-experience aid when indexes are reported as
created but ingest latency still grows across batches.

---

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
- `.stats` — includes `rows_returned`, `duration_ms`, and write-batch tuning fields:
    `configured_max_write_batch_bytes`, `effective_max_write_batch_bytes`,
    `max_write_batch_bytes_tuned`
- `.max_write_batch_bytes` — convenience accessor that prefers effective tuned
    batch limit from `Execute` response stats
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
- `.index_ddl_supported`
- `.configured_max_write_batch_bytes` (when advertised by the server)
- `.effective_max_write_batch_bytes` (when auto-tuning is active)
- `.max_write_batch_bytes_tuned`
- `.max_write_batch_bytes` (convenience accessor that prefers effective over configured)

#### `create_property_index(*, schema, property, tenant, if_not_exists, timeout) → dict`

Create a server-side property index over gRPC. Returns:
- `{"created": bool, "indexed_entities": int}`

This is intended for MERGE-heavy ingest flows where equality lookups on keys
like `Movie.movie_id` and `User.user_id` need to stay fast as the graph grows.

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
