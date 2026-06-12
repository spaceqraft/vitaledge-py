"""Intermediate movie recommendation using VitalEdge graph analytics.

WARNING: large dataset, uses server-side hints for batch sizes but takes a long
time to load the data since this is frankly a stress tester for data set size.

To explore, reduce the size of the dataset by using a subset of the kaggle data
files.

Builds a graph of users, movies, and genres from standard MovieLens-format CSVs,
then generates recommendations via graph-native collaborative filtering enhanced
with decade-aware user preference weighting.

Graph model:
  (:Movie {movie_id, title, year, avg_rating, num_ratings, base_score})
  (:Genre {genre})
  (:User  {user_id})
  (:Movie)-[:GENRED]->(:Genre)
  (:User)-[:RATED   {rating, ts}]->(:Movie)
  (:User)-[:RECOMMENDED {score, rank}]->(:Movie)   ← written during recommendation

Recommendation approach:
  1. Score each movie with a Bayesian-weighted popularity score to reduce bias
     towards low-count movies with a single 5-star rating.
  2. For each user, traverse the graph to find peer users who rated the same films
     similarly (collaborative filtering via shared RATED edges).
  3. Collect movies rated highly by peers that the target user has not yet seen.
  4. In Python, apply a decade-affinity boost: users often prefer films from a
     particular era — a reflection of nostalgia, production style, or life stage
     when they first discovered a genre — not simply "newer is better".
  5. Store top-N recommendations as RECOMMENDED relationships in the graph so
     they can be queried like any other graph relationship.

Why Python orchestrates the per-user scoring step:
  Computing a multi-dimensional score that combines collaborative graph signals,
  per-user decade affinity, and global movie quality is more legible and flexible
  in Python than in a single deeply nested Cypher query. The graph handles the
  expensive traversal and aggregation; Python handles the final ranking logic.

Dataset:
  https://www.kaggle.com/datasets/parasharmanas/movie-recommendation-system
  Expected files:
    movies.csv  — movieId, title (e.g. "Toy Story (1995)"), genres (pipe-separated)
    ratings.csv — userId, movieId, rating, timestamp

Usage:
    python examples/intermediate_movie_recommendation.py \\
        --movies /path/to/movies.csv \\
        --ratings /path/to/ratings.csv \\
        --host localhost --port 7443 --tenant movierec

For large datasets use --ratings-limit to cap rows loaded during development:
    python examples/intermediate_movie_recommendation.py \\
        --movies movies.csv --ratings ratings.csv --ratings-limit 200000
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import grpc
from dataclasses import dataclass, field
from pathlib import Path
from math import log
from time import perf_counter
from typing import Any, Callable, Iterable

from vitaledge import VitalEdgeClient


# ── Data model ────────────────────────────────────────────────────────────────

@dataclass
class MovieRecord:
    movie_id: int
    title: str
    year: int
    genres: list[str] = field(default_factory=list)


@dataclass
class RatingRecord:
    user_id: int
    movie_id: int
    rating: float
    ts: int


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VitalEdge movie recommendation example")
    parser.add_argument("--movies", type=Path, required=True, help="Path to movies.csv")
    parser.add_argument("--ratings", type=Path, required=True, help="Path to ratings.csv")
    parser.add_argument("--host", default="localhost", help="VitalEdge host")
    parser.add_argument("--port", type=int, default=7443, help="VitalEdge gRPC port")
    parser.add_argument("--tenant", default="movierec", help="VitalEdge tenant")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Node/relationship ingest batch size",
    )
    parser.add_argument(
        "--edge-batch-size",
        type=int,
        default=5000,
        help="GENRED relationship ingest batch size",
    )
    parser.add_argument(
        "--ratings-limit",
        type=int,
        default=0,
        help="Cap on ratings rows loaded (0 = all); useful for large datasets",
    )
    parser.add_argument(
        "--user-sample",
        type=int,
        default=50,
        help="Number of most-active users to generate individual recommendations for",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Top-N results per output table",
    )
    return parser.parse_args()


# ── Parsing ───────────────────────────────────────────────────────────────────

_YEAR_RE = re.compile(r"\((\d{4})\)\s*$")


def _parse_year(title: str) -> tuple[str, int]:
    """Return (clean_title, year) extracted from titles like 'Toy Story (1995)'."""
    m = _YEAR_RE.search(title)
    if m:
        return title[: m.start()].strip(), int(m.group(1))
    return title.strip(), 0


def load_movies(path: Path) -> list[MovieRecord]:
    records: list[MovieRecord] = []
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            movie_id = int(row.get("movieId") or row.get("movie_id") or 0)
            if not movie_id:
                continue
            raw_title = (row.get("title") or "").strip()
            title, year = _parse_year(raw_title)
            genres_raw = row.get("genres") or ""
            genres = [
                g.strip()
                for g in genres_raw.split("|")
                if g.strip() and g.strip().lower() != "(no genres listed)"
            ]
            records.append(MovieRecord(movie_id=movie_id, title=title, year=year, genres=genres))
    return records


def load_ratings(path: Path, limit: int = 0) -> list[RatingRecord]:
    records: list[RatingRecord] = []
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            user_id = int(row.get("userId") or row.get("user_id") or 0)
            movie_id = int(row.get("movieId") or row.get("movie_id") or 0)
            if not user_id or not movie_id:
                continue
            rating = float(row.get("rating") or 0)
            ts = int(row.get("timestamp") or row.get("ts") or 0)
            records.append(RatingRecord(user_id=user_id, movie_id=movie_id, rating=rating, ts=ts))
            if limit and len(records) >= limit:
                break
    return records


# ── Helpers ───────────────────────────────────────────────────────────────────

def _chunks(items: list, size: int) -> Iterable[list]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def print_rows(title: str, rows: list[dict]) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("  (no results)")
        return
    for row in rows:
        print(" ", row)


def _payload_size_bytes(payload: object) -> int:
    return len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def _extract_max_write_batch_bytes(stats: dict[str, Any]) -> int | None:
    for key in (
        "effective_max_write_batch_bytes",
        "configured_max_write_batch_bytes",
        "max_write_batch_bytes",
    ):
        value = stats.get(key)
        if isinstance(value, int) and value > 0:
            return value
    return None


def _run_batched_query(
    client: VitalEdgeClient,
    query: str,
    parameters: dict,
    label: str,
    batch_index: int,
    batch_total: int | None = None,
) -> tuple[float, int | None, bool]:
    payload_rows = 0
    for value in parameters.values():
        if isinstance(value, list):
            payload_rows = len(value)
            break

    query_chars = len(query.strip())
    payload_bytes = _payload_size_bytes(parameters)
    started_at = perf_counter()
    result = client.execute(query, parameters=parameters, include_stats=True)
    elapsed = perf_counter() - started_at
    tuned_max_write_batch_bytes = _extract_max_write_batch_bytes(result.stats)
    max_write_batch_bytes_tuned = bool(result.stats.get("max_write_batch_bytes_tuned", False))

    batch_suffix = f"/{batch_total}" if batch_total is not None else ""
    tuned_suffix = (
        f" tuned_max_write_batch_bytes={tuned_max_write_batch_bytes}"
        if tuned_max_write_batch_bytes is not None
        else ""
    )

    print(
        f"  [{label}] batch {batch_index}{batch_suffix}: "
        f"rows={payload_rows} query_chars={query_chars} payload_bytes={payload_bytes} "
        f"elapsed={elapsed:.2f}s"
        f"{tuned_suffix} max_write_batch_bytes_tuned={max_write_batch_bytes_tuned}"
    )
    return elapsed, tuned_max_write_batch_bytes, max_write_batch_bytes_tuned


def _run_adaptive_batched_pass(
    client: VitalEdgeClient,
    *,
    items: Iterable[Any],
    query: str,
    parameter_name: str,
    row_to_payload: Callable[[Any], dict],
    row_limit: int,
    max_write_batch_bytes: int | None,
    label: str,
) -> dict:
    rows = list(items)
    total_rows = len(rows)
    if total_rows == 0:
        return {
            "rows": 0,
            "batches": 0,
            "elapsed_s": 0.0,
            "max_write_batch_bytes": max_write_batch_bytes,
        }

    current_max_write_batch_bytes = max_write_batch_bytes
    current_row_limit = max(1, row_limit)
    payload_envelope_bytes = (
        _payload_size_bytes({parameter_name: []})
        if current_max_write_batch_bytes is not None
        else 0
    )

    elapsed_s = 0.0
    batches = 0
    index = 0
    while index < total_rows:
        batch: list[dict] = []
        batch_payload_bytes = payload_envelope_bytes

        while index < total_rows and len(batch) < current_row_limit:
            payload_row = row_to_payload(rows[index])
            row_payload_bytes = (
                _payload_size_bytes(payload_row)
                if current_max_write_batch_bytes is not None
                else 0
            )
            candidate_payload_bytes = (
                batch_payload_bytes
                + row_payload_bytes
                + (1 if batch else 0)  # comma between array elements
            )

            if (
                batch
                and current_max_write_batch_bytes is not None
                and candidate_payload_bytes > current_max_write_batch_bytes
            ):
                break

            batch.append(payload_row)
            batch_payload_bytes = candidate_payload_bytes
            index += 1

            # If a single row is larger than the tuned byte ceiling, send it
            # alone to preserve forward progress and let the server enforce
            # its final write policy.
            if (
                len(batch) == 1
                and current_max_write_batch_bytes is not None
                and row_payload_bytes > current_max_write_batch_bytes
            ):
                break

        batches += 1
        batch_elapsed_s, observed_max_write_batch_bytes, _ = _run_batched_query(
            client,
            query,
            parameters={parameter_name: batch},
            label=label,
            batch_index=batches,
        )
        elapsed_s += batch_elapsed_s

        if (
            observed_max_write_batch_bytes is not None
            and observed_max_write_batch_bytes != current_max_write_batch_bytes
        ):
            current_max_write_batch_bytes = observed_max_write_batch_bytes
            payload_envelope_bytes = _payload_size_bytes({parameter_name: []})
            print(
                f"    [{label}] updated client write limit to "
                f"{current_max_write_batch_bytes} bytes"
            )

    return {
        "rows": total_rows,
        "batches": batches,
        "elapsed_s": elapsed_s,
        "max_write_batch_bytes": current_max_write_batch_bytes,
    }


def _ingest_queries() -> dict[str, str]:
    return {
        "movie_node_query": """
    UNWIND $movies AS m
    CREATE (:Movie {movie_id: m.movie_id, title: m.title, year: m.year})
    """,
        "genre_edge_query": """
    UNWIND $pairs AS p
    MATCH (mov:Movie {movie_id: p.movie_id})
    MATCH (g:Genre {genre: p.genre})
    CREATE (mov)-[:GENRED]->(g)
    """,
        "genre_node_query": """
    UNWIND $genres AS g
    CREATE (:Genre {genre: g.genre})
    """,
        "user_node_query": """
    UNWIND $users AS u
    CREATE (:User {user_id: u.user_id})
    """,
        "rating_query": """
    UNWIND $ratings AS r
    MATCH (u:User {user_id: r.user_id})
    MATCH (mov:Movie {movie_id: r.movie_id})
    CREATE (u)-[:RATED {rating: r.rating, ts: r.ts}]->(mov)
    """,
    }


# ── Graph operations ──────────────────────────────────────────────────────────

def reset_graph(client: VitalEdgeClient) -> None:
    client.execute("MATCH (n:Movie|Genre|User) DETACH DELETE n")


def ensure_ingest_indexes(client: VitalEdgeClient) -> dict:
    """Create lookup indexes used by MERGE-heavy ingest passes.

    With server-side index DDL enabled over gRPC, creating these indexes before
    loading avoids label scans on each MERGE and keeps batch latency from
    growing with graph size.
    """
    started_at = perf_counter()
    caps = client.get_capabilities()
    summary = {
        "supported": caps.index_ddl_supported,
        "attempted": 0,
        "created": 0,
        "existing": 0,
        "failed": 0,
        "elapsed_s": 0.0,
    }
    if not caps.index_ddl_supported:
        print("  Index DDL not supported by this server; continuing without index creation")
        summary["elapsed_s"] = perf_counter() - started_at
        return summary

    specs = [
        ("Movie", "movie_id"),
        ("User", "user_id"),
        ("Genre", "genre"),
    ]
    for schema, property_name in specs:
        summary["attempted"] += 1
        try:
            result = client.create_property_index(
                schema=schema,
                property=property_name,
                if_not_exists=True,
            )
            state = "created" if result["created"] else "already exists"
            if result["created"]:
                summary["created"] += 1
            else:
                summary["existing"] += 1
            print(
                "  Index "
                f"{schema}.{property_name}: {state} "
                f"(indexed_entities={result['indexed_entities']})"
            )
        except grpc.RpcError as exc:
            summary["failed"] += 1
            # If one index fails, ingest can still proceed; report and continue.
            print(f"  Index {schema}.{property_name}: failed ({exc.code().name}: {exc.details()})")

    summary["elapsed_s"] = perf_counter() - started_at
    return summary


def ingest_graph(
    client: VitalEdgeClient,
    movies: list[MovieRecord],
    ratings: list[RatingRecord],
    batch_size: int,
    edge_batch_size: int,
    max_write_batch_bytes: int | None,
) -> dict:
    """Ingest movies, genres, users, and ratings in batched passes.

    Pass 1  — Genre nodes (unique values only, MERGE).
    Pass 2  — Movie nodes + GENRED edges (CREATE only — graph is always reset
              before ingest so no existence check is needed, eliminating the
              O(n) label scan that MERGE performs on every row).
    Pass 3a — User nodes (deduplicated, CREATE only).
    Pass 3b — RATED edges (MATCH user+movie by index, CREATE edge).

    Performance note — MERGE scan cost (May 2026):
      Each MERGE clause performs a property-equality scan over the target label
      to decide whether to match or create.  Without a server-managed index on
      the lookup key (e.g. Movie.movie_id, User.user_id) that scan is O(n) in
      the number of existing nodes, so elapsed time per batch grows noticeably
      as the graph fills.  Two upcoming VitalEdge gRPC features will resolve
      this:

        1. Index DDL via gRPC — CREATE INDEX / DROP INDEX issued over the API
           so clients can assert lookup indexes before ingesting large datasets
           instead of relying on server-side config.

        2. Bulk Load via gRPC — a dedicated streaming load RPC that bypasses
           per-row MERGE overhead for initial population, enabling
           significantly higher throughput for cold-start ingest.

            Until those features ship, reduce batch_size (e.g. --batch-size 100) to
            limit how much growth each batch sees, at the cost of more round-trips.
            When the server advertises MaxWriteBatchBytes, batches are further split
            so the serialized UNWIND payload stays under the write ceiling.
    """
    started_at = perf_counter()
    pass_timings: list[dict] = []
    current_max_write_batch_bytes = max_write_batch_bytes

    queries = _ingest_queries()

    # Pass 1: Genre nodes
    unique_genres = sorted({g for m in movies for g in m.genres})
    genre_node_query = queries["genre_node_query"]
    print(f"  Pass 1/3: genre nodes ({len(unique_genres)} rows)")
    pass_summary = _run_adaptive_batched_pass(
        client,
        items=unique_genres,
        query=genre_node_query,
        parameter_name="genres",
        row_to_payload=lambda genre: {"genre": genre},
        row_limit=batch_size,
        max_write_batch_bytes=current_max_write_batch_bytes,
        label="Pass 1 genre_node_query",
    )
    current_max_write_batch_bytes = pass_summary["max_write_batch_bytes"]
    pass_timings.append(
        {
            "name": "genre_nodes",
            "rows": pass_summary["rows"],
            "batches": pass_summary["batches"],
            "elapsed_s": pass_summary["elapsed_s"],
        }
    )

    # Pass 2a: Movie nodes (pure CREATE — no MATCH, no scan)
    movie_node_query = queries["movie_node_query"]
    print(f"  Pass 2a/3: movie nodes ({len(movies)} rows)")
    pass_summary = _run_adaptive_batched_pass(
        client,
        items=movies,
        query=movie_node_query,
        parameter_name="movies",
        row_to_payload=lambda movie: {
            "movie_id": movie.movie_id,
            "title": movie.title,
            "year": movie.year,
            "genres": movie.genres,
        },
        row_limit=batch_size,
        max_write_batch_bytes=current_max_write_batch_bytes,
        label="Pass 2a movie_node_query",
    )
    current_max_write_batch_bytes = pass_summary["max_write_batch_bytes"]
    pass_timings.append(
        {
            "name": "movie_nodes",
            "rows": pass_summary["rows"],
            "batches": pass_summary["batches"],
            "elapsed_s": pass_summary["elapsed_s"],
        }
    )

    # Pass 2b: GENRED edges (indexed equality MATCH on movie_id + genre)
    genre_edge_query = queries["genre_edge_query"]
    genre_pairs = [
        {"movie_id": m.movie_id, "genre": g}
        for m in movies
        for g in m.genres
    ]
    genre_edge_count = len(genre_pairs)
    print(f"  Pass 2b/3: genre edges ({genre_edge_count} edges)")
    pass_summary = _run_adaptive_batched_pass(
        client,
        items=genre_pairs,
        query=genre_edge_query,
        parameter_name="pairs",
        row_to_payload=lambda pair: pair,
        row_limit=edge_batch_size,
        max_write_batch_bytes=current_max_write_batch_bytes,
        label="Pass 2b genre_edge_query",
    )
    current_max_write_batch_bytes = pass_summary["max_write_batch_bytes"]
    pass_timings.append(
        {
            "name": "genre_edges",
            "rows": pass_summary["rows"],
            "batches": pass_summary["batches"],
            "elapsed_s": pass_summary["elapsed_s"],
        }
    )

    # Pass 3a: User nodes (deduplicated — each user_id created exactly once)
    unique_user_ids = sorted({r.user_id for r in ratings})
    user_node_query = queries["user_node_query"]
    print(f"  Pass 3a/3: user nodes ({len(unique_user_ids)} rows)")
    pass_summary = _run_adaptive_batched_pass(
        client,
        items=unique_user_ids,
        query=user_node_query,
        parameter_name="users",
        row_to_payload=lambda user_id: {"user_id": user_id},
        row_limit=batch_size,
        max_write_batch_bytes=current_max_write_batch_bytes,
        label="Pass 3a user_node_query",
    )
    current_max_write_batch_bytes = pass_summary["max_write_batch_bytes"]
    pass_timings.append(
        {
            "name": "user_nodes",
            "rows": pass_summary["rows"],
            "batches": pass_summary["batches"],
            "elapsed_s": pass_summary["elapsed_s"],
        }
    )

    # Pass 3b: RATED relationships (MATCH user+movie by index, CREATE edge)
    rating_query = queries["rating_query"]
    print(f"  Pass 3b/3: ratings ({len(ratings)} rows)")
    pass_summary = _run_adaptive_batched_pass(
        client,
        items=ratings,
        query=rating_query,
        parameter_name="ratings",
        row_to_payload=lambda rating: {
            "user_id": rating.user_id,
            "movie_id": rating.movie_id,
            "rating": rating.rating,
            "ts": rating.ts,
        },
        row_limit=batch_size,
        max_write_batch_bytes=current_max_write_batch_bytes,
        label="Pass 3b rating_query",
    )
    current_max_write_batch_bytes = pass_summary["max_write_batch_bytes"]
    pass_timings.append(
        {
            "name": "ratings",
            "rows": pass_summary["rows"],
            "batches": pass_summary["batches"],
            "elapsed_s": pass_summary["elapsed_s"],
        }
    )

    return {
        "pass_timings": pass_timings,
        "max_write_batch_bytes": current_max_write_batch_bytes,
        "elapsed_s": perf_counter() - started_at,
    }


def print_ingest_timing_summary(index_summary: dict, ingest_summary: dict) -> None:
    """Print one compact timing block for run-to-run ingest comparisons."""
    print("\n=== Ingest Timing Summary ===")
    if index_summary.get("supported"):
        print(
            "  Index DDL: "
            f"attempted={index_summary['attempted']} "
            f"created={index_summary['created']} "
            f"existing={index_summary['existing']} "
            f"failed={index_summary['failed']} "
            f"elapsed={index_summary['elapsed_s']:.2f}s"
        )
    else:
        print(
            "  Index DDL: unsupported "
            f"(elapsed={index_summary['elapsed_s']:.2f}s)"
        )

    total_rows = 0
    for pass_info in ingest_summary.get("pass_timings", []):
        rows = int(pass_info["rows"])
        elapsed_s = float(pass_info["elapsed_s"])
        total_rows += rows
        rows_per_s = rows / elapsed_s if elapsed_s > 0 else 0.0
        print(
            "  "
            f"{pass_info['name']}: rows={rows} batches={pass_info['batches']} "
            f"elapsed={elapsed_s:.2f}s throughput={rows_per_s:.1f} rows/s"
        )

    total_elapsed_s = float(ingest_summary.get("elapsed_s", 0.0))
    total_rows_per_s = total_rows / total_elapsed_s if total_elapsed_s > 0 else 0.0
    print(
        "  Total ingest: "
        f"rows={total_rows} elapsed={total_elapsed_s:.2f}s "
        f"throughput={total_rows_per_s:.1f} rows/s"
    )


def score_movies(
    client: VitalEdgeClient,
    ratings: list[RatingRecord],
    batch_size: int,
    max_write_batch_bytes: int | None,
) -> None:
    """Compute Bayesian movie scores from loaded ratings and persist in batches.

    This avoids a full graph aggregation pass over all RATED edges, which can be
    expensive on very large datasets. Instead, we aggregate in Python from the
    already-loaded CSV ratings and write per-movie stats back via indexed MATCH.
    """
    if not ratings:
        return

    rating_sum_by_movie: dict[int, float] = {}
    rating_count_by_movie: dict[int, int] = {}
    total_sum = 0.0
    total_count = 0
    for r in ratings:
        rating_sum_by_movie[r.movie_id] = rating_sum_by_movie.get(r.movie_id, 0.0) + r.rating
        rating_count_by_movie[r.movie_id] = rating_count_by_movie.get(r.movie_id, 0) + 1
        total_sum += r.rating
        total_count += 1

    global_avg = (total_sum / total_count) if total_count else 3.0
    C = 25

    updates = []
    for movie_id, num_ratings in rating_count_by_movie.items():
        avg_rating = rating_sum_by_movie[movie_id] / num_ratings
        base_score = (C * global_avg + avg_rating * num_ratings) / (C + num_ratings)
        updates.append(
            {
                "movie_id": movie_id,
                "avg_rating": avg_rating,
                "num_ratings": num_ratings,
                "base_score": base_score,
            }
        )

    score_query = """
    UNWIND $updates AS u
    MATCH (m:Movie {movie_id: u.movie_id})
    SET m.avg_rating = u.avg_rating,
        m.num_ratings = u.num_ratings,
        m.base_score = u.base_score
    """
    print(f"  Scoring pass: movie stats ({len(updates)} rows)")
    summary = _run_adaptive_batched_pass(
        client,
        items=updates,
        query=score_query,
        parameter_name="updates",
        row_to_payload=lambda update: update,
        row_limit=batch_size,
        max_write_batch_bytes=max_write_batch_bytes,
        label="Score movie_stats_query",
    )
    print(
        "  Scoring pass complete: "
        f"batches={summary['batches']} elapsed={summary['elapsed_s']:.2f}s"
    )


def _get_user_decade_affinities(
    client: VitalEdgeClient, user_id: int
) -> dict[int, float]:
    """Return {decade: avg_rating} for movies this user has rated.

    Decade is computed in Python (from the returned year) to avoid any
    ambiguity around integer vs. float division in the Cypher runtime.
    A decade affinity of 4.2 for the 1980s means this user rates 1980s
    films 4.2 / 5.0 on average — a strong positive signal.

    Aggregates by year server-side (avg per year) so only O(distinct years)
    rows are returned to Python instead of O(ratings), avoiding the large
    per-user response buffer that caused high memory usage on active users.
    """
    rows = client.execute(
        """
        MATCH (u:User {user_id: $user_id})-[r:RATED]->(m:Movie)
        WHERE m.year > 0
        RETURN m.year AS year, avg(r.rating) AS avg_rating
        """,
        parameters={"user_id": user_id},
    ).rows

    # Bucket years into decades client-side; rows is now O(distinct years)
    # not O(individual ratings), so this loop is always small.
    decade_sums: dict[int, float] = {}
    decade_counts: dict[int, int] = {}
    for row in rows:
        year = int(row.get("year") or 0)
        avg_rating = float(row.get("avg_rating") or 0)
        if year > 0:
            decade = (year // 10) * 10
            decade_sums[decade] = decade_sums.get(decade, 0.0) + avg_rating
            decade_counts[decade] = decade_counts.get(decade, 0) + 1

    return {
        decade: decade_sums[decade] / decade_counts[decade]
        for decade in decade_sums
    }


def _get_collaborative_candidates(
    client: VitalEdgeClient,
    user_id: int,
    candidate_limit: int,
) -> list[dict]:
    """Find candidate movies via graph-native collaborative filtering.

    Traversal pattern:
      target -[:RATED]-> shared_movie <-[:RATED]- peer
      peer   -[:RATED (>= 4.0)]-> candidate (not rated by target)

    Peer similarity = shared_rated_count / (1 + avg_rating_disagreement)
    A peer who agreed closely on many shared films scores high.

    Returns raw aggregates so Python can apply the final weighted scoring.
    """
    return client.execute(
        """
        MATCH (target:User {user_id: $user_id})-[r1:RATED]->(shared:Movie)<-[r2:RATED]-(peer:User)
        WHERE peer <> target AND abs(r1.rating - r2.rating) <= 1.5
        WITH target, peer,
             count(shared) AS shared_count,
             avg(abs(r1.rating - r2.rating)) AS avg_diff
        WHERE shared_count >= 3
        WITH target, peer,
             shared_count * (1.0 / (1.0 + avg_diff)) AS similarity
        ORDER BY similarity DESC
        LIMIT 30
        MATCH (peer)-[rp:RATED]->(candidate:Movie)
        WHERE rp.rating >= 4.0 AND NOT (target)-[:RATED]->(candidate)
        RETURN candidate.movie_id AS movie_id,
               candidate.title    AS title,
               candidate.year     AS year,
               coalesce(candidate.base_score, 0.0) AS base_score,
               avg(rp.rating)     AS peer_avg,
               count(rp)          AS peer_count,
               sum(similarity)    AS total_sim
        ORDER BY total_sim DESC
        LIMIT $candidate_limit
        """,
        parameters={"user_id": user_id, "candidate_limit": candidate_limit},
    ).rows


def _write_user_recommendations(
    client: VitalEdgeClient,
    user_id: int,
    recommendations: list[dict],
) -> None:
    if not recommendations:
        return
    payload = [
        {
            "user_id": user_id,
            "movie_id": r["movie_id"],
            "score": r["score"],
            "rank": i + 1,
        }
        for i, r in enumerate(recommendations)
    ]
    client.execute(
        """
        UNWIND $recs AS r
        MATCH (u:User {user_id: r.user_id}), (m:Movie {movie_id: r.movie_id})
        CREATE (u)-[rec:RECOMMENDED]->(m)
        SET rec.score = r.score, rec.rank = r.rank
        """,
        parameters={"recs": payload},
    )


def recommend_for_users(
    client: VitalEdgeClient,
    user_sample: int,
    limit: int,
) -> None:
    """Generate and store top-N recommendations for the most-active users.

    Scoring formula per candidate movie M for user U:
      collab_score   = peer_avg * log(1 + peer_count) * total_peer_similarity
      decade_boost   = user's avg rating for M's decade * 0.5
      base_boost     = M's Bayesian base_score * 0.3
      final_score    = collab_score + decade_boost + base_boost

    The decade_boost captures era preference without treating it as recency bias.
    A user who consistently rates 1970s films highly will see more 1970s films
    surfaced, not because they are old or new but because that era resonates.
    """
    user_rows = client.execute(
        """
        MATCH (u:User)-[:RATED]->()
        RETURN u.user_id AS user_id, count(*) AS rated_count
        ORDER BY rated_count DESC
        LIMIT $n
        """,
        parameters={"n": user_sample},
    ).rows

    total_users = len(user_rows)
    for user_index, row in enumerate(user_rows, start=1):
        user_id = int(row["user_id"])
        rated_count = int(row.get("rated_count") or 0)
        print(
            f"  [{user_index}/{total_users}] user_id={user_id} "
            f"rated_count={rated_count}: computing decade affinities ..."
        )
        decade_affinity = _get_user_decade_affinities(client, user_id)
        print(
            f"  [{user_index}/{total_users}] user_id={user_id}: "
            f"{len(decade_affinity)} decade(s); fetching candidates ..."
        )
        candidates = _get_collaborative_candidates(client, user_id, limit * 4)

        scored: list[dict] = []
        for c in candidates:
            year = int(c.get("year") or 0)
            decade = (year // 10) * 10 if year > 0 else 0
            collab = (
                float(c.get("peer_avg") or 0)
                * log(1 + float(c.get("peer_count") or 0))
                * float(c.get("total_sim") or 0)
            )
            decade_boost = decade_affinity.get(decade, 0.0) * 0.5 if decade else 0.0
            base_boost = float(c.get("base_score") or 0) * 0.3
            scored.append(
                {
                    "movie_id": int(c["movie_id"]),
                    "title": str(c.get("title") or ""),
                    "year": year,
                    "score": round(collab + decade_boost + base_boost, 4),
                }
            )

        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:limit]
        print(
            f"  [{user_index}/{total_users}] user_id={user_id}: "
            f"{len(candidates)} candidates scored, writing top {len(top)} recommendations"
        )
        _write_user_recommendations(client, user_id, top)


# ── Result output ─────────────────────────────────────────────────────────────

def print_top_overall(client: VitalEdgeClient, limit: int) -> None:
    rows = client.execute(
        """
        MATCH (m:Movie)
        WHERE m.num_ratings >= 1
        RETURN m.title AS title, m.year AS year,
               m.avg_rating AS avg_rating, m.num_ratings AS num_ratings,
               m.base_score AS score
        ORDER BY score DESC
        LIMIT $limit
        """,
        parameters={"limit": limit},
    ).rows
    print_rows(f"Top {limit} Overall Movies (Bayesian score)", rows)


def print_top_per_genre(client: VitalEdgeClient, limit: int) -> None:
    """Print the top-N movies per genre, each via a focused indexed query.

    One query per genre is intentional: there is no standard Cypher construct
    for TOP-N-per-group without subqueries or list slicing, and issuing a
    targeted parameterized query per genre keeps each query simple and fast.
    """
    genre_rows = client.execute(
        "MATCH (g:Genre) RETURN g.genre AS genre ORDER BY g.genre",
    ).rows
    genres = [str(r["genre"]) for r in genre_rows if r.get("genre")]

    for genre in genres:
        rows = client.execute(
            """
            MATCH (m:Movie)-[:GENRED]->(g:Genre {genre: $genre})
            WHERE m.num_ratings >= 1
            RETURN m.title AS title, m.year AS year, m.base_score AS score
            ORDER BY score DESC
            LIMIT $limit
            """,
            parameters={"genre": genre, "limit": limit},
        ).rows
        print_rows(f"Top {limit} {genre} Movies", rows)


def print_top_recent_year(client: VitalEdgeClient, limit: int) -> None:
    year_rows = client.execute(
        "MATCH (m:Movie) WHERE m.year > 0 AND m.num_ratings >= 1 RETURN max(m.year) AS max_year",
    ).rows
    if not year_rows or not year_rows[0].get("max_year"):
        print("\n=== Top Recent Year Movies === (no year data available)")
        return
    max_year = int(year_rows[0]["max_year"])
    rows = client.execute(
        """
        MATCH (m:Movie)
        WHERE m.year = $year AND m.num_ratings >= 1
        RETURN m.title AS title, m.year AS year,
               m.avg_rating AS avg_rating, m.base_score AS score
        ORDER BY score DESC
        LIMIT $limit
        """,
        parameters={"year": max_year, "limit": limit},
    ).rows
    print_rows(f"Top {limit} Movies from {max_year}", rows)


def print_user_recommendations(
    client: VitalEdgeClient,
    limit: int,
    display_count: int = 5,
) -> None:
    """Display recommendations for a sample of users who have them."""
    user_rows = client.execute(
        """
        MATCH (u:User)-[rec:RECOMMENDED]->()
        RETURN u.user_id AS user_id, count(rec) AS rec_count
        ORDER BY rec_count DESC
        LIMIT $n
        """,
        parameters={"n": display_count},
    ).rows

    for row in user_rows:
        user_id = int(row["user_id"])
        recs = client.execute(
            """
            MATCH (u:User {user_id: $user_id})-[rec:RECOMMENDED]->(m:Movie)
            RETURN m.title AS title, m.year AS year,
                   rec.score AS score, rec.rank AS rank
            ORDER BY rank
            LIMIT $limit
            """,
            parameters={"user_id": user_id, "limit": limit},
        ).rows
        print_rows(f"Top {limit} Recommendations for User {user_id}", recs)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()

    print(f"Loading movies from {args.movies} ...")
    movies = load_movies(args.movies)
    print(f"  {len(movies)} movies loaded")

    print(f"Loading ratings from {args.ratings} ...")
    ratings = load_ratings(args.ratings, limit=args.ratings_limit)
    print(f"  {len(ratings)} ratings loaded")

    if not movies or not ratings:
        raise SystemExit("No data found — check CSV paths and column names.")

    with VitalEdgeClient(host=args.host, port=args.port, tenant=args.tenant) as client:
        caps = client.get_capabilities()
        server_max_write_batch_bytes = caps.max_write_batch_bytes

        if server_max_write_batch_bytes is not None:
            print(f"Server initial MaxWriteBatchBytes: {server_max_write_batch_bytes}")
        if caps.max_write_batch_bytes_tuned:
            print("Server reports write-batch auto-tuning is active")

        print("Resetting graph ...")
        reset_graph(client)

        print("Ensuring ingest lookup indexes ...")
        index_summary = ensure_ingest_indexes(client)

        print("Ingesting movies, genres, and ratings ...")
        ingest_summary = ingest_graph(
            client,
            movies,
            ratings,
            args.batch_size,
            args.edge_batch_size,
            server_max_write_batch_bytes,
        )
        final_max_write_batch_bytes = ingest_summary.get("max_write_batch_bytes")
        if isinstance(final_max_write_batch_bytes, int) and final_max_write_batch_bytes > 0:
            print(f"Final tuned MaxWriteBatchBytes observed during ingest: {final_max_write_batch_bytes}")
        print_ingest_timing_summary(index_summary, ingest_summary)

        print("Scoring movies (Bayesian weighted average) ...")
        score_movies(
            client,
            ratings,
            args.batch_size,
            server_max_write_batch_bytes,
        )

        print(f"Generating recommendations for top {args.user_sample} users ...")
        recommend_for_users(client, args.user_sample, args.limit)

        print("\nResults:")
        print_top_overall(client, args.limit)
        print_top_per_genre(client, args.limit)
        print_top_recent_year(client, args.limit)
        print_user_recommendations(client, args.limit, display_count=5)


if __name__ == "__main__":
    main()
