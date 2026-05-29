"""Advanced cyber threat detection with VitalEdge-first analytics.

This example performs detection inside VitalEdge using graph + Cypher analytics.
The Kaggle fields Attack_Type and Label are explicitly held out from the threat
scoring logic and are only used for post-hoc evaluation.

Dataset:
https://www.kaggle.com/datasets/hussainsheikh03/cyber-threat-detection

Usage:
    python examples/advanced_cyber_threat_detection.py \
        --csv /path/to/cyberfeddefender_dataset.csv \
        --host localhost --port 7443 --tenant default
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from vitaledge import VitalEdgeClient


REQUIRED_COLUMNS = {
    "Timestamp",
    "Source_IP",
    "Destination_IP",
    "Protocol",
    "Packet_Length",
    "Duration",
    "Source_Port",
    "Destination_Port",
    "Bytes_Sent",
    "Bytes_Received",
    "Flags",
    "Flow_Packets/s",
    "Flow_Bytes/s",
    "Avg_Packet_Size",
    "Total_Fwd_Packets",
    "Total_Bwd_Packets",
    "Fwd_Header_Length",
    "Bwd_Header_Length",
    "Sub_Flow_Fwd_Bytes",
    "Sub_Flow_Bwd_Bytes",
    "Inbound",
    "Attack_Type",
    "Label",
}


@dataclass
class FlowRecord:
    flow_id: int
    timestamp: str
    source_ip: str
    destination_ip: str
    protocol: str
    packet_length: float
    duration_s: float
    source_port: int
    destination_port: int
    bytes_sent: float
    bytes_received: float
    flags: str
    flow_packets_per_s: float
    flow_bytes_per_s: float
    avg_packet_size: float
    total_fwd_packets: float
    total_bwd_packets: float
    fwd_header_length: float
    bwd_header_length: float
    sub_flow_fwd_bytes: float
    sub_flow_bwd_bytes: float
    inbound: int
    attack_type: str
    label: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VitalEdge cyber threat detection example")
    parser.add_argument("--csv", type=Path, required=True, help="Path to Kaggle CSV file")
    parser.add_argument("--host", default="localhost", help="VitalEdge host")
    parser.add_argument("--port", type=int, default=7443, help="VitalEdge gRPC port")
    parser.add_argument("--tenant", default="default", help="VitalEdge tenant")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250,
        help="How many flow rows to ingest per graph write",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.9,
        help="Threat score threshold for suspected malicious flows (z-score based; lower=more sensitive)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max rows to print for each result table",
    )
    return parser.parse_args()


def _to_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: str) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def load_records(csv_path: Path) -> list[FlowRecord]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    records: list[FlowRecord] = []
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        headers = set(reader.fieldnames or [])
        missing = REQUIRED_COLUMNS - headers
        if missing:
            missing_csv = ", ".join(sorted(missing))
            raise ValueError(f"CSV missing required columns: {missing_csv}")

        for row in reader:
            flow_id = len(records)
            records.append(
                FlowRecord(
                    flow_id=flow_id,
                    timestamp=(row.get("Timestamp") or "").strip(),
                    source_ip=(row.get("Source_IP") or "unknown").strip(),
                    destination_ip=(row.get("Destination_IP") or "unknown").strip(),
                    protocol=(row.get("Protocol") or "UNKNOWN").strip(),
                    packet_length=_to_float(row.get("Packet_Length", "0")),
                    duration_s=_to_float(row.get("Duration", "0")),
                    source_port=_to_int(row.get("Source_Port", "0")),
                    destination_port=_to_int(row.get("Destination_Port", "0")),
                    bytes_sent=_to_float(row.get("Bytes_Sent", "0")),
                    bytes_received=_to_float(row.get("Bytes_Received", "0")),
                    flags=(row.get("Flags") or "").strip(),
                    flow_packets_per_s=_to_float(row.get("Flow_Packets/s", "0")),
                    flow_bytes_per_s=_to_float(row.get("Flow_Bytes/s", "0")),
                    avg_packet_size=_to_float(row.get("Avg_Packet_Size", "0")),
                    total_fwd_packets=_to_float(row.get("Total_Fwd_Packets", "0")),
                    total_bwd_packets=_to_float(row.get("Total_Bwd_Packets", "0")),
                    fwd_header_length=_to_float(row.get("Fwd_Header_Length", "0")),
                    bwd_header_length=_to_float(row.get("Bwd_Header_Length", "0")),
                    sub_flow_fwd_bytes=_to_float(row.get("Sub_Flow_Fwd_Bytes", "0")),
                    sub_flow_bwd_bytes=_to_float(row.get("Sub_Flow_Bwd_Bytes", "0")),
                    inbound=_to_int(row.get("Inbound", "0")),
                    attack_type=(row.get("Attack_Type") or "Unknown").strip(),
                    label=_to_int(row.get("Label", "0")),
                )
            )

    return records


def _chunks(items: list[FlowRecord], size: int) -> Iterable[list[FlowRecord]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def reset_graph(client: VitalEdgeClient, tenant: str) -> None:
    reset_query = """
    MATCH (f:Host|Flow)
    DETACH DELETE f
    """
    client.execute(reset_query, tenant=tenant)


def ingest_flows(client: VitalEdgeClient, tenant: str, records: list[FlowRecord], batch_size: int) -> None:
    ingest_query = """
    UNWIND $events AS e
    MERGE (src:Host {ip: e.source_ip})
    MERGE (dst:Host {ip: e.destination_ip})
    CREATE (f:Flow {
        flow_id: e.flow_id,
        timestamp: e.timestamp,
        protocol: e.protocol,
        flags: e.flags,
        packet_length: e.packet_length,
        duration_s: e.duration_s,
        source_port: e.source_port,
        destination_port: e.destination_port,
        bytes_sent: e.bytes_sent,
        bytes_received: e.bytes_received,
        flow_packets_per_s: e.flow_packets_per_s,
        flow_bytes_per_s: e.flow_bytes_per_s,
        avg_packet_size: e.avg_packet_size,
        total_fwd_packets: e.total_fwd_packets,
        total_bwd_packets: e.total_bwd_packets,
        fwd_header_length: e.fwd_header_length,
        bwd_header_length: e.bwd_header_length,
        sub_flow_fwd_bytes: e.sub_flow_fwd_bytes,
        sub_flow_bwd_bytes: e.sub_flow_bwd_bytes,
        inbound: e.inbound,
        attack_type: e.attack_type,
        label: e.label
    })
    MERGE (src)-[:SENT]->(f)
    MERGE (f)-[:TO]->(dst)
    MERGE (src)-[:COMMUNICATES_WITH]->(dst)
    """

    for batch in _chunks(records, batch_size):
        payload = [r.__dict__ for r in batch]
        client.execute(ingest_query, parameters={"events": payload}, tenant=tenant)


def score_threats(
    client: VitalEdgeClient,
    tenant: str,
    threshold: float,
    records: list[FlowRecord],
) -> None:
    """Threat scoring using per-protocol anomaly detection with z-scores.
    
    Threat scoring intentionally excludes attack_type and label.
    Strategy: 
    - Group flows by protocol for per-protocol baseline statistics
    - Compute z-scores for each flow feature within its protocol group
    - Use the mean z-score as a protocol-relative anomaly score
    - Classify suspicious flows directly inside VitalEdge
    """
    if not records:
        return

    update_query = """
    MATCH (f:Flow)
    WITH
        f.protocol AS protocol,
        avg(f.bytes_sent) AS mean_bytes_sent,
        stDev(f.bytes_sent) AS stdev_bytes_sent,
        avg(f.bytes_received) AS mean_bytes_received,
        stDev(f.bytes_received) AS stdev_bytes_received,
        avg(f.flow_packets_per_s) AS mean_pps,
        stDev(f.flow_packets_per_s) AS stdev_pps,
        avg(f.flow_bytes_per_s) AS mean_bps,
        stDev(f.flow_bytes_per_s) AS stdev_bps,
        avg(f.packet_length) AS mean_packet_length,
        stDev(f.packet_length) AS stdev_packet_length
    MATCH (f:Flow)
    WHERE f.protocol = protocol
    WITH
        f,
        abs((f.bytes_sent - mean_bytes_sent) / stdev_bytes_sent) AS z_bytes_sent,
        abs((f.bytes_received - mean_bytes_received) / stdev_bytes_received) AS z_bytes_received,
        abs((f.flow_packets_per_s - mean_pps) / stdev_pps) AS z_pps,
        abs((f.flow_bytes_per_s - mean_bps) / stdev_bps) AS z_bps,
        abs((f.packet_length - mean_packet_length) / stdev_packet_length) AS z_packet_length
    WITH f, (z_bytes_sent + z_bytes_received + z_pps + z_bps + z_packet_length) / 5.0 AS threat_score
    SET f.threat_score = threat_score,
            f.detected_malicious = CASE WHEN threat_score >= $threshold THEN true ELSE false END,
            f.model_version = "vitaledge-rulegraph-v3-cypher-anomaly"
    RETURN count(f) AS updated_flows
    """
    client.execute(update_query, parameters={"threshold": threshold}, tenant=tenant)


def print_rows(title: str, rows: list[dict]) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("No rows returned")
        return
    for row in rows:
        print(row)


def run_hunting_queries(client: VitalEdgeClient, tenant: str, limit: int) -> None:
    limit_value = max(1, int(limit))
    hunting_query = """
    MATCH (src:Host)-[:SENT]->(f:Flow)
    WHERE f.detected_malicious = true
    RETURN "Top Suspicious Sources" AS report,
           src.ip AS source_ip,
           null AS destination_ip,
           count(f) AS suspicious_flows,
           null AS inbound_suspicious_flows,
           null AS distinct_targets,
           null AS distinct_ports,
           null AS distinct_sources,
           avg(f.threat_score) AS avg_score,
           max(f.threat_score) AS max_score
    ORDER BY suspicious_flows DESC, avg_score DESC
    LIMIT $limit_value
    UNION ALL
    MATCH (src:Host)-[:SENT]->(f:Flow)-[:TO]->(dst:Host)
    WHERE f.detected_malicious = true
    WITH src,
         count(f) AS suspicious_flows,
         count(DISTINCT dst.ip) AS distinct_targets,
         count(DISTINCT f.destination_port) AS distinct_ports,
         avg(f.threat_score) AS avg_score
    WHERE suspicious_flows >= 8 AND distinct_targets >= 4 AND distinct_ports >= 3
    RETURN "Possible Lateral Movement" AS report,
           src.ip AS source_ip,
           null AS destination_ip,
           suspicious_flows,
           null AS inbound_suspicious_flows,
           distinct_targets,
           distinct_ports,
           null AS distinct_sources,
           avg_score AS avg_score,
           null AS max_score
    ORDER BY distinct_targets DESC, avg_score DESC
    LIMIT $limit_value
    UNION ALL
    MATCH (src:Host)-[:SENT]->(f:Flow)-[:TO]->(dst:Host)
    WHERE f.detected_malicious = true
    RETURN "Destination Concentration" AS report,
           null AS source_ip,
           dst.ip AS destination_ip,
           null AS suspicious_flows,
           count(f) AS inbound_suspicious_flows,
           null AS distinct_targets,
           null AS distinct_ports,
           count(DISTINCT src.ip) AS distinct_sources,
           avg(f.threat_score) AS avg_score,
           null AS max_score
    ORDER BY inbound_suspicious_flows DESC, distinct_sources DESC
    LIMIT $limit_value
    """

    rows = client.execute(
        hunting_query,
        parameters={"limit_value": limit_value},
        tenant=tenant,
        include_stats=True,
    ).rows

    grouped: dict[str, list[dict]] = {
        "Top Suspicious Sources": [],
        "Possible Lateral Movement": [],
        "Destination Concentration": [],
    }
    for row in rows:
        report = str(row.get("report") or "")
        if report == "Top Suspicious Sources":
            grouped[report].append(
                {
                    "source_ip": row.get("source_ip"),
                    "suspicious_flows": row.get("suspicious_flows"),
                    "avg_score": row.get("avg_score"),
                    "max_score": row.get("max_score"),
                }
            )
        elif report == "Possible Lateral Movement":
            grouped[report].append(
                {
                    "source_ip": row.get("source_ip"),
                    "suspicious_flows": row.get("suspicious_flows"),
                    "distinct_targets": row.get("distinct_targets"),
                    "distinct_ports": row.get("distinct_ports"),
                    "avg_score": row.get("avg_score"),
                }
            )
        elif report == "Destination Concentration":
            grouped[report].append(
                {
                    "destination_ip": row.get("destination_ip"),
                    "inbound_suspicious_flows": row.get("inbound_suspicious_flows"),
                    "distinct_sources": row.get("distinct_sources"),
                    "avg_score": row.get("avg_score"),
                }
            )

    print_rows("Top Suspicious Sources", grouped["Top Suspicious Sources"])
    print_rows("Possible Lateral Movement", grouped["Possible Lateral Movement"])
    print_rows("Destination Concentration", grouped["Destination Concentration"])


def evaluate_against_labels(client: VitalEdgeClient, tenant: str, limit: int) -> None:
    limit_value = max(1, int(limit))
    confusion_query = """
    MATCH (f:Flow)
    RETURN
      sum(CASE WHEN f.detected_malicious = true AND f.label = 1 THEN 1 ELSE 0 END) AS tp,
      sum(CASE WHEN f.detected_malicious = true AND f.label = 0 THEN 1 ELSE 0 END) AS fp,
      sum(CASE WHEN f.detected_malicious = false AND f.label = 1 THEN 1 ELSE 0 END) AS fn,
      sum(CASE WHEN f.detected_malicious = false AND f.label = 0 THEN 1 ELSE 0 END) AS tn
    """
    confusion_rows = client.execute(confusion_query, tenant=tenant).rows
    confusion = []
    if confusion_rows:
        row = confusion_rows[0]
        tp = int(row.get("tp") or 0)
        fp = int(row.get("fp") or 0)
        fn = int(row.get("fn") or 0)
        tn = int(row.get("tn") or 0)
        confusion = [{
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "tn": tn,
            "precision": round((tp / (tp + fp)) if (tp + fp) else 0.0, 4),
            "recall": round((tp / (tp + fn)) if (tp + fn) else 0.0, 4),
            "f1": round(((2.0 * tp) / (2 * tp + fp + fn)) if (2 * tp + fp + fn) else 0.0, 4),
        }]

    attack_summary_query = """
    MATCH (f:Flow)
    RETURN f.attack_type AS attack_type,
        count(*) AS total,
        sum(CASE WHEN f.label = 1 THEN 1 ELSE 0 END) AS labeled_malicious,
        sum(CASE WHEN f.detected_malicious = true THEN 1 ELSE 0 END) AS detected_malicious,
        avg(f.threat_score) AS avg_score
    ORDER BY avg_score DESC
    """

    attack_rows = client.execute(attack_summary_query, tenant=tenant).rows

    breakdown = []
    for row in attack_rows[:limit_value]:
        attack_type = str(row.get("attack_type") or "Unknown")
        total = int(row.get("total") or 0)
        detected = int(row.get("detected_malicious") or 0)
        enriched = {
            "attack_type": attack_type,
            "total": total,
            "labeled_malicious": int(row.get("labeled_malicious") or 0),
            "detected_malicious": detected,
            "avg_score": row.get("avg_score"),
        }
        enriched["detected_rate"] = round((detected / total) if total else 0.0, 4)
        breakdown.append(enriched)

    print_rows("Evaluation vs Held-Out Labels", confusion)
    print_rows("Attack-Type Comparison (Post-Hoc Only)", breakdown)


def main() -> None:
    args = parse_args()
    records = load_records(args.csv)

    if not records:
        raise SystemExit("No rows found in CSV")

    with VitalEdgeClient(host=args.host, port=args.port, tenant=args.tenant) as client:
        print(f"Loaded {len(records)} flow rows from {args.csv}")
        print("Resetting graph and ingesting flow data...")
        reset_graph(client, args.tenant)
        ingest_flows(client, args.tenant, records, args.batch_size)

        print("Scoring threats in VitalEdge (without Attack_Type/Label features)...")
        score_threats(client, args.tenant, args.threshold, records)

        run_hunting_queries(client, args.tenant, args.limit)
        evaluate_against_labels(client, args.tenant, args.limit)


if __name__ == "__main__":
    main()
