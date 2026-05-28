"""Basic usage example for the VitalEdge Python client."""

from vitaledge import VitalEdgeClient

# Connect to a local VitalEdge instance (gRPC on port 7443)
with VitalEdgeClient(host="localhost", port=7443, tenant="default") as client:
    # Check server capabilities
    caps = client.get_capabilities()
    print(f"Protocol version : {caps.protocol_version}")
    print(f"Parser versions  : {caps.parser_versions}")
    print(f"Prepared queries : {caps.prepared_query_supported}")
    print()

    # Execute a simple Cypher query
    result = client.execute(
        "MATCH (n) RETURN n LIMIT 5",
        include_stats=True,
    )
    print(f"Columns : {result.columns}")
    for row in result.rows:
        print(row)
    print(f"Stats   : {result.stats}")

    # Explain a query
    plan = client.explain("MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 10")
    import json
    print(json.loads(plan.explain_json))

    query = """
    MATCH (:Movie {title: $movieTitle})<-[r:ACTED_IN]-(p:Person)
    WHERE r.role CONTAINS $actorRole
    RETURN p.name AS actor, r.role AS role
    """

    parameters = {
        "movieTitle": "Wall Street",
        "actorRole": "Fox",
    }

    result = client.execute(query, parameters=parameters, include_stats=True)
    for row in result.rows:
        print(f"actor: {row['actor']}, role: {row['role']}")