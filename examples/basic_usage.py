"""Basic usage example for the VitalEdge Python client."""

import datetime
import json

from vitaledge import VitalEdgeClient

# Connect to a local VitalEdge instance (gRPC on port 7443)
with VitalEdgeClient(host="localhost", port=7443, tenant="basic_example") as client:
    # Check server capabilities
    caps = client.get_capabilities()
    print(f"Protocol version : {caps.protocol_version}")
    print(f"Parser versions  : {caps.parser_versions}")
    print(f"Prepared queries : {caps.prepared_query_supported}")
    print()

    # Create some sample data
    client.execute(
        """CREATE (a:Person {name: 'Alice', age: 30})
           CREATE (b:Person {name: 'Bob', age: 52})
           CREATE (c:Person {name: 'Charlie', age: 42})
           CREATE (a)-[:KNOWS]->(b)"""
    )

    # Execute a simple Cypher query
    result = client.execute(
        "MATCH (p:Person) RETURN p LIMIT 5"
    )
    print(f"Columns : {result.columns}")
    for row in result.rows:
        print(row)

    # Explain a query
    plan = client.explain("""MATCH (p1:Person)-[r:KNOWS]->(p2:PERSON)
                          RETURN p1, r, p2
                          LIMIT 10""")
    print(json.dumps(json.loads(plan.explain_json), indent=2))

    # Execute a parameterized query
    query = """
    MATCH (p:Person {name: $personName})
    RETURN p.name, $thisYear - p.age AS year_of_birth
    """

    parameters = {
        "personName": "Bob",
        "thisYear": datetime.datetime.now().year,
    }

    result = client.execute(query, parameters=parameters)
    for row in result.rows:
        print(f"{row['p.name']} was born in {row['year_of_birth']}")

    # Clean up
    client.execute("MATCH (p:Person) DETACH DELETE p")
