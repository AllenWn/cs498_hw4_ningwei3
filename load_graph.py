import csv
from neo4j import GraphDatabase

URI = "bolt://34.27.133.37:7687"
USER = "neo4j"
PASSWORD = "allenwei050503"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def clear_database():
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def create_constraints():
    queries = [
        "CREATE CONSTRAINT driver_id_unique IF NOT EXISTS FOR (d:Driver) REQUIRE d.driver_id IS UNIQUE",
        "CREATE CONSTRAINT company_name_unique IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE",
        "CREATE CONSTRAINT area_id_unique IF NOT EXISTS FOR (a:Area) REQUIRE a.area_id IS UNIQUE"
    ]
    with driver.session() as session:
        for q in queries:
            session.run(q)

def load_graph():
    query = """
    MERGE (d:Driver {driver_id: $driver_id})
    MERGE (c:Company {name: $company})
    MERGE (a:Area {area_id: $dropoff_area})
    MERGE (d)-[:WORKS_FOR]->(c)
    CREATE (d)-[:TRIP {
        trip_id: $trip_id,
        fare: $fare,
        trip_seconds: $trip_seconds
    }]->(a)
    """

    with driver.session() as session:
        with open("data/taxi_trips_clean.csv", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                session.run(
                    query,
                    trip_id=row["trip_id"],
                    driver_id=row["driver_id"],
                    company=row["company"],
                    dropoff_area=int(row["dropoff_area"]),
                    fare=float(row["fare"]),
                    trip_seconds=int(row["trip_seconds"])
                )

if __name__ == "__main__":
    clear_database()
    create_constraints()
    load_graph()
    driver.close()
    print("Graph loading completed.")