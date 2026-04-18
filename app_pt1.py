from flask import Flask, request, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)

URI = "bolt://34.27.133.37:7687"
USER = "neo4j"
PASSWORD = "allenwei050503"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))


@app.route("/")
def home():
    return jsonify({"message": "HW4 Neo4j API is running"})


@app.route("/graph-summary", methods=["GET"])
def graph_summary():
    with driver.session() as session:
        driver_count = session.run(
            "MATCH (d:Driver) RETURN count(d) AS c"
        ).single()["c"]

        company_count = session.run(
            "MATCH (c:Company) RETURN count(c) AS c"
        ).single()["c"]

        area_count = session.run(
            "MATCH (a:Area) RETURN count(a) AS c"
        ).single()["c"]

        trip_count = session.run(
            "MATCH ()-[t:TRIP]->() RETURN count(t) AS c"
        ).single()["c"]

    return jsonify({
        "driver_count": driver_count,
        "company_count": company_count,
        "area_count": area_count,
        "trip_count": trip_count
    })


@app.route("/top-companies", methods=["GET"])
def top_companies():
    n = int(request.args.get("n", 5))

    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[:TRIP]->(:Area)
    RETURN c.name AS name, count(*) AS trip_count
    ORDER BY trip_count DESC
    LIMIT $n
    """

    with driver.session() as session:
        result = session.run(query, n=n)
        companies = [
            {"name": record["name"], "trip_count": record["trip_count"]}
            for record in result
        ]

    return jsonify({"companies": companies})


@app.route("/high-fare-trips", methods=["GET"])
def high_fare_trips():
    area_id = int(request.args.get("area_id"))
    min_fare = float(request.args.get("min_fare"))

    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
    WHERE t.fare > $min_fare
    RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
    ORDER BY fare DESC
    """

    with driver.session() as session:
        result = session.run(query, area_id=area_id, min_fare=min_fare)
        trips = [
            {
                "trip_id": record["trip_id"],
                "fare": record["fare"],
                "driver_id": record["driver_id"]
            }
            for record in result
        ]

    return jsonify({"trips": trips})


@app.route("/co-area-drivers", methods=["GET"])
def co_area_drivers():
    driver_id = request.args.get("driver_id")

    query = """
    MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
    WHERE d1 <> d2
    RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
    ORDER BY shared_areas DESC, driver_id ASC
    """

    with driver.session() as session:
        result = session.run(query, driver_id=driver_id)
        co_drivers = [
            {
                "driver_id": record["driver_id"],
                "shared_areas": record["shared_areas"]
            }
            for record in result
        ]

    return jsonify({"co_area_drivers": co_drivers})


@app.route("/avg-fare-by-company", methods=["GET"])
def avg_fare_by_company():
    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[t:TRIP]->(:Area)
    RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
    ORDER BY avg_fare DESC
    """

    with driver.session() as session:
        result = session.run(query)
        companies = [
            {"name": record["name"], "avg_fare": record["avg_fare"]}
            for record in result
        ]

    return jsonify({"companies": companies})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)