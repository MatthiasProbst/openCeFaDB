from gldb.query.rdfstorequery import SparqlQuery

from opencefadb import connect_to_database


def plot_fan_curve():
    db = connect_to_database()

    properties = db.select_fan_properties()
    print(properties)

    sparql_query = SparqlQuery(
        """PREFIX ssno: <https://matthiasprobst.github.io/ssno#>
        
        SELECT ?property ?value
        WHERE {
          ?subject ssno:standardName ?value .
        }
        """,
        description="Selects all properties of the fan"
    )
    print(db.execute_query("rdf_db", sparql_query).result.bindings)

    # ops = db.select_all_operation_points()


if __name__ == '__main__':
    plot_fan_curve()
