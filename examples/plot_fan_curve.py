from gldb.query.rdfstorequery import SparqlQuery

from opencefadb import connect_to_database
from opencefadb.configuration import get_config
from opencefadb.database.dbinit import initialize_database
import pathlib

__this_dir__ = pathlib.Path(__file__).resolve().parent
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
    # print(db.execute_query("rdf_db", sparql_query).result.bindings)

    # ops = db.select_all_operation_points()

def reset_database():
    db = connect_to_database()
    cfg = get_config()
    initialize_database(cfg.metadata_directory)

def add_opm_files():
    root_dir = __this_dir__ / "../../data/measurements/processed/opm/main_cases"
    assert root_dir.is_dir(), f"{root_dir} does not exist."
    hdf_filenames = sorted(root_dir.rglob('*.hdf'))
    db = connect_to_database()
    for hdf_filename in hdf_filenames:
        db.upload_hdf(hdf_filename)

if __name__ == '__main__':
    # reset_database()
    add_opm_files()
    plot_fan_curve()
