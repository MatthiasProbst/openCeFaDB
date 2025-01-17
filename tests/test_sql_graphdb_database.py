import pathlib
import shutil
import unittest

from opencefadb import connect_to_database
from opencefadb import set_logging_level
from opencefadb.configuration import get_config
from opencefadb.database.dbinit import initialize_database
from opencefadb.database.query_templates.sparql import SELECT_ALL
from opencefadb.database.stores.rdf_stores.graphdb import GraphDBStore

__this_dir__ = pathlib.Path(__file__).parent
set_logging_level('DEBUG')


class TestSqlGraphDBDatabase(unittest.TestCase):
    """Testing the combination of using a SQL database and a graph database."""

    def setUp(self):
        pathlib.Path("./test_download").mkdir(exist_ok=True)
        self._cfg = get_config()
        self._current_profile = self._cfg.profile
        self._cfg.select_profile("local_graphdb.test")

        # create or get existing:
        GraphDBStore.create(
            config_filename=__this_dir__ / "test-repo-config.ttl",
            host="localhost",
            port=7201
        )

        db = connect_to_database()
        initialize_database(self._cfg.metadata_directory)

    def tearDown(self):
        shutil.rmtree("./test_download")
        self._cfg.select_profile(self._current_profile)

    def test_singleton(self):
        db1 = connect_to_database()
        db2 = connect_to_database()
        self.assertIs(db1, db2)

    def test_init_rdf_database(self):
        db = connect_to_database(
            profile="local_graphdb.test"
        )
        db.rdf.reset(__this_dir__ / "test-repo-config.ttl")
        results = db.rdf.execute_query(SELECT_ALL)
        print(len(results.result))
        # db.rdf.upload_file("tests/data/ontology.ttl")
