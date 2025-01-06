import unittest

from opencefadb import set_logging_level
from opencefadb.ontologies import dcat

set_logging_level('DEBUG')


class TestOntologies(unittest.TestCase):

    def test_dcat_DataService(self):
        data_service = dcat.DataService(
            id="http://local.org/sqlite3",
            title="sqlite3 Database",
            endpointURL="file:///path/to/endpoint",
            servesDataset=dcat.Dataset(
                title="Table Title",
                description="An SQL Table with the name 'Table Title'",
                distribution=dcat.Distribution(
                    id="http://local.org/sqlite3/12345",
                    identifier="12345",
                    mediaType="application/vnd.sqlite3",
                )
            )
        )
        self.assertEqual(data_service.id, "http://local.org/sqlite3")
        self.assertIsInstance(data_service, dcat.DataService)
