import pathlib
import shutil
import unittest

import rdflib

from opencefadb import connect_to_database
from opencefadb import set_logging_level
from opencefadb.configuration import get_config
from opencefadb.database import dbinit
from opencefadb.utils import download_file

set_logging_level('DEBUG')


class TestDownload(unittest.TestCase):

    def setUp(self):
        pathlib.Path("./test_download").mkdir(exist_ok=True)
        self._cfg = get_config()
        self._current_profile = self._cfg.profile
        self._cfg.select_profile("test")

    def tearDown(self):
        shutil.rmtree("./test_download")
        self._cfg.select_profile(self._current_profile)

    def test_download_with_metadata(self):
        download_url = "https://zenodo.org/records/14551649/files/metadata.jsonld"
        target_dir = pathlib.Path("./test_download")
        r = download_file(download_url, target_dir / "metadata.jsonld")
        print(r)

    def test_download_hdf_ontology(self):
        url = "https://purl.allotrope.org/voc/adf/REC/2024/12/hdf.ttl"
        target_dir = pathlib.Path("./test_download")
        filename = download_file(url, target_dir / "ontology-hdf.ttl")
        self.assertTrue(filename.exists())
        graph = rdflib.Graph()
        graph.parse(filename, format="ttl")
        self.assertGreater(len(graph), 0)

        # query for hdf5:File:
        res = graph.query("""
        PREFIX hdf: <http://purl.allotrope.org/ontologies/hdf5/1.8#>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX owl: <http://www.w3.org/2002/07/owl#>
        
        SELECT ?p ?o WHERE {
            hdf:File ?p ?o .
        }
        """)
        bindings = res.bindings
        self.assertEqual(7, len(bindings))
        property_value_dict = {b[rdflib.Variable("p")]: b[rdflib.Variable("o")] for b in bindings}
        self.assertEqual(
            property_value_dict[rdflib.URIRef("http://www.w3.org/2000/01/rdf-schema#label")],
            rdflib.Literal("file")
        )
        self.assertEqual(
            property_value_dict[rdflib.URIRef("http://www.w3.org/2004/02/skos/core#altLabel")],
            rdflib.Literal("HDF 5 file")
        )