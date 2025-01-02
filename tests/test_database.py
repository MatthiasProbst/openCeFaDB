import pathlib
import shutil
import unittest

import rdflib

from opencefadb import connect_to_database
from opencefadb import set_logging_level
from opencefadb.database import dbinit

set_logging_level('DEBUG')


class TestDatabase(unittest.TestCase):

    def setUp(self):
        pathlib.Path("./test_download").mkdir(exist_ok=True)

    def tearDown(self):
        shutil.rmtree("./test_download")

    def test_read_dataset_files(self):
        dataset = dbinit._get_metadata_datasets()
        self.assertIsInstance(dataset, rdflib.Graph)

    def test_init_database(self):
        filenames = dbinit.initialize_database(metadata_directory="./test_download")
        for filename in filenames:
            self.assertTrue(filename.exists())
        self.assertEqual(len(filenames), 2)

    def test_download_cad_file(self):
        db = connect_to_database()
        filename = db.download_cad_file(target_dir="./test_download")
        self.assertTrue(filename.exists())
        self.assertTrue(filename.suffix == ".igs")
