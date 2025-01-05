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

    # def tearDown(self):
    #     shutil.rmtree("./test_download")
    #     self._cfg.select_profile(self._current_profile)

    def test_download_with_metadata(self):
        download_url = "https://zenodo.org/records/14551649/files/metadata.jsonld"
        target_dir = pathlib.Path("./test_download")
        r = download_file(download_url, target_dir / "metadata.jsonld")
        print(r)