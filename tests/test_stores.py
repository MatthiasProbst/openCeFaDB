import sqlite3
import unittest

from pydantic import AnyUrl

from opencefadb import set_logging_level
from opencefadb.database.stores.filedb.hdf5sqldb import HDF5SqlDB

set_logging_level('DEBUG')


class TestStores(unittest.TestCase):

    def test_sql_store(self):
        sql_store = HDF5SqlDB()
        sql_row = sql_store.generate_mapping_dataset("1")
        print(AnyUrl(sql_row.endpointURL))
        conn = sqlite3.connect(AnyUrl(sql_row.endpointURL).path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM hdf5_files")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.close()
