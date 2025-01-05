import json
import logging
import pathlib
import sqlite3

from gldb import RawDataStore
from gldb.query import RawDataStoreQuery, QueryResult

logger = logging.getLogger("opencefadb")


class SQLQuery(RawDataStoreQuery):

    def __init__(self, sql_query: str, filters=None):
        self.sql_query = sql_query
        self.filters = filters

    def execute(self, *args, **kwargs) -> QueryResult:
        pass


class HDF5SqlDB(RawDataStore):
    """
    HDF5SQLDB is a SQL database interface that stores data in HDF5 files.
    """

    def __init__(self):
        from ....configuration import get_config
        cfg = get_config()
        self._connection = self._initialize_database(cfg.rawdata_directory / "hdf5sql.db")
        self._filenames = {}
        self._expected_file_extensions = {".hdf", ".hdf5", ".h5"}

    def __repr__(self):
        return f"<{self.__class__.__name__}>"

    def upload_file(self, filename):
        return self._insert_hdf5_reference(self._connection, filename)

    def execute_query(self, query: SQLQuery) -> QueryResult:
        cursor = self._connection.cursor()
        params = []

        if query.filters:
            for key, value in query.filters.items():
                query += f" AND {key} = ?"
                params.append(value)

        cursor.execute(query.sql_query, params)
        return cursor.fetchall()

    @classmethod
    def _initialize_database(cls, db_path="hdf5_files.db"):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS hdf5_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_path TEXT NOT NULL UNIQUE,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            metadata TEXT
        )
        """)
        conn.commit()
        return conn

    @classmethod
    def _insert_hdf5_reference(cls, conn, filename, metadata=None):
        filename = pathlib.Path(filename).resolve().absolute()
        cursor = conn.cursor()
        metadata_dump = json.dumps(metadata) or '{}'
        try:
            cursor.execute("""
            INSERT INTO hdf5_files (file_path, metadata)
            VALUES (?, ?)
            """, (str(filename), metadata_dump))
            conn.commit()
            logger.debug(f"File {filename} inserted successfully.")
        except sqlite3.IntegrityError:
            logger.error(f"File {filename} already exists.")

    def __del__(self):
        """Ensure the connection is closed when the object is deleted."""
        if self._connection:
            self._connection.close()
            logger.debug("Database connection closed.")
