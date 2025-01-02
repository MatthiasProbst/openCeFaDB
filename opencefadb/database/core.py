import logging
import pathlib
from typing import Union

import pandas as pd
import rdflib
from gldb import GenericLinkedDatabase
from gldb.stores import DataStoreManager

from opencefadb.configuration import get_config
from opencefadb.database.query_templates.sparql import (
    SELECT_FAN_PROPERTIES,
    SELECT_ALL,
    SELECT_FAN_CAD_FILE
)
from opencefadb.database.stores.rdf_stores.filedb.hdf5filedb import HDF5FileDB
from opencefadb.database.stores.rdf_stores.rdffiledb.rdffilestore import RDFFileStore
from opencefadb.utils import download_file

logger = logging.getLogger("opencefadb")

_db_instance = None


def _parse_to_qname(uri: rdflib.URIRef):
    uri_str = str(uri)
    for prefix, namespace in RDFFileStore.namespaces.items():
        if namespace in uri_str:
            return f"{prefix}:{uri_str.replace(namespace, '')}"
    return uri_str


class OpenCeFaDB(GenericLinkedDatabase):

    def __init__(self, store_manager: DataStoreManager):
        self._store_manager = store_manager

    @property
    def store_manager(self) -> DataStoreManager:
        return self._store_manager

    def linked_upload(self, filename: Union[str, pathlib.Path]):
        raise NotImplemented("Linked upload not yet implemented")

    def select_fan_properties(self) -> pd.DataFrame:
        def _parse_term(term):
            if isinstance(term, rdflib.URIRef):
                return _parse_to_qname(term)
            if isinstance(term, rdflib.Literal):
                return term.value
            return term

        bindings = self.execute_query("rdf_db", SELECT_FAN_PROPERTIES).bindings
        result_data = [{str(k): _parse_term(v) for k, v in binding.items()} for binding in bindings]
        variables = {}
        for data in result_data:
            if data["parameter"] not in variables:
                variables[data["parameter"]] = {}

            _data = data.copy()
            if data["property"] in ("m4i:hasStringValue", "m4i:hasNumericalValue"):
                key = "value"
            else:
                key = data["property"]

            if isinstance(data["value"], str) and "/standard_names/" in data["value"]:
                value = data["value"].split("/standard_names/")[-1]
            else:
                value = data["value"]
            variables[data["parameter"]][key] = value
        for var in variables.values():
            var.pop("rdf:type")
        return pd.DataFrame(variables.values())

    def download_cad_file(self, target_dir: Union[str, pathlib.Path]):
        """Queries the RDF database for the iges cad file"""
        bindings = self.execute_query("rdf_db", SELECT_FAN_CAD_FILE).bindings
        assert len(bindings) == 1, f"Expected one CAD file, got {len(bindings)}"
        download_url = bindings[0]["downloadURL"]
        _guess_filenames = download_url.rsplit("/", 1)[-1]
        target_dir = pathlib.Path(target_dir)
        return download_file(bindings[0]["downloadURL"], target_dir / _guess_filenames)

    def select_all(self):
        return self.execute_query("rdf_db", SELECT_ALL).bindings


def connect_to_database() -> OpenCeFaDB:
    """Connects to the database according to the configuration."""
    global _db_instance
    if _db_instance:
        return _db_instance
    cfg = get_config()
    store_manager = DataStoreManager()
    if cfg.rawdata_store == "hdf5_file_db":
        store_manager.add_store("hdf_db", HDF5FileDB())
    else:
        raise TypeError(f"Raw data store {cfg.rawdata_store} not (yet) supported.")
    if cfg.metadata_datastore == "rdf_file_db":
        rdf_file_store = RDFFileStore()
        for filename in cfg.metadata_directory.glob("*.jsonld"):
            rdf_file_store.upload_file(filename)
        for filename in cfg.metadata_directory.glob("*.ttl"):
            rdf_file_store.upload_file(filename)
        store_manager.add_store("rdf_db", rdf_file_store)
    else:
        raise TypeError(f"Metadata store {cfg.metadata_datastore} not supported.")
    _db_instance = OpenCeFaDB(store_manager)
    return _db_instance
