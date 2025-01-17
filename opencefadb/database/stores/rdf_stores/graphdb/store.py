import logging
import pathlib
from typing import Union

import rdflib
from gldb.query import QueryResult
from gldb.query.rdfstorequery import SparqlQuery
from gldb.stores import RDFStore
from rdflib.plugins.stores.sparqlstore import SPARQLStore

from .connection import GraphDB

logger = logging.getLogger("opencefadb")


class GraphDBStore(RDFStore):

    def __init__(
            self,
            host,
            port,
            user,
            password,
            repository):
        self._graphdb_url = f"http://{host}:{port}"
        self._auth = (user, password)
        _graphdb = GraphDB(
            url=self._graphdb_url,
            auth=self._auth
        )
        self._repo = _graphdb[repository]
        self._expected_file_extensions = {".ttl", ".rdf", ".jsonld"}

    def __repr__(self):
        return f"<{self.__class__.__name__} (GraphDB-Repo={self._repo})>"

    @property
    def graph(self) -> rdflib.Graph:
        return self._repo.graph

    def upload_file(self, filename: Union[str, pathlib.Path]) -> bool:
        pass

    def execute_query(self, query: SparqlQuery) -> QueryResult:
        return QueryResult(query=query, result=query.execute(self.graph))
