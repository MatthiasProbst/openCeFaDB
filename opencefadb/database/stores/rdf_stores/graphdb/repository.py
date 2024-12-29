import logging
import pathlib

import rdflib

from .request_utils import _post_request

logger = logging.getLogger("pygraphdb")

CONTENT_TYPE = {
    ".jsonld": "application/ld+json",
    ".ttl": "text/turtle",
}


class GraphDBRepository:

    def __init__(self, params):
        self._params = params

    def __getitem__(self, item):
        return self._params[item]

    def upload_file(self, filename: pathlib.Path):
        logger.debug(f"Uploading file {filename} to {self['id']} ...")
        filename = pathlib.Path(filename)
        if not filename.exists():
            raise FileNotFoundError(f"File '{filename}' does not exist")

        _check_for_blank_nodes(filename)

        headers = {"Content-Type": CONTENT_TYPE[filename.suffix]}

        repo_id = self['id']

        with open(filename, 'rb') as f:
            response = _post_request(self["uri"] + "/statements", headers=headers, data=f, auth=self["auth"])
        if response.status_code == 204:
            logger.info(f"File '{filename}' successfully uploaded to GraphDB repository '{repo_id}'")
        else:
            logger.error(f"Could not upload file {filename}: {response.status_code} - {response.text}")
        logger.debug(f"Done uploading file {filename} to {repo_id} ...")
        return response


def _check_for_blank_nodes(filename):
    logger.debug(f"Checking file {filename} for blank nodes ...")
    g = rdflib.Graph()
    g.parse(source=filename)
    for s, p, o in g.triples((None, None, None)):
        if isinstance(s, rdflib.BNode) or isinstance(p, rdflib.BNode) or isinstance(o, rdflib.BNode):
            raise ValueError(f"File '{filename}' contains blank nodes. Blank nodes are not supported: {s}, {p}, {o}")
