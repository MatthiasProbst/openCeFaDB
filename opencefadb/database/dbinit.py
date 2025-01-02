import enum
import logging
import pathlib
from typing import List

import rdflib

from opencefadb.configuration import get_config
from opencefadb.database import connect_to_database
from opencefadb.utils import download_file

__this_dir__ = pathlib.Path(__file__).parent
logger = logging.getLogger("opencefadb")
level = logger.level
for h in logger.handlers:
    h.setLevel(level)


class ApplicationType(enum.Enum):
    JSON_LD = "application/ld+json"
    TURTLE = "text/turtle"
    IGES = "model/iges"


def _get_metadata_datasets() -> rdflib.Graph:
    db_dataset_config = __this_dir__ / "../db-dataset-config.jsonld"
    assert db_dataset_config.exists(), f"Database dataset config file not found: {db_dataset_config}"
    g = rdflib.Graph()
    g.parse(source=db_dataset_config, format="json-ld")
    return g


def initialize_database(metadata_directory):
    """Downloads all metadata (jsonld-files) from the known zenodo repositories"""
    download_dir = pathlib.Path(metadata_directory)

    logger.debug("Downloading metadata datasets...")

    # use sparql to get all distributions that are of type application/ld+json

    filenames = download_metadata_datasets(_get_metadata_datasets(), download_dir=download_dir)

    logger.debug("Init the opencefadb...")
    db = connect_to_database()
    rdf_file_store = db["rdf_db"]

    cfg = get_config()
    for filename in cfg.metadata_directory.glob("*.jsonld"):
        rdf_file_store.upload_file(filename)
    for filename in cfg.metadata_directory.glob("*.ttl"):
        rdf_file_store.upload_file(filename)
    logger.debug("...done")
    return filenames


def download_metadata_datasets(
        graph: rdflib.Graph,
        application_type: ApplicationType = ApplicationType.JSON_LD,
        download_dir=None) -> List[pathlib.Path]:
    if download_dir is None:
        download_dir = pathlib.Path.cwd()
    else:
        download_dir = pathlib.Path(download_dir)
    res = graph.query(f"""
    SELECT ?identifier ?downloadURL
    WHERE {{
      ?dataset a dcat:Dataset .
      ?dataset dcat:distribution ?distribution .
      ?dataset dcat:identifier ?identifier .
      ?distribution dcat:downloadURL ?downloadURL .
      ?distribution dcat:mediaType "{application_type.value}" .
    }}
    """)
    filenames = []
    for r in res.bindings:
        download_url = str(r[rdflib.Variable("downloadURL")])
        identifier = str(r[rdflib.Variable("identifier")])
        filename = pathlib.Path(download_url.rsplit('/', 1)[-1])
        target_filename = download_dir / f"{filename.stem}{identifier}{filename.suffix}"
        download_file(download_url, target_filename.resolve())
        filenames.append(target_filename)
    return filenames
