import logging
import pathlib

import rdflib

from opencefadb.configuration import get_config
from opencefadb.database import connect_to_database
from opencefadb.database.repositories.zenodo.download import download_metadata_file

__this_dir__ = pathlib.Path(__file__).parent
logger = logging.getLogger("opencefadb")
level = logger.level
for h in logger.handlers:
    h.setLevel(level)


def _get_metadata_datasets():
    db_dataset_config = __this_dir__ / "../db-dataset-config.jsonld"
    # metadata_datasets = {
    #     "@context": {
    #         "dcat": "http://www.w3.org/ns/dcat#",
    #     },
    #     "@graph": [
    #         {
    #             "@id": "https://zenodo.org/record/14551649",
    #             "@type": "dcat:Dataset",
    #             "dcat:identifier": "14551649",
    #             "dcat:distribution": {
    #                 "dcat:mediaType": "application/ld+json",
    #                 "dcat:downloadURL": "https://zenodo.org/records/14551649/files/metadata.jsonld"
    #             }
    #         },
    #         {
    #             "@id": "https://zenodo.org/record/14055811",
    #             "@type": "dcat:Dataset",
    #             "dcat:identifier": "14055811",
    #             "dcat:distribution": {
    #                 "dcat:mediaType": "application/ld+json",
    #                 "dcat:downloadURL": "https://zenodo.org/records/14055811/files/Standard_Name_Table_for_the_Property_Descriptions_of_Centrifugal_Fans.jsonld"
    #             }
    #         },
    #         {
    #             "@id": "https://zenodo.org/record/13351343",
    #             "@type": "dcat:Dataset",
    #             "dcat:identifier": "13351343",
    #             "dcat:distribution": {
    #                 "dcat:mediaType": "text/turtle",
    #                 "dcat:downloadURL": "https://zenodo.org/records/13351343/files/ssno.ttl"
    #             }
    #         }
    #     ]
    # }

    assert db_dataset_config.exists(), f"Database dataset config file not found: {db_dataset_config}"
    g = rdflib.Graph()
    g.parse(source=db_dataset_config, format="json-ld")
    return g


def initialize_database(metadata_directory):
    """Downloads all metadata from the known zenodo repositories"""
    download_dir = pathlib.Path(metadata_directory)

    logger.debug("Downloading metadata datasets...")
    download_metadata_datasets(_get_metadata_datasets(), download_dir)

    logger.debug("Init the opencefadb...")
    db = connect_to_database()
    rdf_file_store = db["rdf_db"]

    cfg = get_config()
    for filename in cfg.metadata_directory.glob("*.jsonld"):
        rdf_file_store.upload_file(filename)
    for filename in cfg.metadata_directory.glob("*.ttl"):
        rdf_file_store.upload_file(filename)
    logger.debug("...done")


def download_metadata_datasets(graph: rdflib.Graph, download_dir=None):
    if download_dir is None:
        download_dir = pathlib.Path.cwd()
    else:
        download_dir = pathlib.Path(download_dir)
    res = graph.query("""
    SELECT ?identifier ?downloadURL
    WHERE {
      ?dataset a dcat:Dataset .
      ?dataset dcat:distribution ?distribution .
      ?dataset dcat:identifier ?identifier .
      ?distribution dcat:downloadURL ?downloadURL .
    }
    """)
    for r in res.bindings:
        download_url = str(r[rdflib.Variable("downloadURL")])
        identifier = str(r[rdflib.Variable("identifier")])
        filename = pathlib.Path(download_url.rsplit('/', 1)[-1])
        target_filename = download_dir / f"{filename.stem}{identifier}{filename.suffix}"
        download_metadata_file(download_url, target_filename)
#
#
# if __name__ == "__main__":
#     download_metadata_datasets(g)
