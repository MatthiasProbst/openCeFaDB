import logging

import requests

logger = logging.getLogger("opencefadb")


def download_metadata_file(download_url, target_filename):
    """Downloads the metadata.json from a Zenodo record"""
    logger.debug(f"Downloading metadata file from zenodo record {download_url}...")
    response = requests.get(download_url)
    response.raise_for_status()

    with open(target_filename, "wb") as f:
        f.write(response.content)

    assert target_filename.exists(), f"File {target_filename} does not exist."
    logger.debug(f"Download successful.")
