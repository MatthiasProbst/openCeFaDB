import logging
import pathlib

import requests

logger = logging.getLogger("opencefadb")


def download_file(download_url, target_filename) -> pathlib.Path:
    """Downloads from a URL"""
    target_filename = pathlib.Path(target_filename)
    logger.debug(f"Downloading metadata file from URL {download_url}...")
    response = requests.get(download_url)
    response.raise_for_status()

    with open(target_filename, "wb") as f:
        f.write(response.content)

    assert target_filename.exists(), f"File {target_filename} does not exist."
    logger.debug(f"Download successful.")
    return target_filename
