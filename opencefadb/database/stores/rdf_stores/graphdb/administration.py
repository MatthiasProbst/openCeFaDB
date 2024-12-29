import pathlib
from typing import Union

import requests

from .url import _parse_url


def delete_repository(repository_id: str, url="http://localhost:7200", auth=(None, None)):
    """Delete a repository from a GraphDB instance.

    Equivalent to "curl -X DELETE localhost:7200/rest/repositories/<repository_id>"
    """
    response = requests.delete(f"{_parse_url(url)}/rest/repositories/{repository_id}", auth=auth)
    response.raise_for_status()


def create_repository(config_filename: Union[str, pathlib.Path], url="http://localhost:7200", auth=(None, None)):
    """Create a repository in a GraphDB instance.

    Equivalent to `curl -X POST localhost:7201/rest/repositories -H "Content-Type: multipart/form-data" -F "config=@test-repo-config.ttl"`
    """
    config_filename = pathlib.Path(config_filename)
    if not config_filename.exists():
        raise FileNotFoundError(f"Configuration file '{config_filename}' does not exist")
    if not config_filename.suffix == '.ttl':
        raise ValueError("Configuration file must be a Turtle file (.ttl)")
    with open(config_filename, 'rb') as config_file:
        files = {'config': config_file}
        response = requests.post(f"{_parse_url(url)}/rest/repositories", files=files)
        response.raise_for_status()
