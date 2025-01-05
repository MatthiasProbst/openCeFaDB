import logging
import pathlib
from concurrent.futures import ThreadPoolExecutor
from typing import List

import requests
from tqdm import tqdm

logger = logging.getLogger("opencefadb")


def download_file(download_url, target_filename, params=None) -> pathlib.Path:
    """Downloads from a URL"""
    target_filename = pathlib.Path(target_filename)
    logger.debug(f"Downloading metadata file from URL {download_url}...")
    response = requests.get(download_url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get('content-length', 0))  # Get total file size
    chunk_size = 1024  # Define chunk size for download

    with tqdm(total=total_size, unit='B', unit_scale=True, desc=target_filename.name, initial=0) as progress:
        with open(target_filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)
                    progress.update(len(chunk))

    assert target_filename.exists(), f"File {target_filename} does not exist."
    logger.debug(f"Download successful.")

    return target_filename


def download_multiple_files(urls, target_filenames, max_workers=4) -> List[pathlib.Path]:
    """Download multiple files concurrently."""
    if max_workers == 1:
        return [download_file(url, target_filename) for url, target_filename in zip(urls, target_filenames)]

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_file, url, target_filename) for url, target_filename in
                   zip(urls, target_filenames)]
        for future in futures:
            # Wait for each future to complete (can handle exceptions if needed)
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Failed to download a file: {e}")
    return results
