"""Ingest sample data during docker-compose"""

from pathlib import Path
from urllib.parse import urljoin
import argparse
import logging
import dask
from dask.diagnostics.progress import ProgressBar
from .generate import generate_itslive_metadata

import requests


def post_or_put(url: str, data: dict):
    """Post or put data to url."""
    r = requests.post(url, json=data)
    if r.status_code == 409:
        new_url = url + f"/{data['id']}"
        # Exists, so update
        r = requests.put(new_url, json=data)
        # Unchanged may throw a 404
        if not r.status_code == 404:
            r.raise_for_status()
    else:
        r.raise_for_status()
    return r.status_code

def generate_stac_metadata(url: str, stac_server: str, collection: str):
    stack_metadata = generate_itslive_metadata(url)
    stac_item = stack_metadata["stac"]
    return post_or_put(urljoin(stac_server, f"collections/{collection}/items"), stac_item)
    
def ingest_items(list_file: str, stac_server: str, workers: int = 4):
    urls = Path(list_file).read_text().splitlines()
    tasks = [dask.delayed(generate_stac_metadata)(url, stac_server, "itslive") for url in urls]
    with ProgressBar():
        results = dask.compute(*tasks,num_workers=workers)


def ingest_stac():
    """Ingest sample data during docker-compose"""
    parser = argparse.ArgumentParser(
        description="Generate metadata sidecar files for ITS_LIVE granules"
    )
    parser.add_argument(
        "-l", "--list", required=True, help="Path to a list of ITS_LIVE URLs to process and ingest"
    )
    parser.add_argument("-w", "--workers", default=4, help="Number of workers")
    
    parser.add_argument("-t", "--target", required=True, help="STAC endpoint")

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )

    logging.info(f"Ingesting {args.item}")
    stac_endpoint = args.target
    ingest_items(args.list, stac_endpoint, args.workers)


if __name__ == "__main__":
    ingest_stac()
