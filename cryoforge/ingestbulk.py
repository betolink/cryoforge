"""Ingest sample data during docker-compose"""

from pathlib import Path
from urllib.parse import urljoin
import argparse
import logging
import dask
import fsspec
from dask.diagnostics.progress import ProgressBar
from .generate import generate_itslive_metadata

import requests
import ctypes

def trim_memory() -> int:
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


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
    stac_item = stack_metadata["stac"].to_dict()
    try:
        post_or_put(urljoin(stac_server, f"collections/{collection}/items"), stac_item)
    except Exception as e:
        logging.error(f"Error with {url}: {e}")
    return
    
def ingest_items(list_file: str, stac_server: str, scheduler: str ="processes", workers: int = 4):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )

    with fsspec.open(list_file, mode="rt") as f:
        urls = f.read().splitlines()

    logging.info(f"Reading list from {list_file}, {len(urls)} URLs found.")
    tasks = [dask.delayed(generate_stac_metadata)(url, stac_server, "itslive") for url in urls]
    with ProgressBar():
        results = dask.compute(*tasks,
                               scheduler=scheduler,
                               num_workers=int(workers))


def ingest_stac():
    """Ingest sample data during docker-compose"""
    parser = argparse.ArgumentParser(
        description="Generate metadata sidecar files for ITS_LIVE granules"
    )
    parser.add_argument(
        "-l", "--list", required=True, help="Path to a list of ITS_LIVE URLs to process and ingest"
    )
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of workers")
    parser.add_argument("-s", "--scheduler", default="processes", help="Dask scheduler")
    
    parser.add_argument("-t", "--target", required=True, help="STAC endpoint")

    args = parser.parse_args()

    stac_endpoint = args.target
    ingest_items(list_file=args.list, stac_server=stac_endpoint, scheduler=args.scheduler, workers=args.workers)


if __name__ == "__main__":
    ingest_stac()
