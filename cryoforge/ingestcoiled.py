#COILED n-tasks     8
#COILED max-workers 2
#COILED region      us-west-2
#COILED vm-type     t4g.medium


"""Ingest sample data during docker-compose"""

from urllib.parse import urljoin
import argparse
import logging
import dask
import fsspec
from dask.diagnostics.progress import ProgressBar
from .generate import generate_itslive_metadata
import os
import s3fs

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


def generate_stac_metadata(
    url: str, stac_server: str, collection: str, ingest: bool = False
):
    stack_metadata = generate_itslive_metadata(url)
    stac_item = stack_metadata["stac"].to_dict()
    if ingest:
        try:
            post_or_put(urljoin(stac_server, f"collections/{collection}/items"), stac_item)
        except Exception as e:
            logging.error(f"Error with {url}: {e}")
    trim_memory()
    return


def ingest_coiled(
    path_list: str,
    stac_server: str,
    scheduler: str = "processes",
    workers: int = 4,
    ingest: bool = False,
):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )
    s3 = s3fs.S3FileSystem(anon=True)
    list_files = s3.glob(path_list + "/**/*.txt")
    task_id = int(os.environ["COILED_BATCH_TASK_ID"])
    if task_id >= len(list_files):
        logging.info(f"Task ID {task_id} is out of range")
        return
    current_page = list_files[task_id]

    with s3.open(current_page, mode="rt") as f:
        urls = f.read().splitlines()

    logging.info(f"Processing {current_page}, {len(urls)} URLs found.")
    tasks = [
        dask.delayed(generate_stac_metadata)(url, stac_server, "itslive", ingest=ingest)
        for url in urls
    ]
    with ProgressBar():
        results = dask.compute(*tasks, scheduler=scheduler, num_workers=int(workers))


def ingest_stac_coiled():
    """Ingest sample data during docker-compose"""
    parser = argparse.ArgumentParser(
        description="Generate metadata sidecar files for ITS_LIVE granules using Coiled"
    )
    parser.add_argument(
        "-l",
        "--list",
        required=True,
        help="Path to a list of ITS_LIVE URLs to process and ingest",
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=4, help="Number of workers"
    )
    parser.add_argument("-s", "--scheduler", default="processes", help="Dask scheduler")

    parser.add_argument("-t", "--target", required=True, help="STAC endpoint")

    parser.add_argument(
        "-i", "--ingest", action="store_true", help="If present will ingest the items"
    )

    args = parser.parse_args()

    ingest_coiled(
        path_list=args.list,
        stac_server=args.target,
        scheduler=args.scheduler,
        workers=args.workers,
        ingest=args.ingest,
    )


if __name__ == "__main__":
    ingest_stac_coiled()
