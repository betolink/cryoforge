"""Ingest sample data during docker-compose"""

from pathlib import Path
from urllib.parse import urljoin
import argparse
import logging
import dask
import fsspec
import os
import orjson
from dask.diagnostics.progress import ProgressBar
from .generate import generate_itslive_metadata

import requests
import ctypes
import pyarrow as pa
import stac_geoparquet

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
    stac_item = stack_metadata["stac"]
    try:
        post_or_put(urljoin(stac_server, f"collections/{collection}/items"), stac_item.to_dict())
    except Exception as e:
        logging.error(f"Error with {url}: {e}")
    return stac_item
    
def ingest_items(list_file: str,
                 stac_server: str,
                 scheduler: str ="processes",
                 workers: int = 4,
                 format: str = "json",
                 s3_bucket: str = "",
                 ingest: bool = False):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )
    task_id = int(os.environ["COILED_BATCH_TASK_ID", "-1"])
    if task_id>=0:
        # coiled job
        list_files = s3.glob(path_list + "/**/*.txt")
        if task_id >= len(list_files):
            logging.info(f"Task ID {task_id} is out of range")
            return
        current_page = list_files[task_id]
    else:
        current_page = list_file
    current_page_name = current_page.split("/")[-1].replace(".txt", "")

    with fsspec.open(current_page, mode="rt") as f:
        urls = f.read().splitlines()

    logging.info(f"Reading list from {list_file}, {len(urls)} URLs found.")
    tasks = [dask.delayed(generate_stac_metadata)(url, stac_server, "itslive") for url in urls]
    with ProgressBar():
        features = dask.compute(*tasks,
                               scheduler=scheduler,
                               num_workers=int(workers))
        trim_memory()
    logging.info(f"Finished processing {current_page}, {len(features)} STAC items generated.")

    if format == "json":
        with open(f"{current_page_name}.ndjson", mode="wb") as f:
            for item in features:
                f.write(orjson.dumps(item.to_dict()) + b"\n")
    elif format == "parquet":
        items_arrow = stac_geoparquet.arrow.parse_stac_items_to_arrow(features)
        stac_geoparquet.arrow.to_parquet(items_arrow, f"{current_page_name}.parquet")
    else:
        logging.error(f"Invalid format {format}")
       

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
    parser.add_argument("-f", "--format", default="json", help="STAC serialization, json or parquet")
    parser.add_argument("-b", "--bucket", default="s3://its-live-data/test-space/luis_catalogs/sentinel1/stac", help="S3 path where the output should be upload to")
    parser.add_argument("-i", "--ingest", help="If present the stac items will be ingested into the STAC endpoint")
    parser.add_argument("-t", "--target", help="STAC endpoint where items will be ingested")

    args = parser.parse_args()

    ingest_items(list_file=args.list,
                 stac_server=args.target,
                 scheduler=args.scheduler,
                 workers=args.workers,
                 s3_bucket=args.bucket,
                 ingest=args.ingest)


if __name__ == "__main__":
    ingest_stac()
