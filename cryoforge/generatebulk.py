"""Ingest sample data during docker-compose"""

from pathlib import Path
from urllib.parse import urljoin
import argparse
import logging
import dask
import fsspec
import s3fs
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

def generate_stac_metadata(url: str, stac_server: str, collection: str, ingest: bool = False):
    stack_metadata = generate_itslive_metadata(url)
    stac_item = stack_metadata["stac"]
    if ingest:
        try:
            post_or_put(urljoin(stac_server, f"collections/{collection}/items"), stac_item.to_dict())
        except Exception as e:
            logging.error(f"Error with {url}: {e}")
    return stac_item
    
def generate_items(list_file: str,
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
    id = os.environ.get("COILED_BATCH_TASK_ID", "-1")
    task_id = int(id)
    s3_read = s3fs.S3FileSystem(anon=True)
    if task_id>=0:
        # coiled job
        list_files = s3_read.glob(list_file + "/**/*.txt")
        if task_id >= len(list_files):
            logging.info(f"Task ID {task_id} is out of range")
            return
        current_page = list_files[task_id]

        logging.info(f"Running in Coiled with task ID: {task_id} and file P{current_page}")
    else:
        logging.info(f"Running in local mode with file: {list_file}")
        current_page = list_file
    current_page_name = current_page.split("/")[-1].replace(".txt", "")

    with s3_read.open(current_page, mode="rt") as f:
        urls = f.read().splitlines()

    logging.info(f"Reading list from {list_file}, {len(urls)} URLs found.")
    tasks = [dask.delayed(generate_stac_metadata)(url, stac_server, "itslive", ingest) for url in urls]
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
    # for now make sure we don't write to any prod dir
    if s3_bucket and s3_bucket.startswith("s3://its-live-data/test-space/"):
        fs = fsspec.filesystem("s3")
        if format == "json":
            fs.put(f"{current_page_name}.ndjson", f"{s3_bucket}/{current_page_name}.ndjson")
        elif format == "parquet":
            fs.put(f"{current_page_name}.parquet", f"{s3_bucket}/{current_page_name}.parquet")
        else:
            logging.error(f"Invalid format {format}")
        logging.info(f"Uploaded {current_page_name} to {s3_bucket}")
       

def generate_stac_catalog():
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
    parser.add_argument("-b", "--bucket", help="S3 path where the output should be upload to")
    parser.add_argument("-i", "--ingest", action="store_true", help="If present the stac items will be ingested into the STAC endpoint")
    parser.add_argument("-t", "--target", help="STAC endpoint where items will be ingested")

    args = parser.parse_args()

    generate_items(list_file=args.list,
                 stac_server=args.target,
                 scheduler=args.scheduler,
                 workers=args.workers,
                 s3_bucket=args.bucket,
                 format=args.format,
                 ingest=args.ingest)


if __name__ == "__main__":
    generate_stac_catalog()
