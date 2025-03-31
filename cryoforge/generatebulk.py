import argparse
import logging
import os
import subprocess
from pathlib import Path

import dask
import orjson
import s3fs
from dask.diagnostics.progress import ProgressBar
from dask.distributed import Client, progress, LocalCluster

from .generate import generate_itslive_metadata
from .tooling import list_s3_objects, split_s3_path, trim_memory


def generate_stac_metadata(url: str):
    metadata = generate_itslive_metadata(url)
    return metadata["stac"]

   
def generate_items(regions_path: str,
                   workers: int = 4,
                   sync: bool = False):
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

        region_paths = s3_read.ls(regions_path)
        if task_id >= len(region_paths):
            logging.info(f"Task ID {task_id} is out of range")
            return 
        current_region = f"s3://{region_paths[task_id]}"

        logging.info(f"Running in Coiled with task ID: {task_id} and region {current_region}")
    else:
        logging.info(f"Running in local mode with path: {regions_path}")
        current_region = regions_path

                                                             
    batch_number = 0
    source_base, source_relative_path = split_s3_path(current_region)
    output_path = Path(source_relative_path)
    output_path.mkdir(parents=True, exist_ok=True)
    logging.info(f"Created output directory {output_path}")

    try:
        client = Client.current()  # Returns existing Client if available
    except Exception as e:
        logging.warn("No cluster found, creating a new one")
        cluster = LocalCluster(processes=True, threads_per_worker=2, n_workers=workers)
        client = Client(cluster)

    processed_items = 0
    files_generated = []

    for batch in list_s3_objects(current_region, pattern="*.nc", batch_size=4000):
        logging.info(f"Processing {len(batch)} files from {current_region}, batch [{batch_number}]")
        if not batch:
            break
        ProgressBar().register() # not sure if needed
        tasks = [dask.delayed(generate_stac_metadata)(url) for url in batch]
        tasks = dask.delayed(tasks)
        comp = tasks.persist()
        progress(comp)
        features = comp.compute()
        for feature in features:
            year = feature.properties["mid_datetime"][0:4]
            stac_path = f"{output_path}/{year}.ndjson"
            files_generated.append(stac_path)
            with open(stac_path, 'ab') as f:  # 'ab' = append in binary mode
                f.write(orjson.dumps(feature.to_dict()) + b"\n")

        processed_items += len(features)
        del features
        trim_memory()
        batch_number += 1
    logging.info(f"Finished processing {current_region}, {processed_items} STAC items generated.")

    if sync:
        logging.info(f"Syncing {output_path.parts[0]} to s3://its-live-data/test-space/stac_catalogs/")

        result = subprocess.run(
            ["aws",
             "s3",
             "sync",
            f"{output_path.parts[0]}",
            f"s3://its-live-data/test-space/stac_catalogs/{output_path.parts[0]}/",
             "--exact-timestamps"], capture_output=True, text=True)
        if result.stderr:
            logging.info("ERRORS: ")
            logging.error("\n" + result.stderr)
        if result.stdout:
            logging.info("\n" + result.stdout)

        result = subprocess.run(
            ["rm",
             "-rf",
             f"{output_path.parts[0]}"],
             check=True)
        if result.stderr:
            logging.info("ERRORS: ")
            logging.error("\n" + result.stderr)


def generate_stac_catalog():
    """Generate and optionally ingest ITS_LIVE STAC catalogs"""
    parser = argparse.ArgumentParser(
        description="Generate metadata sidecar files for ITS_LIVE granules"
    )
    parser.add_argument(
        "-p", "--path", required=True, help="Path to a list of ITS_LIVE URLs to process and ingest"
    )
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of Dask workers")
    parser.add_argument("-s", "--sync", action="store_true", help="If present the yearly stac items will be uploaded to S3")

    args = parser.parse_args()

    generate_items(regions_path=args.path,
                 workers=args.workers,
                 sync=args.sync)


if __name__ == "__main__":
    generate_stac_catalog()
