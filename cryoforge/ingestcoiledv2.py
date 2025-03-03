#COILED n-tasks     8
#COILED max-workers 4  # Increased from 2 to 4
#COILED region      us-west-2
#COILED vm-type     t4g.medium


"""Ingest sample data during docker-compose with optimized asyncio and Dask"""

from urllib.parse import urljoin
import argparse
import logging
import dask
import fsspec
from dask.diagnostics.progress import ProgressBar
from .generate import generate_itslive_metadata
import os
import s3fs
import asyncio
import aiohttp
import math
from functools import partial
import ctypes


def trim_memory() -> int:
    """Trim memory, but we'll call this less frequently for better performance"""
    libc = ctypes.CDLL("libc.so.6")
    return libc.malloc_trim(0)


async def post_or_put_async(session, url: str, data: dict):
    """Async version of post or put data to url."""
    try:
        async with session.post(url, json=data) as response:
            if response.status == 409:
                new_url = url + f"/{data['id']}"
                # Exists, so update
                async with session.put(new_url, json=data) as put_response:
                    if put_response.status != 404:
                        put_response.raise_for_status()
                return put_response.status
            else:
                response.raise_for_status()
            return response.status
    except Exception as e:
        logging.error(f"Error with {url}: {e}")
        return None


async def process_urls_batch(urls, stac_server, collection, ingest=False, batch_size=10):
    """Process a batch of URLs using asyncio"""
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i+batch_size]
            tasks = []
            
            # Generate metadata and prepare requests (non-async part)
            metadata_items = []
            for url in batch:
                try:
                    stack_metadata = generate_itslive_metadata(url)
                    stac_item = stack_metadata["stac"].to_dict()
                    metadata_items.append((url, stac_item))
                except Exception as e:
                    logging.error(f"Error generating metadata for {url}: {e}")
            
            # Process HTTP requests asynchronously
            if ingest and metadata_items:
                for _, stac_item in metadata_items:
                    task = post_or_put_async(
                        session, 
                        urljoin(stac_server, f"collections/{collection}/items"), 
                        stac_item
                    )
                    tasks.append(task)
                
                await asyncio.gather(*tasks)
            
            # Trim memory occasionally, not after every item
            if i % (batch_size * 5) == 0:
                trim_memory()


def process_url_chunk(urls, stac_server, collection, ingest=False, batch_size=10):
    """Process a chunk of URLs on a single Dask worker using asyncio"""
    logging.info(f"Processing chunk of {len(urls)} URLs")
    
    # Create and run the asyncio event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            process_urls_batch(urls, stac_server, collection, ingest, batch_size)
        )
    finally:
        loop.close()
    
    return len(urls)


def ingest_coiled_async(
    path_list: str,
    stac_server: str,
    workers: int = 4,
    scheduler: str = "processes",
    ingest: bool = False,
    batch_size: int = 10,
    chunk_size: int = 100
):
    """Ingest data using Coiled with improved distribution and async processing"""
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )
    
    # Get task ID from Coiled environment
    task_id = int(os.environ["COILED_BATCH_TASK_ID"])
    
    # Connect to S3
    s3 = s3fs.S3FileSystem(anon=True)
    list_files = s3.glob(path_list + "/**/*.txt")
    
    if task_id >= len(list_files):
        logging.info(f"Task ID {task_id} is out of range")
        return
    
    # Get URLs for this task
    current_page = list_files[task_id]
    with s3.open(current_page, mode="rt") as f:
        all_urls = f.read().splitlines()
    
    logging.info(f"Processing {current_page}, {len(all_urls)} URLs found.")
    
    # Calculate chunk size based on number of URLs
    # We want to distribute work evenly across workers
    adjusted_chunk_size = min(chunk_size, math.ceil(len(all_urls) / workers))
    
    # Break the list of URLs into chunks for Dask workers
    url_chunks = [all_urls[i:i+adjusted_chunk_size] for i in range(0, len(all_urls), adjusted_chunk_size)]
    
    # Create delayed tasks for each chunk
    process_func = partial(
        process_url_chunk,
        stac_server=stac_server, 
        collection="itslive",
        ingest=ingest,
        batch_size=batch_size
    )
    
    tasks = [dask.delayed(process_func)(chunk) for chunk in url_chunks]
    
    # Compute results with Dask (using processes to avoid GIL)
    with ProgressBar():
        results = dask.compute(*tasks, scheduler=scheduler, num_workers=workers)
    
    total_processed = sum(results)
    logging.info(f"Total URLs processed: {total_processed}")
    return total_processed


def ingest_stac_coiled_async():
    """Ingest sample data during docker-compose"""
    parser = argparse.ArgumentParser(
        description="Generate metadata sidecar files for ITS_LIVE granules using Coiled with asyncio"
    )
    parser.add_argument(
        "-l",
        "--list",
        required=True,
        help="Path to a list of ITS_LIVE URLs to process and ingest"
    )
    parser.add_argument(
        "-w", "--workers", type=int, default=4, help="Number of Dask workers"
    )

    parser.add_argument("-s", "--scheduler", default="processes", help="Dask scheduler")
    parser.add_argument(
        "-b", "--batch-size", type=int, default=10, 
        help="Number of URLs to process in a single asyncio batch"
    )
    parser.add_argument(
        "-c", "--chunk-size", type=int, default=100,
        help="Number of URLs to process per Dask worker"
    )
    parser.add_argument(
        "-t", "--target", required=True, help="STAC endpoint"
    )
    parser.add_argument(
        "-i", "--ingest", action="store_true", help="If present will ingest the items"
    )

    args = parser.parse_args()

    ingest_coiled_async(
        path_list=args.list,
        stac_server=args.target,
        workers=args.workers,
        scheduler=args.scheduler,
        ingest=args.ingest,
        batch_size=args.batch_size,
        chunk_size=args.chunk_size,
    )


if __name__ == "__main__":
    ingest_stac_coiled_async()
