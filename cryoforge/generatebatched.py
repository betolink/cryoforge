import argparse
import logging
import warnings
from pathlib import Path
from datetime import datetime
import re
from collections import defaultdict
import orjson
import os
from dask.distributed import Client, LocalCluster, as_completed
from distributed import WorkerPlugin
import s3fs
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from tqdm import tqdm

from .generate import generate_itslive_metadata
from .tooling import trim_memory

os.environ["PYTHONUNBUFFERED"] = "1"


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)
warnings.filterwarnings("ignore", category=ResourceWarning)


class FSReadWorkerPlugin(WorkerPlugin):
    def __init__(self, fs_type: str = "fsspec", fs_opts: dict = None):
        if fs_type == "fsspec":
            if fs_opts is None:
                fs_opts = {
                    "anon": True,  # Set to False if you have credentials
                }
            self.fs_read = s3fs.S3FileSystem(**fs_opts)
        elif fs_type == "obstore":
            from obstore.store import S3Store
            self.fs_read = S3Store(
                bucket="its-live-data",
                region="us-west-2",
                client_options={
                    "timeout": "90s"
                },
                skip_signature=True
            )

    def setup(self, worker):
        worker.fs_read = self.fs_read
        print("Read-only FS initialized on worker", worker.address)

    def teardown(self, worker):
        fs = getattr(worker, 'fs_read', None)
        if isinstance(fs, s3fs.S3FileSystem):
            fs.clear_instance_cache()

def get_mid_date_from_filename(filename: str) -> str:
    dates = re.findall(r'\d{8}', filename)
    
    if len(dates) < 2:
        raise ValueError("Filename must contain at least two 8-digit dates.")
    
    # Parse first two as start and end
    start_date = datetime.strptime(dates[0], "%Y%m%d")
    end_date = datetime.strptime(dates[2], "%Y%m%d")  # Use the second *start* date

    mid_date = start_date + (end_date - start_date) / 2
    return mid_date.date().isoformat()

def get_files(pf: pq.ParquetFile, row_group_index: int = 0):
    """
    User-implemented function to retrieve files for a row group.
    Returns: list of tuples (prefix, filename, year)
    """
    files = []
    table = pf.read_row_group(row_group_index)
    batches = table.to_batches()
    for batch in batches:
        for i in range(table.num_rows):
            row = {col: batch.column(j)[i].as_py() for j, col in enumerate(batch.schema.names)}
            files.append((row["prefix"], row["path"], get_mid_date_from_filename(row['path'])[0:4]))
    return files
            

def generate_stac_metadata(full_uri: str):
    from distributed import get_worker
    worker = get_worker()
    if hasattr(worker, "fs_read"):
        fs = get_worker().fs_read
    else: 
        fs = s3fs.S3FileSystem(anon=True)
    try:
        metadata = generate_itslive_metadata(full_uri, fs)["stac"]
        return {"metadata": metadata, "url": full_uri, "error": None}
    except Exception as e:
        return {"metadata": None, "url": full_uri, "error": str(e)}


class BatchWriter:
    """Writes STAC items directly to prefix/year files in row group directory"""
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.processed_count = 0
        self.expected_count = 0
        self.file_handles = {}
        self.file_counts = defaultdict(int)
    
    def write_item(self, feature, prefix, year, filename):
        if not feature:
            return False
        
        # Create directory structure
        prefix_dir = self.base_path / prefix
        prefix_dir.mkdir(parents=True, exist_ok=True)
        
        # Get file handle for this prefix/year
        file_key = f"{prefix}/{year}"
        if file_key not in self.file_handles:
            file_path = prefix_dir / f"{year}.ndjson"
            # Open in append mode to support multiple writes
            self.file_handles[file_key] = open(file_path, "ab")
        
        # Write the item
        self.file_handles[file_key].write(orjson.dumps(feature.to_dict()) + b"\n")
        self.file_handles[file_key].flush()
        self.file_counts[file_key] += 1
        self.processed_count += 1
        return True
    
    def close(self):
        # Close all open file handles
        for handle in self.file_handles.values():
            handle.flush()
            handle.close()
        self.file_handles = {}
    
    def report(self):
        # Log counts per prefix/year
        for file_key, count in self.file_counts.items():
            prefix, year = file_key.split('/', 1)
            logging.info(f"Wrote {count} items to {prefix}/{year}.ndjson")
        
        # Validate total counts
        if self.processed_count != self.expected_count:
            logging.warning(
                f"Processed count mismatch: Expected {self.expected_count}, "
                f"Processed {self.processed_count}"
            )
        else:
            logging.info(f"Successfully processed all {self.processed_count} files")
        
        return self.processed_count

def upload_group_row(group_path: Path, mission: str = "sentinel2", row_path: str = "", target: str = "its-live-data/test-space/cloud-experiments/catalog/"):
    """
    Upload a row group directory to S3.
    """
    fs_writer = s3fs.S3FileSystem(anon=False)

    local_dir = os.path.abspath(group_path)

    for root, _, files in os.walk(group_path):
        for file in files:
            if file.endswith(".ndjson"):
                local_path = os.path.join(root, file)
                rel_path = os.path.relpath(local_path, local_dir)
                s3_path = f"{target.rstrip('/')}/{mission}/{row_path}/{rel_path}"
                fs_writer.put(local_path, s3_path)
                print(f"Test - uploaded {local_path} → {s3_path}")


def process_row_group(file: str = "",
                      row_group_index: int = 0,
                      io_driver: str = "fsspec",
                      processes=False,
                      num_workers: int = 4,
                      batch_size: int = 20000):
    """
    Process a row group containing potentially many files.
    Breaks the row group into batches for distributed processing.
    """
    # Get files to process for this row group
    if file.startswith("s3://"):
        fs = pafs.S3FileSystem(region="us-west-2",
                               anonymous=True)
        prefix = file.replace("s3://", "")
    else:
        fs = None
        prefix = file
        

    if (task_id := int(os.environ.get("COILED_BATCH_TASK_ID", -1))) >= 0:
        row_group_index = task_id

    logging.info(f"Using bach id {row_group_index}")


    pf = pq.ParquetFile(prefix, filesystem=fs)
    files = get_files(pf, row_group_index)
    if not files:
        logging.info(f"No files to process for row group {row_group_index}")
        return 0

    logging.info(f"Processing row group {row_group_index} with {len(files)} files")
    
    # Setup output directory
    output_path = Path(f"output/row_group_{row_group_index}")
    writer = BatchWriter(output_path)
    writer.expected_count = len(files)
    from dask import config as cfg

    cfg.set({'distributed.scheduler.worker-ttl': None})
    
    client = Client(LocalCluster(n_workers=num_workers, processes=True, threads_per_worker=1),
                    timeout='300s', heartbeat_interval='60s')
    print(f"Using Dask client with {num_workers} workers with I/O driver: {io_driver}")
    read_plugin = FSReadWorkerPlugin(fs_type=io_driver)
    client.register_worker_plugin(read_plugin, name="fs_read_plugin")
    
    # Process in batches
    total_batches = (len(files) + batch_size - 1) // batch_size
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(files))
        batch_files = files[start_idx:end_idx]
        
        logging.info(f"Processing batch {batch_num+1}/{total_batches} "
                     f"with {len(batch_files)} files")
        
        # Submit processing tasks
        futures = []
        for prefix, filename, year in batch_files:
            full_uri = f"s3://its-live-data/{prefix.rstrip('/')}/{filename}"
            futures.append(client.submit(generate_stac_metadata, full_uri))


        # Process each future as it completes (robust to failures)
        batch_results = []
        for future in tqdm(as_completed(futures), total=len(futures), desc="STAC generation"):
            batch_results.append(future.result())

        for result, (prefix, filename, year) in zip(batch_results, batch_files):
            try:
                if result["metadata"] is not None:
                    writer.write_item(result["metadata"], prefix, year, filename)
                else:
                    logging.error(f"Failed to generate metadata for {prefix}, {year}, {filename}: {result['error']}")
            except Exception as e:
                print(f"Error processing {prefix}, {year}, {filename}: {e}")            

        trim_memory()
    
    client.close()
    writer.close()
    processed_count = writer.report()
    upload_group_row(output_path, mission="sentinel1-extra", row_path = f"row_group_{row_group_index}", target="its-live-data/test-space/cloud-experiments/catalog/")
    logging.info(f"Completed row group {row_group_index}")
    return processed_count



def generate_stac_catalog():
    parser = argparse.ArgumentParser(description="Generate ITS_LIVE STAC metadata")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Row group processing command
    row_group_parser = subparsers.add_parser("process-row-group")
    row_group_parser.add_argument("-f", "--file-list", type=str, required=True, 
                                help="Parquet file containing a list of files to process")
    row_group_parser.add_argument("-i", "--row-group-index", type=int, 
                                 help="Row group index to process")
    row_group_parser.add_argument("-d", "--driver", type=str, default="fsspec", 
                                help="Filesystem driver to use (fsspec or obstore)")
    row_group_parser.add_argument("-w", "--workers", type=int, default=4, 
                                 help="Dask workers per batch")
    row_group_parser.add_argument("-b", "--batch-size", type=int, default=20000, 
                                 help="Processing batch size within row group")
    
    # Consolidation command
    consolidate_parser = subparsers.add_parser("consolidate")
    consolidate_parser.add_argument("-p", "--prefixes", nargs="+", 
                                   help="Specific prefixes to consolidate")
    consolidate_parser.add_argument("-o", "--output-base", default="output", 
                                   help="Output base directory")
    
    args = parser.parse_args()
    
    if args.command == "process-row-group":
        processed_count = process_row_group(
            file = args.file_list,
            row_group_index=args.row_group_index,
            io_driver=args.driver,
            num_workers=args.workers,
            batch_size=args.batch_size,
        )
        logging.info(f"Processed {processed_count} files for row group {args.row_group_index}")


if __name__ == "__main__":
    generate_stac_catalog()
