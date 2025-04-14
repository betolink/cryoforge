import argparse
import logging
import os
import json
import warnings
from pathlib import Path
from collections import defaultdict
from tempfile import NamedTemporaryFile
import orjson
from dask.distributed import Client, LocalCluster, progress
import s3fs
from smart_open import open as smart_open
from .generate import generate_itslive_metadata
from .tooling import list_s3_objects, trim_memory

def generate_stac_metadata(url: str):
    metadata = generate_itslive_metadata(url)
    return metadata["stac"]

# Configure logging and suppress asyncio resource warnings
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)
warnings.filterwarnings("ignore", category=ResourceWarning)

class ChunkedFileHandler:
    def __init__(self, base_path, s3_prefix, s3_fs):
        self.base_path = Path(base_path)
        self.s3_prefix = s3_prefix
        self.s3 = s3_fs
        self.chunk_dir = self.base_path / "chunks"
        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        self.tracker = defaultdict(int)
        self.current_batch_chunks = set()

    def write_feature(self, feature):
        year = feature.properties["mid_datetime"][:4]
        chunk_num = self.tracker[year]
        chunk_path = self.chunk_dir / f"{year}-chunk{chunk_num:04d}.ndjson"
        self.current_batch_chunks.add((year, chunk_num))
        data = orjson.dumps(feature.to_dict()) + b"\n"
        with open(chunk_path, 'ab') as f:
            f.write(data)

    def rotate_chunks(self):
        logging.info(f"Rotating chunks for all years after batch completion")
        for year in self.tracker:
            self.tracker[year] += 1

    def upload_batch_chunks(self, sync=False):
        if not sync:
            logging.info("Sync disabled, skipping chunk uploads")
            self.current_batch_chunks.clear()
            return
        logging.info(f"Uploading {len(self.current_batch_chunks)} chunks from current batch")
        for year, chunk_num in self.current_batch_chunks:
            chunk_path = self.chunk_dir / f"{year}-chunk{chunk_num:04d}.ndjson"
            self._upload_chunk(chunk_path, year, chunk_num)
        self.current_batch_chunks.clear()

    def _upload_chunk(self, chunk_path, year, chunk_num):
        s3_path = f"{self.s3_prefix}/chunks/{year}-chunk{chunk_num:04d}.ndjson"
        try:
            self.s3.put(str(chunk_path), s3_path)
            logging.info(f"Uploaded {chunk_path.name} to S3")
        except Exception as e:
            logging.error(f"Failed to upload {chunk_path.name}: {str(e)}")

    def consolidate_year(self, year, sync=False):
        chunk_count = self.tracker[year] + 1
        logging.info(f"Consolidating {chunk_count} chunks for {year}")
        
        try:
            if sync:
                # Cloud consolidation - download chunks from S3, consolidate, then upload
                s3_consolidated_path = f"{self.s3_prefix}/{year}.ndjson"
                local_consolidated_path = self.base_path / f"{year}.ndjson"
                
                # Download and consolidate chunks from S3
                with open(local_consolidated_path, 'wb') as f_out:
                    for chunk_num in range(chunk_count):
                        chunk_path = f"{self.s3_prefix}/chunks/{year}-chunk{chunk_num:04d}.ndjson"
                        try:
                            with self.s3.open(chunk_path, 'rb') as f_in:
                                for line in f_in:
                                    f_out.write(line)
                        except Exception as e:
                            logging.error(f"Failed to read chunk {chunk_num} from S3: {str(e)}")
                            continue
                
                # Upload consolidated file to S3
                try:
                    self.s3.put(str(local_consolidated_path), s3_consolidated_path)
                    logging.info(f"Uploaded consolidated {year}.ndjson to S3")
                except Exception as e:
                    logging.error(f"Failed to upload consolidated file to S3: {str(e)}")
                
                # Clean up S3 chunks
                for chunk_num in range(chunk_count):
                    chunk_path = f"{self.s3_prefix}/chunks/{year}-chunk{chunk_num:04d}.ndjson"
                    try:
                        self.s3.rm(chunk_path)
                    except Exception as e:
                        logging.error(f"Failed to delete chunk {chunk_num} from S3: {str(e)}")
                
                # Clean up local files
                local_consolidated_path.unlink(missing_ok=True)
            else:
                # Local consolidation only
                local_consolidated_path = self.base_path / f"{year}.ndjson"
                with open(local_consolidated_path, 'wb') as f_out:
                    for chunk_num in range(chunk_count):
                        chunk_path = self.chunk_dir / f"{year}-chunk{chunk_num:04d}.ndjson"
                        try:
                            with open(chunk_path, 'rb') as f_in:
                                for line in f_in:
                                    f_out.write(line)
                        except Exception as e:
                            logging.info(f"Skipping chunk {chunk_num}: {str(e)}")
                            continue
                
                logging.info(f"Created local consolidated file at {local_consolidated_path}")
            
            # Clean up local chunks regardless of sync mode
            # for chunk_num in range(chunk_count):
            #     local_chunk = self.chunk_dir / f"{year}-chunk{chunk_num:04d}.ndjson"
            #     local_chunk.unlink(missing_ok=True)
            
            logging.info(f"Successfully consolidated {year} with {chunk_count} chunks")
        except Exception as e:
            logging.error(f"Failed to consolidate {year}: {str(e)}")


class S3ProgressTracker:
    def __init__(self, s3_fs, s3_base_path):
        self.s3 = s3_fs
        self.s3_base = s3_base_path
        self.progress_file = f"{self.s3_base}/progress.json"
        self.progress = self._load_progress()

    def _load_progress(self):
        try:
            if self.s3.exists(self.progress_file):
                with self.s3.open(self.progress_file, 'rb') as f:
                    return orjson.loads(f.read())
            return {}
        except Exception as e:
            logging.warning(f"Failed to load progress file: {str(e)}. Starting with empty progress.")
            return {}

    def get_last_batch(self, region):
        return self.progress.get(region, -1)

    def update(self, region, batch):
        logging.info(f"Updating S3 progress for {region}: completed batch {batch}")
        self.progress[region] = batch
        self._sync_to_s3()

    def _sync_to_s3(self):
        try:
            with NamedTemporaryFile(mode='w+b') as tmp:
                tmp.write(orjson.dumps(self.progress))
                tmp.flush()
                tmp.seek(0)
                self.s3.put(tmp.name, self.progress_file)
                logging.info(f"Progress file updated on S3: {self.progress_file}")
        except Exception as e:
            logging.error(f"Failed to update progress file: {str(e)}")

class LocalProgressTracker:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.progress_file = self.base_path / "progress.json"
        self.progress = self._load_progress()

    def _load_progress(self):
        try:
            if self.progress_file.exists():
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            return {}
        except Exception as e:
            logging.warning(f"Failed to load local progress file: {str(e)}. Starting with empty progress.")
            return {}

    def get_last_batch(self, region):
        return self.progress.get(region, -1)

    def update(self, region, batch):
        logging.info(f"Updating local progress for {region}: completed batch {batch}")
        self.progress[region] = batch
        self._save_local()

    def _save_local(self):
        try:
            with open(self.progress_file, 'w') as f:
                json.dump(self.progress, f)
            logging.info(f"Progress file updated locally: {self.progress_file}")
        except Exception as e:
            logging.error(f"Failed to update local progress file: {str(e)}")

def generate_items(regions_path, workers=4, sync=False, batch_size=200, reingest=False):
    s3_read = s3fs.S3FileSystem(anon=True, client_kwargs={'region_name': 'us-west-2'})
    s3_write = s3fs.S3FileSystem(anon=False, client_kwargs={'region_name': 'us-west-2'})

    if (task_id := int(os.environ.get("COILED_BATCH_TASK_ID", "-1"))) >= 0:
        region_paths = s3_read.ls(regions_path)
        current_region = f"s3://{region_paths[task_id]}" if task_id < len(region_paths) else None
    else:
        current_region = regions_path

    if not current_region:
        logging.info("No region to process")
        return

    s3_target = "s3://its-live-data/test-space/stac_catalogs"
    s3_regions_path = "s3://its-live-data/velocity_image_pair"
    region_id = os.path.relpath(current_region, start=s3_regions_path).strip("/")
    output_path = Path(region_id)
    output_path.mkdir(parents=True, exist_ok=True)

    file_handler = ChunkedFileHandler(output_path, f"{s3_target}/{region_id}", s3_write)
    progress_tracker = S3ProgressTracker(s3_write, f"{s3_target}/{region_id}") if sync else LocalProgressTracker(output_path)

    last_batch = progress_tracker.get_last_batch(current_region)

    # Handle reingest flag
    if reingest:
        logging.info("Reingest flag set - resetting progress and cleaning up existing data")
        last_batch = -1
        
        # Reset progress tracking
        if sync:
            progress_tracker.progress = {}
            progress_tracker._sync_to_s3()
            # Clean up S3 chunks if they exist
            try:
                chunk_prefix = f"{s3_target}/{region_id}/chunks/"
                existing_chunks = s3_write.glob(f"{chunk_prefix}*.ndjson")
                for chunk in existing_chunks:
                    s3_write.rm(chunk)
                logging.info(f"Cleaned up {len(existing_chunks)} S3 chunks")
            except Exception as e:
                logging.error(f"Error cleaning S3 chunks: {str(e)}")
        else:
            # Clean local progress file
            progress_tracker.progress_file.unlink(missing_ok=True)
            progress_tracker.progress = {}
        
        # Clean local chunks directory
        if (output_path / "chunks").exists():
            for chunk_file in (output_path / "chunks").glob("*.ndjson"):
                chunk_file.unlink(missing_ok=True)
            logging.info("Cleaned up local chunks")
        
        # Clean any existing consolidated files
        for year_file in output_path.glob("*.ndjson"):
            year_file.unlink(missing_ok=True)
            logging.info(f"Removed existing consolidated file {year_file.name}")
    else:
        last_batch = progress_tracker.get_last_batch(current_region)

    logging.info(f"Starting from batch {last_batch + 1} for region {region_id}")

    try:
        client = Client.current()
        cluster = client.cluster
    except Exception:
        cluster = LocalCluster(n_workers=workers, threads_per_worker=2)
        client = Client(cluster)

    batch_number = 0
    for batch in list_s3_objects(current_region, pattern="*.nc", batch_size=batch_size):
        if batch_number <= last_batch and not reingest:
            if len(batch) < batch_size:
                logging.info(f"All batches processed for {current_region}")
            else:
                logging.info(f"Skipping already processed batch {batch_number} with {len(batch)} items")
            batch_number += 1
            continue
        else:
            try:
                logging.info(f"Processing batch {batch_number} with {len(batch)} files")
                futures = [client.submit(generate_stac_metadata, url) for url in batch]
                progress(futures)
                features = client.gather(futures)

                for feature in features:
                    file_handler.write_feature(feature)

                file_handler.upload_batch_chunks(sync)
                progress_tracker.update(current_region, batch_number)
                file_handler.rotate_chunks()
                batch_number += 1
                trim_memory()
            except Exception as e:
                logging.error(f"Batch {batch_number} failed: {str(e)}")
                batch_number += 1
    client.close()

    for year in file_handler.tracker:
        logging.info(f"Consolidating chunks for year {year}")
        file_handler.consolidate_year(year, sync=sync)
   
    # Rest of your cleanup code
    if sync:
        for item in output_path.glob("*"):
            if item.is_file():
                item.unlink()
        for subdir in output_path.glob("*/"):
            for f in subdir.glob("*"):
                f.unlink()
            subdir.rmdir()
        output_path.rmdir()

    # Clean up S3FS clients
    s3_read.clear_instance_cache()
    s3_write.clear_instance_cache()

def generate_stac_catalog():
    parser = argparse.ArgumentParser(description="Generate ITS_LIVE STAC metadata")
    parser.add_argument("-p", "--path", required=True, help="Path to ITS_LIVE data")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Dask workers")
    parser.add_argument("-b", "--batch", type=int, default=20000, help="Batch size")
    parser.add_argument("-s", "--sync", action="store_true", help="Sync to S3")
    parser.add_argument("-r", "--reingest", action="store_true", 
                       help="Reset progress and reingest all data from scratch")


    args = parser.parse_args()
    generate_items(
        regions_path=args.path,
        workers=args.workers,
        sync=args.sync,
        batch_size=args.batch,
        reingest=args.reingest
    )

if __name__ == "__main__":
    generate_stac_catalog()

