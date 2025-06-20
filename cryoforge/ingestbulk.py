import argparse
import logging
import os
import json
import warnings
import hashlib
import datetime
from pathlib import Path
from collections import defaultdict
import orjson
from dask.distributed import Client, LocalCluster, progress
import s3fs
from .generate import generate_itslive_metadata
from .tooling import list_s3_objects, trim_memory

def generate_stac_metadata(url: str):
    try:
        metadata = generate_itslive_metadata(url)
    except Exception as e:
        logging.error(f"Failed to generate STAC metadata for {url}: {str(e)}")
        return {}
    return metadata["stac"]

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)
warnings.filterwarnings("ignore", category=ResourceWarning)

class RegionTracker:
    """Implementation with sync validation, counter handling, batch control,
    chunk metadata updates, per-year chunk consolidation (with S3 sync/local cleanup),
    and progress file support.
    If a progress.json file is found in the region directory, its counter is used
    to initialize the start batch.
    """
    def __init__(self, base_path, s3_prefix, s3_fs):
        self.base_path = Path(base_path)
        self.s3_prefix = s3_prefix
        self.s3 = s3_fs
        self.chunk_dir = self.base_path / "chunks"
        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        # Track chunks written in the current batch.
        self.current_batch_chunks = set()
        
        self.metadata = {
            "last_batch": -1,
            "last_update": None,
            "total_files_processed": 0,
            "chunks": defaultdict(list),
            "counters": defaultdict(int),
            "version": "1.4"
        }
        self._initialize_metadata()
        self._load_progress_file()

    def _initialize_metadata(self):
        """Initialize metadata with sync checks and batch control"""
        self._load_existing_metadata()
        
        # Perform S3 sync validation if enabled.
        if self.s3_prefix:
            self._validate_s3_sync()

    def _load_progress_file(self):
        """
        Check for a progress.json file in the region directory.
        If found, read the counter value and update self.metadata["last_batch"].
        The file should contain a JSON object with at least the key "counter".
        """
        progress_file = f"{self.s3_prefix}/progress.json".replace("s3://", "")
        if self.s3.exists(progress_file):
            try:
                with self.s3.open(progress_file, "rb") as pf:
                    progress_data = json.loads(pf.read())
                for key, value in progress_data.items():
                    counter = int(value)
                    logging.info(f"Found progress file {progress_file}. "
                                 f"Starting processing from batch {counter + 1} (skipping up to {counter}).")
                    self.metadata["last_batch"] = counter
                    break
                else:
                    logging.info(f"Progress file {progress_file} does not contain a {self.s3_prefix} key.")
            except Exception as e:
                logging.info(f"Failed to load progress file {progress_file}: {str(e)}")

    def _load_existing_metadata(self):
        """Load metadata from local or S3"""
        local_file = self.base_path / "region_metadata.json"

        if self.s3_prefix:
            try:
                s3_path = f"{self.s3_prefix}/region_metadata.json".replace("s3://", "")
                if self.s3.exists(s3_path):
                    with self.s3.open(s3_path, 'rb') as f:
                        s3_meta = json.loads(f.read())
                        logging.info(f"Loading metadata from S3: {s3_path}, last_batch: {s3_meta['last_batch']}")
                        self.metadata.update(s3_meta)
                        local_file.write_text(json.dumps(s3_meta, indent=2))
            except Exception as e:
                logging.warning(f"S3 metadata load failed: {str(e)}")

    def _validate_s3_sync(self):
        """Validate and sync metadata with S3 chunks"""
        s3_chunks = defaultdict(list)
        chunk_prefix = f"{self.s3_prefix}/chunks/".replace("s3://", "")
        
        try:
            for chunk_path in self.s3.glob(f"{chunk_prefix}*.ndjson"):
                try:
                    name = os.path.basename(chunk_path)
                    year, chunk_part = name.split('-chunk')
                    chunk_num = int(chunk_part.split('.')[0])
                    s3_chunks[year].append(chunk_num)
                except ValueError:
                    continue
        except Exception as e:
            logging.error(f"S3 chunk scan failed: {str(e)}")
            return

        needs_rebuild = False
        for year, chunks in s3_chunks.items():
            meta_chunks = [c['id'] for c in self.metadata['chunks'].get(year, [])]
            meta_nums = [int(c.split('-chunk')[1]) for c in meta_chunks if '-chunk' in c]
            if chunks and (not meta_nums or max(chunks) > max(meta_nums)):
                logging.warning(f"Metadata out of sync for {year}: "
                                f"S3 chunks up to {max(chunks)}, metadata up to {max(meta_nums or [0])}")
                needs_rebuild = True
        
        if needs_rebuild or not self.metadata['chunks']:
            self._rebuild_metadata(s3_chunks)

    def _rebuild_metadata(self, s3_chunks):
        """
        Rebuild metadata from S3 chunks. For each chunk, if a corresponding local
        file exists, use its file modification time as the timestamp and compute its
        size, MD5 checksum, and item count.
        """
        logging.info("Rebuilding metadata from S3 chunks")
        self.metadata['chunks'].clear()
        self.metadata['counters'].clear()
        
        for year, chunks in s3_chunks.items():
            max_chunk = max(chunks) if chunks else 0
            self.metadata['chunks'][year] = []
            for c in range(max_chunk + 1):
                chunk_id = f"{year}-chunk{c:04d}"
                file_path = self.chunk_dir / f"{chunk_id}.ndjson"
                timestamp = ""
                size_bytes = 0
                md5_hash = ""
                item_count = 0
                if file_path.exists():
                    timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                    size_bytes = os.path.getsize(file_path)
                    md5_hash = self.compute_md5(file_path)
                    item_count = self.count_items(file_path)
                self.metadata['chunks'][year].append({
                    "id": chunk_id,
                    "timestamp": timestamp,
                    "size_bytes": size_bytes,
                    "md5_hash": md5_hash,
                    "item_count": item_count
                })
            self.metadata['counters'][year] = max_chunk + 1
        
        self._save_metadata(sync_immediately=True)

    def compute_md5(self, file_path):
        """Compute MD5 hash for the file contents."""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def count_items(self, file_path):
        """Count non-empty lines in the file (each line is one item)."""
        count = 0
        with open(file_path, "rb") as f:
            for line in f:
                if line.strip():
                    count += 1
        return count

    def update_chunk_info(self):
        """Update metadata for each chunk in the current batch based on the local file."""
        for year, chunk_num in self.current_batch_chunks:
            file_path = self.chunk_dir / f"{year}-chunk{chunk_num:04d}.ndjson"
            if file_path.exists():
                size_bytes = os.path.getsize(file_path)
                md5_hash = self.compute_md5(file_path)
                item_count = self.count_items(file_path)
                if year in self.metadata['chunks']:
                    for entry in self.metadata['chunks'][year]:
                        if entry["id"] == f"{year}-chunk{chunk_num:04d}":
                            entry["size_bytes"] = size_bytes
                            entry["md5_hash"] = md5_hash
                            entry["item_count"] = item_count
                            entry["timestamp"] = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()
                            break

    def process_batch(self, batch_num, features, sync):
        """Process a batch of features, write them as a new chunk, update metadata, and optionally sync to S3."""
        for feature in features:
            if not feature or feature=={}:
                continue
            year = feature.properties["mid_datetime"][:4]
            # Ensure the year is initialized in the counters.
            if year not in self.metadata['counters']:
                self.metadata['counters'][year] = 0
                self.metadata['chunks'][year] = []
            chunk_num = self.metadata['counters'][year]
            path = self.chunk_dir / f"{year}-chunk{chunk_num:04d}.ndjson"
            self.current_batch_chunks.add((year, chunk_num))
            
            with open(path, 'ab') as f:
                f.write(orjson.dumps(feature.to_dict()) + b"\n")

        if sync:
            self._upload_chunks()
        
        self.update_chunk_info()
        
        self.metadata["last_batch"] = batch_num
        self.metadata["total_files_processed"] += len(features)
        self._rotate_counters()
        self._save_metadata(sync_immediately=sync)

    def _upload_chunks(self):
        """Upload current chunk files to S3."""
        for year, chunk_num in self.current_batch_chunks:
            local_path = self.chunk_dir / f"{year}-chunk{chunk_num:04d}.ndjson"
            s3_path = f"{self.s3_prefix}/chunks/{local_path.name}".replace("s3://", "")
            if not self.s3.exists(s3_path):
                try:
                    self.s3.put(str(local_path), s3_path)
                    logging.info(f"Uploaded {local_path.name} to S3 at {s3_path}")
                except Exception as e:
                    logging.error(f"Failed to upload {local_path.name}: {str(e)}")

    def _rotate_counters(self):
        """Advance counters for processed chunks and clear the current batch tracker."""
        for year, chunk_num in self.current_batch_chunks:
            self.metadata['counters'][year] = chunk_num + 1
        self.current_batch_chunks.clear()

    def _save_metadata(self, sync_immediately=False):
        """Persist metadata locally, and optionally upload to S3."""
        self.metadata['last_update'] = datetime.datetime.now().isoformat()
        local_path = self.base_path / "region_metadata.json"
        try:
            with open(local_path, 'w') as f:
                json.dump(self.metadata, f, indent=2)
            if sync_immediately and self.s3_prefix:
                s3_path = f"{self.s3_prefix}/region_metadata.json".replace("s3://", "")
                self.s3.put(str(local_path), s3_path)
        except Exception as e:
            logging.error(f"Metadata save failed: {str(e)}")

    def sync_remote_chunks_to_local(self):
        """
        Download remote chunk files from S3 to the local chunk directory if they do not exist locally.
        """
        if not self.s3_prefix:
            logging.info("No S3 prefix defined, skipping remote chunk sync.")
            return

        remote_chunk_prefix = f"{self.s3_prefix}/chunks/".replace("s3://", "")
        try:
            remote_files = self.s3.glob(f"{remote_chunk_prefix}*-chunk*.ndjson")
            for remote_file in remote_files:
                local_file = self.chunk_dir / os.path.basename(remote_file)
                if not local_file.exists():
                    try:
                        self.s3.get(remote_file, str(local_file))
                        logging.info(f"Synced remote chunk {os.path.basename(remote_file)} to local")
                    except Exception as e:
                        logging.error(f"Failed to sync remote chunk {os.path.basename(remote_file)}: {str(e)}")
        except Exception as e:
            logging.error(f"Failed to list remote chunks for sync: {str(e)}")


    def consolidate_chunks(self, sync=False):
        """
        First, if sync is True, run remote chunk sync to download remote files to local.
        Then, consolidate per-year chunk files into a single {year}.ndjson file.
        If sync is True, the consolidated file is uploaded to S3 and then removed locally.
        """
        # If sync is on, run the remote chunk sync first.

        # Gather all local chunk files.
        year_files = {}
        for file in self.chunk_dir.glob("*-chunk*.ndjson"):
            filename = file.name  # e.g. "2020-chunk0001.ndjson"
            try:
                year, chunk_part = filename.split('-chunk')
                chunk_num = int(chunk_part.split('.')[0])
            except ValueError:
                logging.info(f"Skipping file {filename}: unable to parse year and chunk number")
                continue
            year_files.setdefault(year, []).append((chunk_num, file))
        
        for year, files in year_files.items():
            files.sort(key=lambda x: x[0])
            consolidated_file = self.chunk_dir / f"{year}.ndjson"
            with open(consolidated_file, "wb") as outfile:
                for _, fpath in files:
                    with open(fpath, "rb") as infile:
                        outfile.write(infile.read())
            logging.info(f"Consolidated {len(files)} chunk files into {consolidated_file}")

            if sync and self.s3_prefix:
                s3_consolidated_path = f"{self.s3_prefix}/{consolidated_file.name}".replace("s3://", "")
                try:
                    self.s3.put(str(consolidated_file), s3_consolidated_path)
                    logging.info(f"Uploaded consolidated file {consolidated_file.name} to S3 at {s3_consolidated_path}")
                    consolidated_file.unlink()
                    logging.info(f"Deleted local consolidated file {consolidated_file.name}")
                except Exception as e:
                    logging.info("Failed to upload consolidated file to S3: %s", str(e))


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
    region_id = os.path.relpath(current_region, start="s3://its-live-data/velocity_image_pair").strip("/")
    output_path = Path(region_id)
    output_path.mkdir(parents=True, exist_ok=True)

    region_tracker = RegionTracker(output_path, f"{s3_target}/{region_id}", s3_write)
    if sync:
        region_tracker.sync_remote_chunks_to_local()
    

    if reingest:
        region_tracker.metadata = {
            "last_batch": -1,
            "last_update": datetime.datetime.now().isoformat(),
            "total_files_processed": 0,
            "chunks": defaultdict(list),
            "counters": defaultdict(int),
            "version": "1.4"
        }
        region_tracker._save_metadata(sync_immediately=sync)

    client = Client(LocalCluster(n_workers=workers, threads_per_worker=2))
    last_batch = region_tracker.metadata["last_batch"]
    logging.info("Starting batch processing from batch %d", last_batch + 1)
    
    for batch_num, batch in enumerate(list_s3_objects(current_region, pattern="*.nc", batch_size=batch_size)):
        if batch_num <= last_batch and not reingest:
            logging.info(f"Skipping batch {batch_num} (already processed)")
            continue

        logging.info(f"Processing batch {batch_num} with {len(batch)} files")
        futures = [client.submit(generate_stac_metadata, url) for url in batch]
        progress(futures)
        region_tracker.process_batch(
            batch_num,
            client.gather(futures),
            sync
        )
        trim_memory()

    client.close()
    region_tracker.consolidate_chunks(sync=sync)

def generate_stac_catalog():
    parser = argparse.ArgumentParser(description="Generate ITS_LIVE STAC metadata")
    parser.add_argument("-p", "--path", required=True, help="Path to ITS_LIVE data")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Dask workers")
    parser.add_argument("-b", "--batch", type=int, default=200, help="Batch size")
    parser.add_argument("-s", "--sync", action="store_true", help="Sync to S3")
    parser.add_argument("-r", "--reingest", action="store_true", help="Reset progress and reingest all data")
    
    args = parser.parse_args()
    
   
    generate_items(
        regions_path=args.path,
        workers=args.workers,
        sync=args.sync,
        batch_size=args.batch,
        reingest=args.reingest,
    )

if __name__ == "__main__":
    generate_stac_catalog()

