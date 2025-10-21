#!/usr/bin/env python

import subprocess
import argparse
import tempfile
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any
import orjson
import os
from tqdm import tqdm
import h3
import logging
import boto3
import botocore
from collections import defaultdict
import rustac
from obstore.store import S3Store
import asyncio


def setup_logging(level):
    """Configure logging with specified level"""
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=level,
    )
    logger = logging.getLogger(__name__)

    # Suppress verbose library logs
    logging.getLogger("rustac").setLevel(logging.WARNING)
    logging.getLogger("obstore").setLevel(logging.WARNING)

    return logger


def latlon_to_tile_id(lat: float, lon: float) -> str:
    lat_center = int(round(lat / 10.0)) * 10
    lon_center = int(round(lon / 10.0)) * 10
    lat_center = max(-80, min(80, lat_center))
    lon_center = max(-180, min(170, lon_center))

    lat_prefix = (
        f"N{abs(lat_center):02d}" if lat_center >= 0 else f"S{abs(lat_center):02d}"
    )
    lon_prefix = (
        f"E{abs(lon_center):03d}" if lon_center >= 0 else f"W{abs(lon_center):03d}"
    )
    return f"{lat_prefix}{lon_prefix}"


def h3_tile(lat, lon, resolution=7):
    return h3.latlng_to_cell(lat, lon, resolution)


def extract_mission(href: str) -> str:
    import re

    match = re.search(r"velocity_image_pair/([^/]+)/", href)
    return match.group(1) if match else "unknown"


def extract_year(stac_item: Dict[str, Any]) -> str:
    dt_str = stac_item.get("properties", {}).get("datetime")
    if dt_str and dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    if dt_str:
        return str(datetime.fromisoformat(dt_str).year)
    return "unknown"


def batch_items_to_disk(
    items, batch_dir, tiling_system, mission, tile_id, year, format="ndjson"
):
    nested_dir = Path(batch_dir) / tiling_system / mission / tile_id
    nested_dir.mkdir(parents=True, exist_ok=True)

    if format == "geoparquet":
        out_file = nested_dir / f"{year}.ndjson"  # Still write as ndjson initially
        with open(out_file, "ab") as f:
            f.write(b"".join(items))
    else:  # ndjson format
        out_file = nested_dir / f"{year}.ndjson"
        with open(out_file, "ab") as f:
            f.write(b"".join(items))


async def translate_ndjson_to_geoparquet_local(local_ndjson_path, local_output_dir):
    """
    Translate local ndjson file to geoparquet and save locally using rustac library.
    """
    try:
        # Read the local ndjson file using rustac
        local_path_str = str(local_ndjson_path)
        stac_data = await rustac.read(local_path_str, store=None)  # None for local file

        # Create output path with .parquet extension, preserving relative structure
        # Calculate relative path from batch_dir
        relative_path = local_ndjson_path.relative_to(
            local_ndjson_path.parent.parent.parent.parent
        )
        parquet_output_path = Path(local_output_dir) / relative_path
        parquet_output_path = parquet_output_path.with_suffix(".parquet")

        # Create parent directories
        parquet_output_path.parent.mkdir(parents=True, exist_ok=True)

        # Convert the path to string for rustac
        parquet_output_str = str(parquet_output_path)

        # Write data as geoparquet locally
        await rustac.write(parquet_output_str, stac_data, format="parquet", store=None)

        # Log success only if verbose enough
        if logging.getLogger().isEnabledFor(logging.INFO):
            logger.info(
                f"Successfully converted {local_ndjson_path} to {parquet_output_path}"
            )
        return True

    except Exception as e:
        logger.error(f"Failed to translate {local_ndjson_path} to geoparquet: {str(e)}")
        return False


def convert_local_ndjson_to_geoparquet(local_root: str, local_output_dir: str):
    """
    Convert all local ndjson files to geoparquet files and save locally.
    """
    ndjson_files = list(Path(local_root).rglob("*.ndjson"))

    logger.info(f"Converting {len(ndjson_files)} ndjson files to geoparquet")

    # Process each ndjson file
    successful = 0
    failed = 0

    for ndjson_file in tqdm(ndjson_files, desc="Converting to GeoParquet"):
        try:
            success = asyncio.run(
                translate_ndjson_to_geoparquet_local(ndjson_file, local_output_dir)
            )
            if success:
                successful += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            logger.error(f"Error processing {ndjson_file}: {str(e)}")

    logger.info(
        f"GeoParquet conversion complete: {successful} successful, {failed} failed"
    )


def list_s3_files(bucket: str, prefix: str, suffix=".json"):
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    all_files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(suffix):
                all_files.append(f"s3://{bucket}/{key}")
    return all_files


def copy_s3_local(
    bucket: str,
    prefix: str,
    suffix: str = ".json",
    local_path: str = ".",
):
    if not bucket or not prefix:
        raise ValueError("Bucket and prefix must be non-empty strings.")
    if not isinstance(suffix, str) or not suffix.startswith("."):
        raise ValueError("Suffix must be a string starting with '.' (e.g., '.json').")

    local_dir = Path(local_path)
    local_dir.mkdir(parents=True, exist_ok=True)

    s3_source = f"s3://{bucket}/{prefix}"
    if not s3_source.endswith("/"):
        s3_source += "/"

    cmd = [
        "aws",
        "s3",
        "cp",
        s3_source,
        str(local_path),
        "--recursive",
        "--no-sign-request",
    ]

    logger.info(
        f"Copying files from {s3_source} to {local_path} with suffix '{suffix}'..."
    )

    env = os.environ.copy()
    env["AWS_MAX_CONCURRENT_REQUESTS"] = "256"  # number of concurrent S3 requests
    env["AWS_MAX_IO_THREADS"] = "64"  # number of threads for I/O

    try:
        # Use subprocess.Popen for real-time output processing
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=env,
        )

        # Count files as they're downloaded
        file_count = 0
        pbar = tqdm(
            unit=" items ", dynamic_ncols=True, desc="Fetching remote stac items"
        )

        # Process stdout line by line in real-time
        for line in process.stdout:
            if "download:" in line or "copy:" in line:
                file_count += 1
                pbar.update(1)

        # Close progress bar
        pbar.close()

        # Wait for process to complete and check return code
        process.wait()
        if process.returncode != 0:
            error_output = process.stderr.read() if process.stderr else "Unknown error"
            raise RuntimeError(
                f"Copy failed with return code {process.returncode}: {error_output}"
            )

    except FileNotFoundError:
        raise RuntimeError(
            "AWS CLI not found. Please install it: https://awscli.amazonaws.com/"
        )

    # Count downloaded files
    downloaded_files = list(local_dir.rglob(f"*{suffix}"))
    total_downloaded = len(downloaded_files)

    logger.info(
        f"\n✅ Copy complete. Total files downloaded with suffix '{suffix}': {total_downloaded}"
    )
    return total_downloaded


# -------------------
# Ingest function
# -------------------
def ingest_stac_files(cfg):
    # List STAC files
    if cfg.source.startswith("s3://"):
        bucket, prefix = cfg.source[5:].split("/", 1)
        logger.info(f"Listing files in S3 bucket {bucket} with prefix {prefix}")
        # all_files = list_s3_files(bucket, prefix, ".json")
        # ALWAYS sync, faster than reading each from S3
        if not cfg.skip_download:
            copy_s3_local(
                bucket=bucket,
                prefix=prefix,
                suffix=".json",
                local_path=cfg.tmp_sync_dir,
            )
        # Process files from the sync directory
        all_files = list(Path(cfg.tmp_sync_dir).glob("*.json"))
        fs_read = None
    else:
        all_files = list(Path(cfg.source).glob("*.json"))
        fs_read = None

    buffers = {}  # buffers[tiling_system][mission][tile_id][year] = list[bytes]

    # Flush year.ndjson after it accumulates more than 10k items
    FLUSH_THRESHOLD = 10_000

    # Keep counters for each buffer to know when to flush
    buffer_counts = defaultdict(int)

    # Process files in parallel
    with ThreadPoolExecutor(max_workers=cfg.workers) as exe:
        futures = {}
        for file_path in all_files:
            futures[exe.submit(read_stac_item, file_path, fs_read)] = file_path

        for future in tqdm(
            as_completed(futures), total=len(all_files), desc="Processing files"
        ):
            stac_item = future.result()
            props = stac_item.get("properties", {})
            lat = props.get("latitude")
            lon = props.get("longitude")
            if lat is None or lon is None:
                continue

            year = extract_year(stac_item)
            mission = extract_mission(
                stac_item.get("assets", {}).get("data", {}).get("href", "")
            )

            for tiling_system, tile_func in [
                ("latlon", latlon_to_tile_id),
                ("h3", h3_tile),
            ]:
                tile_id = tile_func(lat, lon)

                buf = (
                    buffers.setdefault(tiling_system, {})
                    .setdefault(mission, {})
                    .setdefault(tile_id, {})
                    .setdefault(year, [])
                )
                buf.append(orjson.dumps(stac_item) + b"\n")
                buffer_counts[(tiling_system, mission, tile_id, year)] += 1

                # Flush when threshold is reached
                if (
                    buffer_counts[(tiling_system, mission, tile_id, year)]
                    >= FLUSH_THRESHOLD
                ):
                    items = buf[:]
                    buf.clear()
                    buffer_counts[(tiling_system, mission, tile_id, year)] = 0

                    batch_items_to_disk(
                        items,
                        cfg.batch_dir,
                        tiling_system,
                        mission,
                        tile_id,
                        year,
                        format=cfg.output_format,
                    )
        # Final flush of any remaining items
        for tiling_system, missions in buffers.items():
            for mission, tiles in missions.items():
                for tile_id, years in tiles.items():
                    for year, items in years.items():
                        if not items:
                            continue
                        batch_items_to_disk(
                            items,
                            cfg.batch_dir,
                            tiling_system,
                            mission,
                            tile_id,
                            year,
                            format=cfg.output_format,
                        )

    # Convert to geoparquet if requested and output is local
    if (
        cfg.output_format == "geoparquet"
        and cfg.output
        and not cfg.output.startswith("s3://")
    ):
        convert_local_ndjson_to_geoparquet(str(cfg.batch_dir), cfg.output)


def read_stac_item(file_path, fs=None):
    if isinstance(file_path, Path):
        with open(file_path, "rb") as f:
            return orjson.loads(f.read())
    else:
        with fs.open(file_path, "rb") as f:
            return orjson.loads(f.read())


def sync_append(local_root: str, s3_prefix: str, format="ndjson"):
    """
    Walks local_root recursively.
    For each file: if it exists in S3, download, append local content, and upload back.
                   if it doesn't exist, just upload.
    """
    s3 = boto3.client("s3")
    bucket, key_prefix = s3_prefix[5:].split("/", 1)

    for path in Path(local_root).rglob("*.ndjson"):
        rel_path = path.relative_to(local_root)
        key = f"{key_prefix}/{rel_path.as_posix()}"

        if format == "geoparquet":
            # Handle GeoParquet translation and upload using rustac
            try:
                success = asyncio.run(
                    translate_ndjson_to_geoparquet_s3(path, bucket, key)
                )
                if not success:
                    logger.error(f"Failed to process {path}")
            except Exception as e:
                logger.error(f"Error processing {path}: {str(e)}")
        else:  # ndjson format
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_path = Path(tmp_file.name)

            try:
                # try to download existing file
                try:
                    s3.download_file(bucket, key, str(tmp_path))
                    # append local content
                    with open(tmp_path, "ab") as f:
                        f.write(path.read_bytes())
                except botocore.exceptions.ClientError as e:
                    code = e.response.get("Error", {}).get("Code", "")
                    if code not in ("404", "NoSuchKey", "NotFound"):
                        raise
                    # doesn't exist — just copy local file
                    tmp_path.write_bytes(path.read_bytes())

                # upload merged or new file
                s3.upload_file(str(tmp_path), bucket, key)
            finally:
                tmp_path.unlink(missing_ok=True)


async def translate_ndjson_to_geoparquet_s3(local_file_path, s3_bucket, s3_key):
    """
    Translate local ndjson file to geoparquet and upload to S3 using rustac library.
    """
    try:
        # Create S3 store for reading (source)
        read_store = S3Store(
            bucket=s3_bucket,
            prefix="",  # Empty prefix as we're working with specific keys
            region="us-west-2",  # Adjust region as needed
            client_options={"timeout": "4m"},
            skip_signature=True,
        )
        # Create S3 store for writing (destination)
        write_store = S3Store(
            bucket=s3_bucket,
            prefix="",  # Empty prefix as we're working with specific keys
            region="us-west-2",  # Adjust region as needed
            client_options={"timeout": "8m"},
        )
        # Read the local ndjson file using rustac
        local_path_str = str(local_file_path)
        stac_data = await rustac.read(local_path_str, store=None)

        # Convert s3_key from .ndjson to .parquet
        parquet_s3_key = s3_key.replace(".ndjson", ".parquet")
        parquet_s3_path = f"s3://{s3_bucket}/{parquet_s3_key}"

        # Normalize new data to features list
        if stac_data["type"] == "Feature":
            new_features = [stac_data]
        elif stac_data["type"] == "FeatureCollection":
            new_features = stac_data["features"]
        else:
            new_features = [stac_data]

        # Try to read existing parquet file from S3 and append
        try:
            existing_data = await rustac.read(parquet_s3_path, store=read_store)

            # Extract existing features
            if existing_data["type"] == "Feature":
                old_features = [existing_data]
            elif existing_data["type"] == "FeatureCollection":
                old_features = existing_data["features"]
            else:
                old_features = []

            # Append new features to old features
            merged_features = old_features + new_features

        except Exception:
            # If file doesn't exist or can't be read, use new data only
            merged_features = new_features

        # Write merged features to S3 as geoparquet
        await rustac.write(
            parquet_s3_path,
            merged_features,
            format="parquet",
            store=write_store,
        )

        return True
    except Exception as e:
        logger.error(
            f"Failed to translate and upload {local_file_path} to {s3_key}: {str(e)}"
        )
        return False


class Config:
    def __init__(
        self,
        source,
        output=None,
        batch_dir=Path("./batches"),
        sync_data="",
        skip_download=False,
        skip_upload=False,
        workers=8,
        output_format="ndjson",
        log_level="ERROR",
    ):
        self.source = source
        self.output = output
        self.batch_dir = (
            Path(output)
            if output and not output.startswith("s3://")
            else Path(batch_dir)
        )
        self.skip_download = skip_download
        self.skip_upload = skip_upload
        self.workers = workers
        self.tmp_sync_dir = Path(sync_data)
        self.output_format = output_format
        self.log_level = log_level

        self.tmp_sync_dir.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument(
        "--sync-directory",
        default="/tmp/stac_ingest/local_synced_data/",
        help="Local directory to store batches",
    )
    parser.add_argument("--output")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument(
        "--workers", type=int, default=8, help="Number of worker threads"
    )
    parser.add_argument(
        "--output-format",
        choices=["ndjson", "geoparquet"],
        default="ndjson",
        help="Output format: ndjson or geoparquet",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="ERROR",
        help="Set logging level (default: ERROR)",
    )

    args = parser.parse_args()

    # Setup logging with the specified level
    logger = setup_logging(getattr(logging, args.log_level))

    cfg = Config(
        args.source,
        output=args.output,
        skip_download=args.skip_download,
        skip_upload=args.skip_upload,
        workers=args.workers,
        output_format=args.output_format,
        sync_data=args.sync_directory,
        log_level=args.log_level,
    )
    ingest_stac_files(cfg)
