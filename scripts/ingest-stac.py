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


def batch_items_to_disk(items, batch_dir, tiling_system, mission, tile_id, year):
    """Write items to disk as ndjson"""
    nested_dir = Path(batch_dir) / tiling_system / mission / tile_id
    nested_dir.mkdir(parents=True, exist_ok=True)

    out_file = nested_dir / f"{year}.ndjson"
    with open(out_file, "ab") as f:
        f.write(b"".join(items))


def copy_s3_local(
    bucket: str,
    prefix: str,
    suffix: str = ".json",
    local_path: str = ".",
):
    """Copy files from S3 to local using AWS CLI"""
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
    env["AWS_MAX_CONCURRENT_REQUESTS"] = "256"
    env["AWS_MAX_IO_THREADS"] = "64"

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True,
            env=env,
        )

        file_count = 0
        pbar = tqdm(
            unit=" items", dynamic_ncols=True, desc="Fetching remote stac items"
        )

        for line in process.stdout:
            if "download:" in line or "copy:" in line:
                file_count += 1
                pbar.update(1)

        pbar.close()

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

    downloaded_files = list(local_dir.rglob(f"*{suffix}"))
    total_downloaded = len(downloaded_files)

    logger.info(
        f"\n✅ Copy complete. Total files downloaded with suffix '{suffix}': {total_downloaded}"
    )
    return total_downloaded


def read_stac_item(file_path: Path):
    """Read a single STAC item from disk"""
    with open(file_path, "rb") as f:
        return orjson.loads(f.read())


def ingest_stac_files(cfg):
    """Main ingestion function"""
    if cfg.source.startswith("s3://"):
        bucket, prefix = cfg.source[5:].split("/", 1)
        logger.info(f"Listing files in S3 bucket {bucket} with prefix {prefix}")

        if not cfg.skip_download:
            copy_s3_local(
                bucket=bucket,
                prefix=prefix,
                suffix=".json",
                local_path=cfg.tmp_sync_dir,
            )
        all_files = list(Path(cfg.tmp_sync_dir).glob("*.json"))
    else:
        all_files = list(Path(cfg.source).glob("*.json"))

    if not all_files:
        logger.warning("No STAC files found to process")
        return

    buffers = {}  # buffers[tiling_system][mission][tile_id][year] = list[bytes]
    buffer_counts = defaultdict(int)
    FLUSH_THRESHOLD = 10_000

    # Process files in parallel
    with ThreadPoolExecutor(max_workers=cfg.workers) as exe:
        futures = {
            exe.submit(read_stac_item, file_path): file_path for file_path in all_files
        }

        for future in tqdm(
            as_completed(futures), total=len(all_files), desc="Processing files"
        ):
            try:
                stac_item = future.result()
            except Exception as e:
                logger.error(f"Failed to read {futures[future]}: {str(e)}")
                continue

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
                        items, cfg.batch_dir, tiling_system, mission, tile_id, year
                    )

    # Final flush of any remaining items
    for tiling_system, missions in buffers.items():
        for mission, tiles in missions.items():
            for tile_id, years in tiles.items():
                for year, items in years.items():
                    if items:
                        batch_items_to_disk(
                            items, cfg.batch_dir, tiling_system, mission, tile_id, year
                        )

    # Handle output
    if cfg.output:
        if cfg.output.startswith("s3://"):
            # S3 output - sync only if upload not skipped
            if not cfg.skip_upload:
                logger.info(f"Syncing local data to {cfg.output}")
                sync_to_s3(str(cfg.batch_dir), cfg.output, format=cfg.output_format)
                logger.info("✅ Sync to S3 complete")
        else:
            # Local output - convert to geoparquet if requested
            if cfg.output_format == "geoparquet":
                logger.info(f"Converting to GeoParquet in {cfg.output}")
                convert_local_ndjson_to_geoparquet(str(cfg.batch_dir), cfg.output)


def sync_to_s3(local_root: str, s3_prefix: str, format="ndjson"):
    """
    Sync local ndjson files to S3.
    For ndjson: append to existing files or create new ones.
    For geoparquet: convert and merge with existing parquet files.
    """
    s3 = boto3.client("s3")
    bucket, key_prefix = s3_prefix[5:].split("/", 1)

    ndjson_files = list(Path(local_root).rglob("*.ndjson"))

    if not ndjson_files:
        logger.warning("No files to sync")
        return

    logger.info(f"Syncing {len(ndjson_files)} files to S3")

    if format == "geoparquet":
        read_store = S3Store(
            bucket=bucket,
            prefix="",
            region="us-west-2",
            client_options={"timeout": "4m"},
            skip_signature=True,
        )
        write_store = S3Store(
            bucket=bucket,
            prefix="",
            region="us-west-2",
            client_options={"timeout": "8m"},
        )

        # Process all files in a single async context
        asyncio.run(
            process_geoparquet_batch(
                ndjson_files, local_root, bucket, key_prefix, read_store, write_store
            )
        )
    else:
        # ndjson format
        for path in tqdm(ndjson_files, desc=f"Syncing to S3 (ndjson)"):
            rel_path = path.relative_to(local_root)
            key = f"{key_prefix}/{rel_path.as_posix()}"
            try:
                upload_ndjson_with_merge(s3, path, bucket, key)
            except Exception as e:
                logger.error(f"Error processing {path}: {str(e)}")


def upload_ndjson_with_merge(s3, local_path: Path, bucket: str, key: str):
    """Upload ndjson file to S3, merging with existing file if present"""
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        try:
            s3.download_file(bucket, key, str(tmp_path))
            # Append local content
            with open(tmp_path, "ab") as f:
                f.write(local_path.read_bytes())
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code not in ("404", "NoSuchKey", "NotFound"):
                raise
            tmp_path.write_bytes(local_path.read_bytes())

        s3.upload_file(str(tmp_path), bucket, key)
    finally:
        tmp_path.unlink(missing_ok=True)


async def process_geoparquet_batch(
    ndjson_files, local_root, bucket, key_prefix, read_store, write_store
):
    semaphore = asyncio.Semaphore(40)

    all_stats = []

    async def process_with_semaphore(path):
        async with semaphore:
            rel_path = path.relative_to(local_root)
            key = f"{key_prefix}/{rel_path.as_posix()}"
            success, stats = await translate_and_upload_geoparquet(
                path, bucket, key, read_store, write_store
            )
            if success and stats:
                all_stats.append(stats)
            return success

    tasks = [process_with_semaphore(path) for path in ndjson_files]

    # Process all files concurrently with progress bar
    successful = 0
    failed = 0

    for coro in tqdm(
        asyncio.as_completed(tasks), total=len(tasks), desc="Syncing to S3 (geoparquet)"
    ):
        try:
            success = await coro
            if success:
                successful += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
            logger.error(f"Error processing file: {str(e)}")

    total_stats = {
        "files_processed": successful,
        "files_failed": failed,
        "existing_items": sum(s["existing_count"] for s in all_stats),
        "new_items": sum(s["new_count"] for s in all_stats),
        "added_items": sum(s["added_count"] for s in all_stats),
        "updated_items": sum(s["updated_count"] for s in all_stats),
        "final_items": sum(s["final_count"] for s in all_stats),
    }

    print("-" * 60)
    print("GeoParquet Sync Summary:")
    print(f"  Files processed:    {total_stats['files_processed']}")
    print(f"  Files failed:       {total_stats['files_failed']}")
    print(f"  Existing items:     {total_stats['existing_items']:,}")
    print(f"  New items:          {total_stats['new_items']:,}")
    print(f"  Items added:        {total_stats['added_items']:,}")
    print(f"  Items updated:      {total_stats['updated_items']:,}")
    print(f"  Final total items:  {total_stats['final_items']:,}")
    print("-" * 60)


async def translate_and_upload_geoparquet(
    local_file_path: Path,
    s3_bucket: str,
    s3_key: str,
    read_store: S3Store,
    write_store: S3Store,
):
    """
    Convert local ndjson to geoparquet and upload to S3, merging with existing data.
    Uses pre-created stores for connection pooling.
    Deduplicates by STAC item 'id' field.
    Returns: (success: bool, stats: dict or None)
    """
    try:
        stac_data = await rustac.read(str(local_file_path), store=None)

        # Convert s3_key from .ndjson to .parquet
        parquet_s3_key = s3_key.replace(".ndjson", ".parquet")
        parquet_s3_path = f"s3://{s3_bucket}/{parquet_s3_key}"

        if stac_data["type"] == "Feature":
            new_features = [stac_data]
        elif stac_data["type"] == "FeatureCollection":
            new_features = stac_data["features"]
        else:
            new_features = [stac_data]

        stats = {
            "existing_count": 0,
            "new_count": len(new_features),
            "added_count": 0,
            "updated_count": 0,
            "final_count": 0,
        }

        try:
            existing_data = await rustac.read(parquet_s3_path, store=read_store)

            if existing_data["type"] == "Feature":
                old_features = [existing_data]
            elif existing_data["type"] == "FeatureCollection":
                old_features = existing_data["features"]
            else:
                old_features = []

            stats["existing_count"] = len(old_features)

            # Deduplicate by 'id' field
            existing_by_id = {f.get("id"): f for f in old_features if f.get("id")}

            for feature in new_features:
                feature_id = feature.get("id")
                if feature_id:
                    if feature_id in existing_by_id:
                        stats["updated_count"] += 1
                    else:
                        stats["added_count"] += 1
                    existing_by_id[feature_id] = feature
                else:
                    logger.warning(f"Feature without 'id' field in {local_file_path}")

            merged_features = list(existing_by_id.values())

        except Exception:
            # File doesn't exist, use new data only
            merged_features = new_features
            stats["added_count"] = len(new_features)

        stats["final_count"] = len(merged_features)

        # Write to S3 as geoparquet
        await rustac.write(
            parquet_s3_path,
            merged_features,
            format="parquet",
            store=write_store,
        )

        return True, stats

    except Exception as e:
        logger.error(
            f"Failed to translate and upload {local_file_path} to {s3_key}: {str(e)}"
        )
        return False, None


def convert_local_ndjson_to_geoparquet(local_root: str, local_output_dir: str):
    """Convert all local ndjson files to geoparquet files"""
    ndjson_files = list(Path(local_root).rglob("*.ndjson"))

    if not ndjson_files:
        logger.warning("No ndjson files to convert")
        return

    logger.info(f"Converting {len(ndjson_files)} ndjson files to geoparquet")

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


async def translate_ndjson_to_geoparquet_local(
    local_ndjson_path: Path, local_output_dir: str
):
    """Convert local ndjson file to geoparquet"""
    try:
        # Read the local ndjson file
        stac_data = await rustac.read(str(local_ndjson_path), store=None)

        # Create output path with .parquet extension
        relative_path = local_ndjson_path.relative_to(
            local_ndjson_path.parent.parent.parent.parent
        )
        parquet_output_path = Path(local_output_dir) / relative_path
        parquet_output_path = parquet_output_path.with_suffix(".parquet")

        # Create parent directories
        parquet_output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write as geoparquet
        await rustac.write(
            str(parquet_output_path), stac_data, format="parquet", store=None
        )

        if logging.getLogger().isEnabledFor(logging.INFO):
            logger.info(
                f"Successfully converted {local_ndjson_path} to {parquet_output_path}"
            )
        return True

    except Exception as e:
        logger.error(f"Failed to translate {local_ndjson_path} to geoparquet: {str(e)}")
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
    parser = argparse.ArgumentParser(
        description="Ingest STAC items and organize by tiling system"
    )
    parser.add_argument("--source", required=True, help="Source path (local or s3://)")
    parser.add_argument(
        "--sync-directory",
        default="/tmp/stac_ingest/local_synced_data/",
        help="Local directory to sync S3 source files",
    )
    parser.add_argument("--output", help="Output path (local or s3://)")
    parser.add_argument(
        "--skip-upload", action="store_true", help="Skip upload to output"
    )
    parser.add_argument(
        "--skip-download", action="store_true", help="Skip download from S3 source"
    )
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

    # Setup logging
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
