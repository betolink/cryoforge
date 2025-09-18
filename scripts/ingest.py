#!/usr/bin/env python
"""
ITS_LIVE STAC Ingestion Pipeline (CLI-ready)

This script:
  - Reads STAC JSON files from a source (S3 or local)
  - Groups by: mission, tiling system (latlon/H3), tile ID, year
  - Batches items locally to disk in NDJSON format
  - Safely appends to existing consolidated files (streaming)
  - Generates an ingestion report
  - Supports Dask (Local or Coiled) for parallelization

📌 Append-only, safe for large files, testable, and modular.
"""

import argparse
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from threading import BoundedSemaphore
from typing import Dict, List, Optional, Any, DefaultDict, Union
from collections import defaultdict
import time
import orjson

import dask
from dask.distributed import Client, as_completed
import s3fs
import h3

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)


# -------------------------------
# 1. Configuration
# -------------------------------


class PipelineConfig:
    def __init__(
        self,
        source: str,
        output_base: str,
        include_missions: Optional[List[str]] = None,
        anon_read: bool = True,
        max_workers: int = 8,
        batch_size: int = 1000,
        temp_dir: str = "/tmp/stac_ingest",
        local_batch_dir: str = "/tmp/stac_ingest/batches",
        upload_temp_dir: str = "/tmp/stac_ingest/upload_tmp",
        max_concurrent_uploads: int = 2,
        chunk_size: int = 1024 * 1024,
        use_coiled: bool = False,
        coiled_workers: Optional[int] = None,
        report_path: Optional[str] = None,
        filesystem: Optional[Union[s3fs.S3FileSystem, Any]] = None,
    ):
        self.source = source.rstrip("/") + "/"
        self.output_base = output_base.rstrip("/")
        self.include_missions = include_missions
        self.anon_read = anon_read
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.temp_dir = Path(temp_dir)
        self.local_batch_dir = Path(local_batch_dir)
        self.upload_temp_dir = Path(upload_temp_dir)
        self.max_concurrent_uploads = max_concurrent_uploads
        self.chunk_size = chunk_size
        self.use_coiled = use_coiled
        self.coiled_workers = coiled_workers or max_workers
        self.report_path = report_path or f"{output_base}/ingestion_report.json"
        self.filesystem = filesystem

    def setup_dirs(self):
        for d in [self.temp_dir, self.local_batch_dir, self.upload_temp_dir]:
            d.mkdir(parents=True, exist_ok=True)


# -------------------------------
# 2. Tiling & Parsing
# -------------------------------


def latlon_to_tile_id(lat: float, lon: float) -> str:
    def floor_10(x: float) -> int:
        return (int(x) // 10) * 10 if x >= 0 else ((int(x) - 9) // 10) * 10

    lat_10 = max(-90, min(80, floor_10(lat)))
    lon_10 = max(-180, min(170, floor_10(lon)))
    lat_dir = "N" if lat_10 >= 0 else "S"
    lon_dir = "E" if lon_10 >= 0 else "W"
    return f"{lat_dir}{abs(lat_10):02d}{lon_dir}{abs(lon_10):03d}"


def get_mission_from_href(href: str) -> Optional[str]:
    match = re.search(r"velocity_image_pair/([^/]+)/", href)
    return match.group(1) if match else None


def get_year_from_stac(stac_data: Dict[str, Any]) -> Optional[str]:
    for field in ["datetime", "start_datetime"]:
        dt_str = stac_data.get("properties", {}).get(field)
        if dt_str:
            try:
                return str(datetime.fromisoformat(dt_str.replace("Z", "+00:00")).year)
            except ValueError:
                continue
    return None


def get_tiling_keys(stac_item: Dict[str, Any]) -> Optional[Tuple[str, str, str, str]]:
    props = stac_item.get("properties", {})
    center = props.get("center")
    if not center or not isinstance(center, list) or len(center) != 2:
        return None
    lon, lat = float(center[0]), float(center[1])
    href = stac_item.get("assets", {}).get("data", {}).get("href", "")
    mission = get_mission_from_href(href)
    year = get_year_from_stac(stac_item)
    if not mission or not year:
        return None
    latlon_tile = latlon_to_tile_id(lat, lon)
    h3_tile = h3.geo_to_h3(lat, lon, resolution=2)
    return mission, latlon_tile, h3_tile, year


# -------------------------------
# 3. Dask Worker Plugin
# -------------------------------


class S3ReadWorkerPlugin:
    def __init__(self, anon: bool = True):
        self.anon = anon

    def setup(self, worker):
        worker.fs_read = s3fs.S3FileSystem(
            anon=self.anon,
            client_kwargs={"config": {"read_timeout": 300, "connect_timeout": 60}},
        )
        logger.info(f"S3 read filesystem initialized on worker {worker.address}")

    def teardown(self, worker):
        if hasattr(worker, "fs_read"):
            worker.fs_read.clear_instance_cache()


# -------------------------------
# 4. File Processing Task
# -------------------------------


@dask.delayed
def process_stac_file(file_path: str, fs: Optional[Any] = None) -> Dict[str, Any]:
    try:
        fs = fs or s3fs.S3FileSystem(anon=True)
        full_path = f"s3://{file_path}" if file_path.startswith("s3://") else file_path
        with fs.open(full_path, "rb") as f:
            data = orjson.loads(f.read())
        items = data if isinstance(data, list) else [data]
        grouped = {
            "latlon": defaultdict(lambda: defaultdict(lambda: defaultdict(list))),
            "h3": defaultdict(lambda: defaultdict(lambda: defaultdict(list))),
        }
        for item in items:
            keys = get_tiling_keys(item)
            if not keys:
                logger.warning(f"Invalid item in {file_path}")
                continue
            mission, latlon_tile, h3_tile, year = keys
            grouped["latlon"][mission][latlon_tile][year].append(item)
            grouped["h3"][mission][h3_tile][year].append(item)
        return grouped
    except Exception as e:
        logger.error(f"Failed to process {file_path}: {e}")
        return {
            "latlon": defaultdict(lambda: defaultdict(lambda: defaultdict(list))),
            "h3": defaultdict(lambda: defaultdict(lambda: defaultdict(list))),
        }


# -------------------------------
# 5. Local Batching
# -------------------------------


def batch_items_locally(
    items: List[bytes],
    local_batch_dir: Path,
    tiling_system: str,
    mission: str,
    tile_id: str,
    year: str,
):
    batch_dir = local_batch_dir / tiling_system / mission / tile_id
    batch_dir.mkdir(parents=True, exist_ok=True)
    batch_file = batch_dir / f"{year}.ndjson"
    with open(batch_file, "ab") as f:
        for item in items:
            f.write(item)


# -------------------------------
# 6. Safe Upload (Streaming Append)
# -------------------------------


def download_in_chunks(s3_path: str, fs: Any, chunk_size: int = 1024 * 1024):
    try:
        with fs.open(s3_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    except Exception as e:
        logger.error(f"Failed to download {s3_path}: {e}")
        raise


def finalize_tile_upload(
    local_batch_file: Path,
    s3_output_path: str,
    fs_write: Any,
    temp_dir: Path,
    semaphore: BoundedSemaphore,
    chunk_size: int = 1024 * 1024,
):
    with semaphore:
        tmp_output = (
            temp_dir / f"combined_{os.getpid()}_{id(local_batch_file) % 100000}.tmp"
        )
        try:
            with open(tmp_output, "wb") as tmpf:
                if fs_write.exists(s3_output_path):
                    for chunk in download_in_chunks(
                        s3_output_path, fs_write, chunk_size
                    ):
                        tmpf.write(chunk)
                with open(local_batch_file, "rb") as batchf:
                    while chunk := batchf.read(chunk_size):
                        tmpf.write(chunk)
            with open(tmp_output, "rb") as f:
                fs_write.upload(f, s3_output_path)
            size = tmp_output.stat().st_size
            logger.info(f"Uploaded {size:,} bytes to {s3_output_path}")
        except Exception as e:
            logger.error(f"Failed to upload {s3_output_path}: {e}")
        finally:
            if tmp_output.exists():
                tmp_output.unlink()


# -------------------------------
# 7. Ingestion Report
# -------------------------------


def generate_ingestion_report(
    stats: Dict[str, Any],
    start_time: float,
    output_path: str,
    fs_write: Any,
):
    def to_dict(obj):
        if isinstance(obj, defaultdict):
            return {k: to_dict(v) for k, v in obj.items()}
        if isinstance(obj, dict):
            return {k: to_dict(v) for k, v in obj.items()}
        return obj

    report = {
        "summary": {
            "total_items_processed": stats["total_items"],
            "total_tiles_updated": len(stats["tiles"]),
            "start_time": datetime.fromtimestamp(
                start_time, tz=timezone.utc
            ).isoformat(),
            "end_time": datetime.now(timezone.utc).isoformat(),
            "duration_seconds": round(time.time() - start_time),
        },
        "by_tiling_system": to_dict(stats["by_system"]),
    }

    logger.info("📊 Ingestion Summary:")
    logger.info(f"   Total items: {report['summary']['total_items_processed']:,}")
    logger.info(f"   Tiles updated: {report['summary']['total_tiles_updated']}")
    logger.info(f"   Duration: {report['summary']['duration_seconds']} seconds")

    local_report = Path("/tmp") / Path(output_path).name
    with open(local_report, "wb") as f:
        f.write(orjson.dumps(report, option=orjson.OPT_INDENT_2))

    try:
        with open(local_report, "rb") as f:
            fs_write.upload(f, output_path)
        logger.info(f"Ingestion report uploaded to {output_path}")
    except Exception as e:
        logger.error(f"Failed to upload report to {output_path}: {e}")

    return report


# -------------------------------
# 8. Main Pipeline
# -------------------------------


def ingest_stac_files(config: PipelineConfig):
    logger.info("🚀 Starting STAC ingestion pipeline")
    start_time = time.time()

    fs_read = config.filesystem or (
        s3fs.S3FileSystem(anon=config.anon_read)
        if config.source.startswith("s3://")
        else None
    )
    fs_write = s3fs.S3FileSystem(anon=False)

    config.setup_dirs()

    # List files
    if config.source.startswith("s3://"):
        try:
            all_files = [f for f in fs_read.glob(config.source + "**/*.json")]
            logger.info(f"Found {len(all_files)} files in S3")
        except Exception as e:
            logger.error(f"Failed to list files in {config.source}: {e}")
            return
    else:
        local_path = Path(config.source.lstrip("/"))
        all_files = [
            str(f.relative_to(local_path)) for f in local_path.glob("**/*.json")
        ]
        logger.info(f"Found {len(all_files)} files locally")

    if not all_files:
        logger.info("No files to process.")
        return

    if config.include_missions:
        filtered = [
            f
            for f in all_files
            if any(
                f.startswith(f"{p}/") or f.startswith(f"velocity_image_pair/{p}/")
                for p in config.include_missions
            )
        ]
        logger.info(f"Filtered to {len(filtered)} files")
        all_files = filtered

    # Dask cluster
    if config.use_coiled:
        try:
            import coiled
        except ImportError:
            raise ImportError("Install coiled: pip install coiled")
        cluster = coiled.Cluster(
            name="stac-ingest",
            region="us-west-2",
            worker_vm_types=["m5.2xlarge"],
            worker_disk_size=100,
            worker_use_spot=True,
            n_workers=config.coiled_workers,
            worker_extra_packages=[
                "s3fs>=2023.0.0",
                "h3>=3.7.0",
                "orjson>=3.0.0",
                "dask[complete]",
                "pyarrow",
            ],
            scheduler_vm_types=["m5.xlarge"],
            tags={"project": "itslive-stac", "job": "ingest"},
        )
    else:
        cluster = dask.distributed.LocalCluster(
            n_workers=config.max_workers,
            threads_per_worker=1,
            processes=True,
            memory_limit="4GB",
            local_directory=config.temp_dir,
        )

    client = Client(cluster)
    client.register_worker_plugin(
        S3ReadWorkerPlugin(anon=config.anon_read), name="s3_read_plugin", replace=True
    )
    logger.info(f"🔗 Dask dashboard: {client.dashboard_link}")

    # Stats
    stats = {
        "total_items": 0,
        "tiles": set(),
        "by_system": {
            "latlon": defaultdict(
                lambda: defaultdict(lambda: {"count": 0, "mission": defaultdict(int)})
            ),
            "h3": defaultdict(
                lambda: defaultdict(lambda: {"count": 0, "mission": defaultdict(int)})
            ),
        },
    }

    buffers: Dict[str, Dict[str, Dict[str, Dict[str, List[bytes]]]]] = {}

    tasks = [process_stac_file(f) for f in all_files]
    futures = client.compute(tasks)

    for future in as_completed(futures):
        try:
            result = future.result()
            for tiling_system in ["latlon", "h3"]:
                for mission, tiles in result[tiling_system].items():
                    for tile_id, years in tiles.items():
                        for year, items in years.items():
                            buf = (
                                buffers.setdefault(tiling_system, {})
                                .setdefault(mission, {})
                                .setdefault(tile_id, {})
                            )
                            if year not in buf:
                                buf[year] = []
                            serialized = [orjson.dumps(item) + b"\n" for item in items]
                            buf[year].extend(serialized)
                            stats["total_items"] += len(items)

                            for item in items:
                                y = get_year_from_stac(item)
                                if y:
                                    counter = stats["by_system"][tiling_system][
                                        tile_id
                                    ][y]
                                    counter["count"] += 1
                                    counter["mission"][mission] += 1
                                    stats["tiles"].add(
                                        (tiling_system, mission, tile_id)
                                    )

                            if len(buf[year]) >= config.batch_size:
                                batch_items_locally(
                                    items=buf[year],
                                    local_batch_dir=config.local_batch_dir,
                                    tiling_system=tiling_system,
                                    mission=mission,
                                    tile_id=tile_id,
                                    year=year,
                                )
                                buf[year] = []
        except Exception as e:
            logger.error(f"Error processing task: {e}")

    # Final flush
    for tiling_system, missions in buffers.items():
        for mission, tiles in missions.items():
            for tile_id, years in tiles.items():
                for year, items in years.items():
                    if items:
                        batch_items_locally(
                            items=items,
                            local_batch_dir=config.local_batch_dir,
                            tiling_system=tiling_system,
                            mission=mission,
                            tile_id=tile_id,
                            year=year,
                        )

    client.close()
    cluster.close()
    logger.info("✅ Dask processing complete. Finalizing uploads...")

    # Upload batches
    semaphore = BoundedSemaphore(config.max_concurrent_uploads)
    for batch_file in config.local_batch_dir.rglob("*.ndjson"):
        if not batch_file.is_file() or batch_file.stat().st_size == 0:
            continue
        rel_path = batch_file.relative_to(config.local_batch_dir)
        s3_path = f"{config.output_base}/{'/'.join(rel_path.parts)}"
        finalize_tile_upload(
            local_batch_file=batch_file,
            s3_output_path=s3_path,
            fs_write=fs_write,
            temp_dir=config.upload_temp_dir,
            semaphore=semaphore,
            chunk_size=config.chunk_size,
        )

    # Report
    generate_ingestion_report(
        stats=stats,
        start_time=start_time,
        output_path=config.report_path,
        fs_write=fs_write,
    )

    logger.info("✅ STAC ingestion pipeline complete.")


# -------------------------------
# 9. CLI Entry Point
# -------------------------------


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "-s", "--source", type=str, required=True, help="Source path (S3 or local)"
    )
    parser.add_argument(
        "-o", "--output-base", type=str, required=True, help="Output base (S3 or local)"
    )
    parser.add_argument(
        "-m",
        "--missions",
        nargs="+",
        default=None,
        help="Filter by mission (e.g., landsatOLI)",
    )
    parser.add_argument("--anon", action="store_true", help="Use anonymous S3 read")
    parser.add_argument(
        "-w", "--workers", type=int, default=8, help="Number of Dask workers"
    )
    parser.add_argument(
        "-b", "--batch-size", type=int, default=1000, help="Batch size for local flush"
    )
    parser.add_argument(
        "--temp-dir", type=str, default="/tmp/stac_ingest", help="Temp dir for Dask"
    )
    parser.add_argument(
        "--local-batch-dir",
        type=str,
        default="/tmp/stac_ingest/batches",
        help="Local batching dir",
    )
    parser.add_argument(
        "--upload-temp-dir",
        type=str,
        default="/tmp/stac_ingest/upload_tmp",
        help="Upload temp dir",
    )
    parser.add_argument(
        "--max-concurrent-uploads", type=int, default=2, help="Max concurrent uploads"
    )
    parser.add_argument(
        "--chunk-size", type=int, default=1024 * 1024, help="Streaming chunk size"
    )
    parser.add_argument("--use-coiled", action="store_true", help="Use Coiled cluster")
    parser.add_argument(
        "--coiled-workers", type=int, default=None, help="Number of Coiled workers"
    )
    parser.add_argument(
        "--report-path", type=str, default=None, help="Custom report path"
    )

    args = parser.parse_args()

    config = PipelineConfig(
        source=args.source,
        output_base=args.output_base,
        include_missions=args.missions,
        anon_read=args.anon,
        max_workers=args.workers,
        batch_size=args.batch_size,
        temp_dir=args.temp_dir,
        local_batch_dir=args.local_batch_dir,
        upload_temp_dir=args.upload_temp_dir,
        max_concurrent_uploads=args.max_concurrent_uploads,
        chunk_size=args.chunk_size,
        use_coiled=args.use_coiled,
        coiled_workers=args.coiled_workers,
        report_path=args.report_path,
    )

    ingest_stac_files(config)


if __name__ == "__main__":
    main()
