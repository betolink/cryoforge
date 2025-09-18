"""
test_stac_ingest.py

📌 Purpose:
  - Unit and integration tests for stac_ingest.py
  - Validates tiling, parsing, batching, reporting
  - Uses synthetic data and mocked filesystems
  - No real S3 calls

🧩 Architecture Diagram (Text-based)
"""

# 🏗️ ARCHITECTURE DIAGRAM
"""
┌─────────────────────────────────────────────────────┐
│                    STAC Ingestion Flow              │
└─────────────────────────────────────────────────────┘

  Input: STAC JSON files (S3 or local)
          │
          ▼
   Dask: process_stac_file → extract center, mission, year
          │
          ▼
   Group by: (mission, latlon_tile, h3_tile, year)
          │
          ▼
   Buffered → flushed to /tmp/batches/.../*.ndjson when full
          │
          ▼
   After processing: upload batches with streaming append
          │
          ▼
   Generate ingestion_report.json
          │
          ▼
   ✅ Done (append-only, scalable, safe)
"""

# 📌 USAGE EXAMPLES
"""
# Local test run
python stac_ingest.py \
  -s /tmp/test_input/ \
  -o /tmp/test_output/ \
  -m landsatOLI sentinel2 \
  --workers 2 \
  --batch-size 5 \
  --local-batch-dir /tmp/test_batches

# Production (S3 + Coiled)
python stac_ingest.py \
  -s s3://its-live-data/staging/ \
  -o s3://its-live-data/consolidated/ \
  --use-coiled \
  --coiled-workers 20 \
  --missions landsatOLI
"""

import pytest
import time
from pathlib import Path
import orjson
import tempfile
from unittest.mock import MagicMock
from stac_ingest import (
    latlon_to_tile_id,
    get_mission_from_href,
    get_year_from_stac,
    get_tiling_keys,
    batch_items_locally,
    generate_ingestion_report,
    ingest_stac_files,
    PipelineConfig,
)


# -------------------------------
# 1. Unit Tests
# -------------------------------


def test_latlon_to_tile_id():
    assert latlon_to_tile_id(60.5, -35.2) == "N60W040"
    assert latlon_to_tile_id(-25.7, 130.1) == "S30E130"
    assert latlon_to_tile_id(0, 0) == "N00E000"


def test_get_mission_from_href():
    href = "s3://bucket/velocity_image_pair/sentinel2/file.tif"
    assert get_mission_from_href(href) == "sentinel2"
    assert get_mission_from_href("invalid/path") is None


def test_get_year_from_stac():
    item = {"properties": {"datetime": "2023-01-01T00:00:00Z"}}
    assert get_year_from_stac(item) == "2023"
    item = {"properties": {"start_datetime": "2022-06-01T00:00:00Z"}}
    assert get_year_from_stac(item) == "2022"
    item = {"properties": {"datetime": "invalid"}}
    assert get_year_from_stac(item) is None


def test_get_tiling_keys():
    item = {
        "properties": {"center": [10.0, 50.0], "datetime": "2023-01-01T00:00:00Z"},
        "assets": {"data": {"href": "velocity_image_pair/landsatOLI/file.tif"}},
    }
    res = get_tiling_keys(item)
    assert res is not None
    mission, latlon, h3_tile, year = res
    assert mission == "landsatOLI"
    assert latlon == "N50E010"
    assert h3_tile == h3.geo_to_h3(50.0, 10.0, 2)
    assert year == "2023"


# -------------------------------
# 2. Integration: Local Batching
# -------------------------------


def test_batch_items_locally():
    with tempfile.TemporaryDirectory() as tmpdir:
        batch_dir = Path(tmpdir) / "batches"
        items = [orjson.dumps({"id": f"item{i}"}) + b"\n" for i in range(3)]

        batch_items_locally(
            items=items,
            local_batch_dir=batch_dir,
            tiling_system="latlon",
            mission="sentinel2",
            tile_id="N10E020",
            year="2023",
        )

        output_file = batch_dir / "latlon" / "sentinel2" / "N10E020" / "2023.ndjson"
        assert output_file.exists()
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 3


# -------------------------------
# 3. Report Generation Test
# -------------------------------


def test_generate_ingestion_report():
    with tempfile.TemporaryDirectory() as tmpdir:
        fs_write = MagicMock()
        fs_write.upload = lambda f, path: Path(path.replace("s3://", "")).write_bytes(
            f.read()
        )

        stats = {
            "total_items": 100,
            "tiles": {("latlon", "landsatOLI", "N60W040")},
            "by_system": {
                "latlon": defaultdict(
                    lambda: defaultdict(
                        lambda: {"count": 100, "mission": {"landsatOLI": 100}}
                    )
                )
            },
        }

        report_path = f"{tmpdir}/report.json"
        generate_ingestion_report(
            stats=stats,
            start_time=time.time() - 10,
            output_path=report_path,
            fs_write=fs_write,
        )

        local_report = Path(report_path)
        assert local_report.exists()
        report_data = orjson.loads(local_report.read_bytes())
        assert report_data["summary"]["total_items_processed"] == 100


# -------------------------------
# 4. End-to-End Simulation (Local)
# -------------------------------


def test_end_to_end_local():
    with (
        tempfile.TemporaryDirectory() as input_dir,
        tempfile.TemporaryDirectory() as output_dir,
    ):
        # Create synthetic data
        input_path = Path(input_dir)
        for i in range(10):
            item = {
                "id": f"test-{i}",
                "properties": {
                    "center": [i * 10 - 100, i * 5 - 40],
                    "datetime": f"{2020 + (i % 4)}-01-01T00:00:00Z",
                },
                "assets": {
                    "data": {
                        "href": f"velocity_image_pair/{'landsatOLI' if i % 2 == 0 else 'sentinel2'}/file.tif"
                    }
                },
            }
            (input_path / f"item_{i}.json").write_bytes(orjson.dumps(item))

        # Config
        config = PipelineConfig(
            source=str(input_path) + "/",
            output_base=str(Path(output_dir) / "output"),
            max_workers=2,
            batch_size=3,
            local_batch_dir=str(Path(output_dir) / "batches"),
            upload_temp_dir=str(Path(output_dir) / "upload_tmp"),
        )
        config.setup_dirs()

        # Mock glob
        class DummyFS:
            def glob(self, pattern):
                return [
                    str(p.relative_to(input_path))
                    for p in Path(pattern.replace("**/*.json", "*.json")).parent.glob(
                        "*.json"
                    )
                ]

        config.filesystem = DummyFS()

        ingest_stac_files(config)

        # Validate
        batch_files = list(Path(config.local_batch_dir).rglob("*.ndjson"))
        assert len(batch_files) > 0
        total_items = sum(len(f.read_text().strip().split("\n")) for f in batch_files)
        assert total_items == 10

        report_file = Path(config.report_path.replace("s3://", ""))
        assert report_file.exists()
        report = orjson.loads(report_file.read_bytes())
        assert report["summary"]["total_items_processed"] == 10
