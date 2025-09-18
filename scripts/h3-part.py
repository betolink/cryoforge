#!/usr/bin/env python

import duckdb
from pathlib import Path
import logging
from typing import List, Dict, Optional
import json
import argparse
from shapely.wkt import loads as wkt_loads
from shapely.geometry import mapping as shapely_mapping
from tqdm import tqdm
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def materialize_all_data(input_dir: Path, h3_resolution: int) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL h3 FROM community; LOAD h3;")
    conn.execute("INSTALL spatial; LOAD spatial;")

    parquet_paths = list(input_dir.rglob("*.parquet"))
    if not parquet_paths:
        raise RuntimeError("No Parquet files found in input directory.")

    logger.info(f"Materializing {len(parquet_paths)} input files into DuckDB table...")
    
    # Prepare DuckDB query
    input_files = [str(p) for p in parquet_paths]
    files_list_str = ", ".join([f"'{p}'" for p in input_files])
    create_query = f"""
    CREATE TABLE all_items AS
    SELECT *,
           h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), ST_X(ST_Centroid(geometry)), {h3_resolution}) AS h3_index,
           CAST(SUBSTR(filename, -12, 4) AS VARCHAR) AS year_col
    FROM read_parquet([{files_list_str}], union_by_name=True)
    """

    conn.execute(create_query)
    logger.info("Materialization complete.")
    return conn

def get_partitions(conn: duckdb.DuckDBPyConnection, max_items_per_file: int) -> List[Dict]:
    partition_query = f"""
    SELECT h3_index, year_col, COUNT(*) as item_count, ST_Extent(geometry) as bbox
    FROM all_items
    GROUP BY h3_index, year_col
    ORDER BY h3_index, year_col
    """
    result = conn.execute(partition_query).fetchall()

    partitions = []
    for h3_cell, year, count, bbox in result:
        num_files = (count + max_items_per_file - 1) // max_items_per_file
        partitions.append({
            'h3_cell': h3_cell,
            'year': year,
            'count': count,
            'num_files': num_files,
            'bbox': [bbox['min_x'], bbox['min_y'], bbox['max_x'], bbox['max_y']] if bbox else None
        })
    return partitions

def write_partition(conn: duckdb.DuckDBPyConnection, output_dir: Path, partition: Dict, max_items_per_file: int, base_geo_metadata: Dict):
    h3_cell = partition['h3_cell']
    year = partition['year']
    num_files = partition['num_files']

    partition_metadata = base_geo_metadata.copy()
    if partition['bbox']:
        partition_metadata['columns']['geometry']['bbox'] = partition['bbox']
    geo_metadata_str = json.dumps(partition_metadata)

    for i in range(num_files):
        output_path = output_dir / str(h3_cell) / str(year)
        output_file = output_path / f"part_{i}.parquet"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        offset = i * max_items_per_file

        query = f"""
        SELECT * EXCLUDE (h3_index, year_col)
        FROM (
            SELECT * FROM all_items
            WHERE h3_index = {h3_cell} AND year_col = '{year}'
            ORDER BY id
        )
        LIMIT {max_items_per_file} OFFSET {offset}
        """

        copy_query = f"COPY ({query}) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')"
        conn.execute(copy_query)
        logger.debug(f"Wrote file: {output_file}")

def write_all_partitions_in_parallel(conn: duckdb.DuckDBPyConnection, output_dir: Path, partitions: List[Dict], max_items_per_file: int, base_geo_metadata: Dict):
    cpu_count = multiprocessing.cpu_count() - 2
    logger.info(f"Writing partitions in parallel using {cpu_count} processes...")

    def task(part):
        local_conn = duckdb.connect()
        local_conn.execute("INSTALL h3 FROM community; LOAD h3;")
        local_conn.execute("INSTALL spatial; LOAD spatial;")
        local_conn.execute("ATTACH ':memory:' AS mem;")
        local_conn.execute("CREATE TABLE all_items AS SELECT * FROM read_parquet('{str(output_dir.parent)}/temp/all_items.parquet', union_by_name=True)")
        write_partition(local_conn, output_dir, part, max_items_per_file, base_geo_metadata)
        local_conn.close()

    # Write all_items to disk once to allow multi-process reads
    tmp_parquet_path = output_dir.parent / "temp/all_items.parquet"
    tmp_parquet_path.parent.mkdir(parents=True, exist_ok=True)
    conn.execute(f"COPY all_items TO '{tmp_parquet_path}' (FORMAT PARQUET, CODEC 'ZSTD')")

    with ProcessPoolExecutor(max_workers=cpu_count) as executor:
        list(tqdm(executor.map(task, partitions), total=len(partitions), desc="Writing partitions"))

def main():
    parser = argparse.ArgumentParser(description="Repartition STAC GeoParquet files using H3, avoiding UUIDs.")
    parser.add_argument("--input-dir", type=str, required=True, help="Input directory with GeoParquet files")
    parser.add_argument("--output-dir", type=str, required=True, help="Output directory")
    parser.add_argument("--h3-resolution", type=int, default=2, help="H3 resolution")
    parser.add_argument("--max-items", type=int, default=200000, help="Max items per output file")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_geo_metadata = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Polygon", "MultiPolygon"],
                "bbox": []
            }
        }
    }

    conn = materialize_all_data(input_dir, args.h3_resolution)
    partitions = get_partitions(conn, args.max_items)
    logger.info(f"Identified {len(partitions)} unique H3-year partitions.")
    write_all_partitions_in_parallel(conn, output_dir, partitions, args.max_items, base_geo_metadata)
    logger.info("Repartitioning complete.")

if __name__ == "__main__":
    main()

