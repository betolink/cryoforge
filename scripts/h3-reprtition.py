#!/usr/bin/env python

import duckdb
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional
import math
import gc
import json
import argparse
from tqdm import tqdm
from shapely.wkt import loads as wkt_loads
from uuid import uuid4
from shapely.geometry import mapping as shapely_mapping

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class H3Repartitioner:
    """
    A class to repartition GeoParquet files based on H3 spatial indexing.

    This class reads a directory of GeoParquet files, partitions the data by a
    specified H3 resolution and year, and writes out a new partitioned dataset.
    It is designed to handle large datasets by processing files in batches and
    preserving GeoParquet metadata.
    """
    def __init__(self, input_dir: str, output_dir: str, h3_resolution: int = 2, max_items_per_file: int = 200000):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.h3_resolution = h3_resolution
        self.max_items_per_file = max_items_per_file
        self.conn = duckdb.connect() # Use a persistent database connection

        # Install and load necessary DuckDB extensions
        try:
            self.conn.execute("INSTALL h3 FROM community;LOAD h3;")
            self.conn.execute("INSTALL spatial; LOAD spatial;")
        except Exception as e:
            logger.error(f"Failed to install/load DuckDB extensions: {e}")
            raise

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def get_input_files(self) -> List[Dict[str, str]]:
        """Find all Parquet files in the input directory, assuming a structure like 'tile/year.parquet'."""
        files = []
        for parquet_file in self.input_dir.rglob("*.parquet"):
            tile_name = parquet_file.parent.name
            year = parquet_file.stem
            files.append({'file_path': str(parquet_file), 'tile': tile_name, 'year': year})
        logger.info(f"Found {len(files)} Parquet files to process.")
        return files

    def create_temp_table_with_h3(self, file_info: Dict[str, str]) -> str:
        """Create a temporary DuckDB table from a Parquet file with an added H3 index column."""
        table_name = f"temp_{file_info['tile'].replace('-', '_')}_{file_info['year']}"
        create_query = f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        SELECT *,
               h3_latlng_to_cell(ST_Y(ST_Centroid(geometry)), ST_X(ST_Centroid(geometry)), {self.h3_resolution}) as h3_index,
               '{file_info['year']}' as year_col
        FROM read_parquet('{file_info['file_path']}', union_by_name=True)
        """
        try:
            self.conn.execute(create_query)
            logger.debug(f"Created temp table {table_name}")
            return table_name
        except Exception as e:
            logger.error(f"Error creating temp table for {file_info['file_path']}: {e}")
            raise

    def get_partition_info(self, table_name: str) -> List[Dict]:
        """Get partition information (H3 cell, year, count) for a given table."""
        # First get the basic partition info
        count_query = f"""
        SELECT h3_index, year_col, COUNT(*) as item_count
        FROM {table_name}
        GROUP BY h3_index, year_col
        ORDER BY h3_index, year_col
        """
        count_result = self.conn.execute(count_query).fetchall()
        
        partitions = []
        for h3_cell, year, count in count_result:
            # Get bbox for this specific partition
            bbox_query = f"""
            SELECT ST_Extent(geometry) as bbox
            FROM {table_name}
            WHERE h3_index = {h3_cell} AND year_col = '{year}'
            """
            bbox_result = self.conn.execute(bbox_query).fetchone()
            bbox = bbox_result[0] if bbox_result and bbox_result[0] else None
            
            num_files = math.ceil(count / self.max_items_per_file)
            partitions.append({
                'h3_cell': h3_cell,
                'year': year,
                'count': count,
                'num_files': num_files,
                'bbox': [bbox['min_x'], bbox['min_y'], bbox['max_x'], bbox['max_y']] if bbox else None
            })
        return partitions
    
    def write_partition_files(self, source_view: str, partition_info: Dict, base_geo_metadata: Dict):
        """Write out the data for a single partition, splitting into multiple files if necessary."""
        h3_cell = partition_info['h3_cell']
        year = partition_info['year']
        num_files = partition_info['num_files']
        
        partition_metadata = base_geo_metadata.copy()
        if partition_info['bbox']:
            partition_metadata['columns']['geometry']['bbox'] = partition_info['bbox']

        geo_metadata_str = json.dumps(partition_metadata)

        for i in range(num_files):
            output_dir = self.output_dir / str(h3_cell) / str(year)
            unique_id = uuid4().hex[:8]
            output_file = output_dir / f"part_{i}_{unique_id}.parquet"
            
            offset = i * self.max_items_per_file
            if offset >= 1:
                logger.info(f"Writing partition {h3_cell}/{year} file {i+1}/{num_files}, offset={offset}")

            query = f"""
            SELECT * EXCLUDE (h3_index, year_col)
            FROM (
                SELECT * FROM {source_view}
                WHERE h3_index = {h3_cell} AND year_col = '{year}'
                ORDER BY id
            )
            LIMIT {self.max_items_per_file} OFFSET {offset}
            """

            self._write_geoparquet_file(query, output_file, geo_metadata_str)


    def _write_geoparquet_file(self, query: str, output_file: Path, geo_metadata: Optional[str] = None):
        """Write a single GeoParquet file, including the 'geo' metadata."""
        output_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            # Clean the query of any trailing semicolons and whitespace
            clean_query = query.strip().rstrip(';').strip()
            
            # Simple COPY statement
            copy_query = f"COPY ({clean_query}) TO '{output_file}' (FORMAT PARQUET, CODEC 'ZSTD')"
            
            logger.debug(f"Clean query: {clean_query}")
            logger.debug(f"Full COPY statement: {copy_query}")
            
            self.conn.execute(copy_query)
            logger.debug(f"Successfully wrote file {output_file}")
                
        except Exception as e:
            logger.error(f"Error writing file {output_file}: {e}")
            logger.error(f"Clean query was: {clean_query}")
            logger.error(f"Full COPY statement was: {copy_query}")
            raise

    def process_batch(self, file_batch: List[Dict[str, str]], base_geo_metadata: Dict):
        """Process a batch of files to repartition data and manage memory usage."""
        logger.debug(f"Processing batch of {len(file_batch)} files.")
        temp_tables = []
        for file_info in file_batch:
            try:
                table_name = self.create_temp_table_with_h3(file_info)
                temp_tables.append(table_name)
            except Exception as e:
                logger.error(f"Skipping file {file_info['file_path']} due to error: {e}")
                continue
        
        if not temp_tables:
            logger.warning("No valid tables created in this batch.")
            return

        union_query = " UNION ALL ".join([f"SELECT * FROM {table}" for table in temp_tables])
        self.conn.execute(f"CREATE OR REPLACE TEMP VIEW batch_combined_view AS ({union_query})")
        
        partitions = self.get_partition_info("batch_combined_view")
        
        for partition in partitions:
            self.write_partition_files("batch_combined_view", partition, base_geo_metadata)
        
        # Cleanup
        for table in temp_tables:
            self.conn.execute(f"DROP TABLE IF EXISTS {table}")
        self.conn.execute("DROP VIEW IF EXISTS batch_combined_view")

    def repartition(self, batch_size: int = 10):
        """Main method to orchestrate the repartitioning process."""
        logger.info("Starting H3 repartitioning process...")
        input_files = self.get_input_files()
        if not input_files:
            logger.error("No input Parquet files found. Aborting.")
            return

        base_geo_metadata = {
            "version": "1.1.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Polygon", "MultiPolygon"], # Specify expected types
                    "bbox": [] # This will be populated per-partition
                }
            }
        }
        
        # Process files in batches with a progress bar
        with tqdm(total=len(input_files), desc="Repartitioning files", unit="file") as pbar:
            for i in range(0, len(input_files), batch_size):
                batch = input_files[i:i + batch_size]
                self.process_batch(batch, base_geo_metadata)
                pbar.update(len(batch))
                gc.collect() # Force garbage collection to free memory
            
        logger.info("H3 repartitioning completed successfully.")

    def __del__(self):
        """Ensure the database connection is closed on object deletion."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

def search_polygon_optimized(output_dir: str, polygon_wkt: str, year: Optional[str] = None) -> List[Any]:
    """Optimized search for a polygon within the H3-partitioned GeoParquet data."""
    import h3
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL h3 FROM community;LOAD h3;")
        conn.execute("INSTALL spatial; LOAD spatial;")
    except Exception as e:
        logger.error(f"Failed to install/load extensions for search: {e}")
        conn.close()
        raise

    try:
        logger.info(f"Searching for items intersecting polygon (Year: {year or 'All'})")

        try:
            shapely_poly = wkt_loads(polygon_wkt)
            geojson_poly_str = json.dumps(shapely_mapping(shapely_poly))
        except Exception as e:
            raise ValueError(f"Invalid WKT polygon provided: {e}")

        h3_cells = h3.h3shape_to_cells_experimental(h3.geo_to_h3shape(geojson_poly_str), 2)
        
        if not h3_cells:
            logger.warning("Polygon does not intersect any H3 level 2 cells.")
            return []

        logger.info(f"Identified {len(h3_cells)} relevant H3 cells.")
        
        file_paths = []
        output_path = Path(output_dir)
        for h3_cell in h3_cells:
            h3_dir = output_path / str(h3_cell)
            if not h3_dir.exists():
                continue
            
            if year:
                year_dir = h3_dir / year
                if year_dir.exists():
                    file_paths.extend([str(f) for f in year_dir.glob("*.parquet")])
            else:
                for year_subdir in h3_dir.iterdir():
                    if year_subdir.is_dir() and year_subdir.name.isdigit():
                        file_paths.extend([str(f) for f in year_subdir.glob("*.parquet")])
            
        if not file_paths:
            logger.warning("No Parquet files found for the specified H3 cells and year filter.")
            return []
        
        logger.info(f"Querying {len(file_paths)} relevant Parquet files.")
        
        files_list_str = "['" + "', '".join(file_paths) + "']"
        
        final_query = f"""
        WITH polygon_geom AS (SELECT ST_GeomFromText('{polygon_wkt}') as geom)
        SELECT t1.*
        FROM read_parquet({files_list_str}) AS t1, polygon_geom
        WHERE ST_Intersects(t1.geometry, polygon_geom.geom);
        """
        
        results = conn.execute(final_query).fetchall()
        logger.info(f"Found {len(results)} matching items.")
        return results
        
    except Exception as e:
        logger.error(f"An error occurred during the polygon search: {e}")
        raise
    finally:
        if conn:
            conn.close()

def create_dummy_data(input_dir: str, num_tiles: int, years: List[str], rows_per_file: int):
    """Creates dummy GeoParquet files for testing purposes."""
    try:
        import geopandas as gpd
        from shapely.geometry import Polygon
    except ImportError:
        logger.error("Please install pandas and geopandas (`pip install pandas geopandas`) to create dummy data.")
        return

    logger.info("Creating dummy input data...")
    for year_val in years:
        for tile_idx in range(1, num_tiles + 1):
            tile_path = Path(input_dir) / f"tile_{tile_idx:02d}"
            tile_path.mkdir(parents=True, exist_ok=True)
            
            data = {
                'id': [f'item_{year_val}_{tile_idx}_{i}' for i in range(rows_per_file)],
                'value': [i * 1.1 for i in range(rows_per_file)],
                'geometry': [
                    Polygon([
                        (lon + i/rows_per_file*0.01, lat + i/rows_per_file*0.01),
                        (lon + 0.05, lat), (lon + 0.05, lat + 0.05), (lon, lat + 0.05)
                    ]) for i, (lon, lat) in enumerate(zip(
                        [80 + (tile_idx-1)*5] * rows_per_file,
                        [28 + (tile_idx-1)*2] * rows_per_file
                    ))
                ]
            }
            gdf = gpd.GeoDataFrame(data, geometry='geometry', crs="EPSG:4326")
            
            output_dummy_path = tile_path / f"{year_val}.parquet"
            logger.info(f"Creating dummy file: {output_dummy_path}")
            gdf.to_parquet(output_dummy_path, engine='pyarrow', compression='snappy')

def main():
    """Main function to parse command-line arguments and run the repartitioner."""
    parser = argparse.ArgumentParser(description="Repartition GeoParquet files using H3 indexing.")
    parser.add_argument("--input-dir", type=str, required=True, help="Directory containing input GeoParquet files.")
    parser.add_argument("--output-dir", type=str, required=True, help="Directory to save H3-partitioned output files.")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of input files to process in each batch.")
    parser.add_argument("--max-items", type=int, default=200000, help="Maximum number of items per output Parquet file.")
    parser.add_argument("--h3-resolution", type=int, default=2, help="H3 resolution for partitioning (0-15).")
    parser.add_argument("--setup-dummy-data", action="store_true", help="If set, create dummy data in the input directory before running.")
    
    args = parser.parse_args()

    if args.setup_dummy_data:
        create_dummy_data(args.input_dir, num_tiles=2, years=["2023", "2024"], rows_per_file=500000)

    # Initialize and run the repartitioner
    repartitioner = H3Repartitioner(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        max_items_per_file=args.max_items,
        h3_resolution=args.h3_resolution
    )
    
    try:
        repartitioner.repartition(batch_size=args.batch_size)
        
        # --- Example Search ---
        logger.info("\n--- Running Example Spatial Search ---")
        example_polygon = "POLYGON((79.9 27.9, 80.1 27.9, 80.1 28.1, 79.9 28.1, 79.9 27.9))"
        
        results_2023 = search_polygon_optimized(args.output_dir, example_polygon, year="2023")
        print(f"Found {len(results_2023)} items in 2023 intersecting with the example polygon.")
        
        results_all_years = search_polygon_optimized(args.output_dir, example_polygon)
        print(f"Found {len(results_all_years)} items across all years intersecting the polygon.")

    except Exception as e:
        logger.error(f"The repartitioning process failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()
