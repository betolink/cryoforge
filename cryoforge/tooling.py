import fnmatch
import gc
import logging
import re
import urllib
from urllib.parse import urlparse

import boto3
import dask
import s3fs
import h3
import math
import os
import json
from shapely.geometry import shape, box

import psutil
import requests
from botocore import UNSIGNED
from botocore.client import Config
import rustac
import duckdb

from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# Connect to DuckDB
con = duckdb.connect()
con.execute("INSTALL spatial")
con.execute("LOAD spatial")
con.execute("SET s3_region='us-west-2'")


s3_fs= s3fs.S3FileSystem(anon=True, default_fill_cache=False, skip_instance_cache=True)


def trim_memory() -> int:
    """
    Efficiently trim memory in a Dask worker context.
    
    Returns:
        int: Approximate number of objects collected
    """
    # Force garbage collection
    collected = gc.collect()
    
    # Attempt to release memory back to the system
    try:
        # Dask-specific memory management
        dask.distributed.worker.logger.debug("Attempting memory trim")
        
        # Release worker local memory if using distributed
        dask.distributed.worker.memory_limit = None
    except Exception:
        pass
    
    # Additional system-level memory management
    try:
        # Force Python to return memory to the system
        psutil.Process().memory_maps(grouped=True)
    except Exception:
        pass
    
    return collected



def post_or_put(url: str, data: dict):
    """Post or put data to url."""
    r = requests.post(url, json=data)
    if r.status_code == 409:
        new_url = url + f"/{data['id']}"
        # Exists, so update
        r = requests.put(new_url, json=data)
        # Unchanged may throw a 404
        if not r.status_code == 404:
            r.raise_for_status()
    else:
        r.raise_for_status()
    return r.status_code

def list_s3_objects(path, pattern='*', batch_size=5000):
    """
    List S3 objects with precise pattern matching, returning full S3 paths.
    
    Args:
        path (str): Full S3 path (s3://bucket-name/prefix/) or just bucket name
        pattern (str, optional): Filename pattern to match. Defaults to '*'.
        batch_size (int, optional): Minimum number of filtered objects to collect. Defaults to 1000.
    
    Yields:
        list: A page of full S3 object paths matching the specified criteria
    
    Raises:
        ValueError: If the path is invalid
    """
   
    # Parse the S3 path
    if path.startswith('s3://'):
        # Remove 's3://' and split into bucket and prefix
        parsed_path = path[5:].split('/', 1)
        bucket_name = parsed_path[0]
        prefix = parsed_path[1] if len(parsed_path) > 1 else ''
    else:
        # If no 's3://' assume it's just the bucket name
        bucket_name = path
        prefix = ''
    
    # URL decode the bucket name and prefix to handle special characters
    bucket_name = urllib.parse.unquote(bucket_name)
    prefix = urllib.parse.unquote(prefix)
    
    # Ensure prefix ends with a '/' if it's not empty
    if prefix and not prefix.endswith('/'):
        prefix += '/'
    
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    # Use regex for more precise filename matching
    filename_pattern = re.compile(fnmatch.translate(pattern) + '$')
    
    # Keep track of continuation token
    continuation_token = None
    collected_results = []
    total_collected = 0  # Track total collected items
    
    while True:
        # Prepare pagination arguments
        list_kwargs = {
            'Bucket': bucket_name,
            'Prefix': prefix,
        }
        
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        
        response = s3.list_objects_v2(**list_kwargs)
        
        if 'Contents' in response:
            # Filter objects based on precise filename matching and .nc file extension
            filtered_page = [
                f's3://{bucket_name}/{obj["Key"]}' for obj in response['Contents'] 
                if filename_pattern.match(obj['Key'].split('/')[-1]) and obj['Key'].endswith('.nc')
            ]

            # filtered_page = [
            #     obj for obj in response['Contents'] 
            #     if filename_pattern.match(obj['Key'].split('/')[-1]) and obj['Key'].endswith('.nc')
            # ]
            # 
            collected_results.extend(filtered_page)
            total_collected += len(filtered_page)
            
            while len(collected_results) >= batch_size:
                yield collected_results[:batch_size]
                collected_results = collected_results[batch_size:]

        if not response.get('IsTruncated', False):
            break
        continuation_token = response.get('NextContinuationToken')
    
    if collected_results:
        yield collected_results

    
def split_s3_path(full_path):
    """
    Splits ITS_LIVE S3 path into (base_path, relative_path_with_slash)
    
    Args:
        full_url: e.g. "s3://its-live-data/velocity_image_pair/010W/a/b/c/"
    
    Returns:
        tuple: (base_path, relative_path)
        e.g. ("s3://its-live-data/velocity_image_pair/", "010W/a/b/c/")
    """
    parsed = urlparse(full_path)
    path_parts = parsed.path.strip('/').split('/')
    
    # Base is always the first two parts (bucket + velocity_image_pair)
    base = f"{parsed.scheme}://{parsed.netloc}/{path_parts[0]}/"
    
    # Relative path is everything after, preserving trailing slash
    rel_path = '/'.join(path_parts[1:]) + '/' if len(path_parts) > 1 else ""
    
    return base, rel_path    
 
def s3_path_to_local_path(s3_path, cache_root="/tmp/duck_cache"):
    """Convert s3://bucket/prefix/file.parquet to /tmp/duck_cache/bucket/prefix/file.parquet"""
    parsed = urlparse(s3_path)
    bucket = parsed.netloc
    prefix_and_file = parsed.path.lstrip('/')  # Remove leading slash
    
    local_path = os.path.join(cache_root, bucket, prefix_and_file.replace("**/*.parquet", ""))
    return local_path


def cache_parquet_file(s3_paths: List[str], cache_root: str = "/tmp/duck_cache") -> List[str]:
    local_paths = []
    for s3_path in s3_paths:
        local_path = s3_path_to_local_path(s3_path, cache_root=cache_root)
        matching_files = s3_fs.glob(s3_path)
        
        # Download file if not already cached
        if not os.path.exists(local_path):
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            logger.debug(f"Downloading {len(matching_files)} parquet files to: {local_path}")
            s3_fs.get(matching_files, local_path)
        else:
            logger.debug(f"Using cached {local_path}")
        
        local_paths.append(local_path+"**/*.parquet")
    return local_paths

def extract_years_from_datetime_str(datetime_str):
    """Extract years from STAC datetime string like '1980-01-01T00:00:00Z/1981-12-31T23:59:59Z'"""
    if '/' not in datetime_str:
        # Single datetime, extract year
        return [datetime_str.split('-')[0]]
    
    # Range format, extract years from both ends
    start_dt, end_dt = datetime_str.split('/')
    start_year = start_dt.split('-')[0]
    end_year = end_dt.split('-')[0]
    
    # Generate all years in range
    return [str(year) for year in range(int(start_year[0:4]), int(end_year[0:4]) + 1)]


def get_overlapping_grid_names(geojson_geometry: dict = {},
                               base_href: str = "s3://its-live-data/test-space/stac/geoparquet/latlon",
                               partition_type: str = "latlon",
                               date_range: str = "all",
                               resolution: int = 2,
                               overlap: str = "overlap"):
    """
    Generates a list of S3 path prefixes corresponding to spatial grid tiles that overlap
    with the provided GeoJSON geometry. These paths are intended for discovering Parquet files
    in a spatially partitioned STAC dataset.

    This is a workaround: ideally, spatial filtering should be handled within the Parquet metadata
    or using spatial indices rather than inferring intersecting tiles manually.

    Parameters:
    ----------
    geojson_geometry : dict, optional
        A GeoJSON geometry dictionary specifying the spatial region of interest.
        The function will find grid cells (by centroid) that intersect with this geometry.
    base_href : str, optional
        The base S3 path where partitioned STAC data is stored. The function will append
        grid identifiers and mission names to this prefix.
    partition_type : str, optional
        Type of partitioning used. Supports:
        - "latlon": Fixed 10x10 degree lat/lon grids with cell names like "N60W040"
        - "h3": H3 hexagonal grid system using resolution and overlap
    date_range : str, optional
    A date range string in the format "start_date/end_date" to filter partitions by date.
        Filters out partitions with no data in the specified date range.
    resolution : int, optional
        Only used if `partition_type` is "h3". Specifies the resolution of the H3 hex cells.
    overlap : str, optional
        Only used if `partition_type` is "h3". Passed to the `h3shape_to_cells_experimental` function
        to control overlap behavior.

    Returns:
    -------
    List[str]
        A list of valid S3-style path prefixes (with wildcards) that point to `.parquet` files
        under spatial partitions overlapping the input geometry.
    
    """
    if date_range != "all":
        year_ranges = extract_years_from_datetime_str(date_range)
    else:
        year_ranges = None

    logger.debug(f"Extracted year ranges: {year_ranges}")
    
    if partition_type == "latlon":
        # ITS_LIVE uses a fixed 10 by 10 grid  (centroid as name for the cell e.g. N60W040)
        def lat_prefix(lat):
            return f"N{abs(lat):02d}" if lat >= 0 else f"S{abs(lat):02d}"

        def lon_prefix(lon):
            return f"E{abs(lon):03d}" if lon >= 0 else f"W{abs(lon):03d}"
            
        geom = shape(geojson_geometry)
        missions = ["landsatOLI", "sentinel1", "sentinel2"]
        
        if not geom.is_valid:
            geom = geom.buffer(0)
    
        minx, miny, maxx, maxy = geom.bounds
    
        # Center-based grid! 
        lon_center_start = int(math.floor((minx - 5) / 10.0)) * 10
        lon_center_end   = int(math.ceil((maxx + 5) / 10.0)) * 10
        lat_center_start = int(math.floor((miny - 5) / 10.0)) * 10
        lat_center_end   = int(math.ceil((maxy + 5) / 10.0)) * 10
        
        grids = set()
        for lon_c in range(lon_center_start, lon_center_end + 1, 10):
            for lat_c in range(lat_center_start, lat_center_end + 1, 10):
                tile = box(lon_c - 5, lat_c - 5, lon_c + 5, lat_c + 5)
                if geom.intersects(tile):
                    name = f"{lat_prefix(lat_c)}{lon_prefix(lon_c)}"
                    grids.add(name)
                    
        prefixes = [f"{base_href}/{p}/{i}" for p in missions for i in list(grids)]
        search_prefixes = [f"{path}/**/*.parquet" for path in prefixes if path_exists(path)]       
        return search_prefixes
    elif partition_type=="h3":
        grids_hex = h3.h3shape_to_cells_experimental(h3.geo_to_h3shape(geojson_geometry), resolution, overlap)
        logger.debug(f"Found {len(grids_hex)} H3 grids for geometry: {geojson_geometry}")
        grids = [int(hs, 16) for hs in grids_hex]
        prefixes = [f"{base_href}/{p}" for p in grids]
        # TODO: implement year filtering
        search_prefixes = [f"{prefix}/**/*.parquet" for prefix in prefixes if path_exists(prefix)]
        return search_prefixes
    else:
        raise NotImplementedError(f"Partition {partition_type} not implemented.")

def expr_to_sql(expr):
    """
    Transform a cql expression into SQL, I wonder if the library does it.
    """
    op = expr["op"]
    left, right = expr["args"]
    
    # Get property name if dict with "property" key, else literal
    def val_to_sql(val):
        if isinstance(val, dict) and "property" in val:
            prop = val["property"]
            if not prop.isidentifier():
                return f'"{prop}"'
            return prop
        elif isinstance(val, str):
            # quote strings
            return f"'{val}'"
        else:
            return str(val)

    left_sql = val_to_sql(left)
    right_sql = val_to_sql(right)

    # Map operators
    op_map = {
        "=": "=",
        "==": "=",
        ">=": ">=",
        "<=": "<=",
        ">": ">",
        "<": "<",
        "!=": "<>",
        "<>": "<>"
    }
    sql_op = op_map.get(op, op)
    return f"{left_sql} {sql_op} {right_sql}"

def filters_to_where(filters):
    # filters is a list of expressions combined with AND
    sql_parts = [expr_to_sql(f) for f in filters if f and f != {}]
    return " AND ".join(sql_parts)
    
def path_exists(path: str) -> bool:
    if path.startswith("s3://"):
        return s3_fs.exists(path)
    else:
        return os.path.exists(path)

def build_cql2_filter(filters_list):
    valid_filters = [f for f in filters_list if f and f != {}]
    if not valid_filters:
        return None
    return filters_list[0] if len(filters_list) == 1 else {"op": "and", "args": filters_list}



def serverless_search(base_catalog_href: str = "s3://its-live-data/test-space/stac/geoparquet/latlon",
                      search_kwargs: dict = {},
                      engine: str = "rustac",
                      cache: bool = True,
                      reduce_spatial_search=True,
                      partition_type: str = "latlon",
                      resolution: int = 2,
                      overlap: str = "overlap", 
                      asset_type: str = ".nc"):
    """
    Performs a serverless!! search over partitioned STAC catalogs stored in Parquet format for the ITS_LIVE project.

    Parameters
    ----------
    base_catalog_href : str
        Base URI of the ITS_LIVE STAC catalog or geoparquet collection. This should point to the
        root location where spatial partitions are stored (e.g. "s3://its-live-data/test-space/stac/geoparquet/latlon").
    search_kwargs : dict, optional
        Dictionary of search parameters compatible with the STAC API. Can include spatial queries
        (e.g., `intersects`) and metadata filters (e.g., `datetime`, `platform`, etc).
    engine : str, optional
        The backend engine to use for querying. Supported options:
        - "rustac": Uses the Rust STAC client (`rustac.DuckdbClient`)
        - "duckdb": Uses DuckDB SQL for querying parquet partitions
    cache: bool, optional
        Wheter to cache the parquet files in memory.
    reduce_spatial_search : bool, optional
        Whether to pre-filter the list of parquet files using overlapping spatial partitions.
        If False, all files under the base path will be searched.
    partition_type : str, optional
        The spatial partitioning scheme used. Supports:
        - "latlon": 10x10 degree tiles (default)
        - "h3": Hexagonal grid (requires `resolution` and `overlap`)
    resolution : int, optional
        Only used if `partition_type` is "h3". Defines the granularity of H3 spatial partitioning.
    overlap : str, optional
        Only used with H3 partitioning. Passed to the `h3shape_to_cells_experimental()` function
        to handle partial overlaps.
    asset_type : str, optional
        A string suffix filter to match asset HREFs (e.g., ".nc" for NetCDF files).

    Returns
    -------
    List[str]
        A list of asset URLs (typically `.nc` NetCDF files) that match the search criteria.

    """
    store = base_catalog_href
    search_prefixes = []

    if reduce_spatial_search:
        if "intersects" in search_kwargs:
            search_prefixes = get_overlapping_grid_names(base_href=store,
                                                         geojson_geometry=search_kwargs["intersects"],
                                                         date_range=search_kwargs["datetime"] if "datetime" in search_kwargs else "all",
                                                         partition_type=partition_type,
                                                         resolution=resolution,
                                                         overlap=overlap)
    else:
        if partition_type == "latlon":
            search_prefixes = [f"{store}/{mission}/**/*.parquet" for mission in ["landsatOLI", "sentinel1", "sentinel2"]]
        else:
            search_prefixes = [f"{store}/**/*.parquet"]


    if cache:
        # Cache the parquet files locally
        search_prefixes = cache_parquet_file(search_prefixes)

    filters = search_kwargs["filter"] if "filter" in search_kwargs else []

    logger.debug(f"Searching in {search_prefixes} with filters: {filters} ")
    hrefs = []
    # TODO: this could run in parallel on a thread or could be passed all to DuckDB/rustac as a combined list of paths.
    # for debugging purposes querying one by one is more convenient for now.
    for prefix in search_prefixes:
        try:
            if engine == "duckdb":
                # TODO: make it more flexible
                filters_sql = filters_to_where(filters)
                logger.debug(f"Filters as SQL: {filters_sql}")
                geojson_str = json.dumps(search_kwargs["intersects"])
                date_filter_sql = ""
                if "datetime" in search_kwargs:
                    date_range = search_kwargs["datetime"]
                    start_date, end_date = date_range.split("/")

                    if start_date and end_date:
                        date_filter_sql = f"AND datetime BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'"
                    elif start_date:
                        date_filter_sql = f"AND datetime >= TIMESTAMP '{start_date}'"
                    elif end_date:
                        date_filter_sql = f"AND datetime <= TIMESTAMP '{end_date}'"                
                query = f"""
                    SELECT 
                        assets -> 'data' ->> 'href' AS data_href
                    FROM read_parquet('{prefix}', union_by_name=true)
                    WHERE ST_Intersects(
                        geometry,
                        ST_GeomFromGeoJSON('{geojson_str}')
                    )
                    {date_filter_sql}
                    AND {filters_sql}
                """
                logger.debug(f"Running DuckDB query: {query}")
                items = con.execute(query).df() # memory intensive?
                links = items["data_href"].to_list()
                hrefs.extend(links)
            elif engine == "rustac":
                # can we use include to only bring the asset links?
                search_kwargs["filter"] = build_cql2_filter(filters)
                client = rustac.DuckdbClient()
                items = client.search(prefix, **search_kwargs)
                for item in items:
                    for asset in item["assets"].values():
                        if "data" in asset["roles"] and asset["href"].endswith(".nc"):
                            hrefs.append(asset["href"])
            else:
                raise NotImplementedError(f"Not a valid query engine: {engine}")
            logger.info(f"Prefx: {prefix} | matching items: {len(items)}")
        except Exception as e:
            logger.error(f"Error while searching in {prefix}: {e}")
        
    return sorted(list(set(hrefs)))


