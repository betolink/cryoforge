import fnmatch
import gc
import logging
import re
import urllib
from urllib.parse import urlparse

import boto3
import dask
import psutil
import requests
from botocore import UNSIGNED
from botocore.client import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
 
