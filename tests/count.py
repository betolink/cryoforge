#!/usr/bin/env python3
import boto3
import re
import fnmatch
import urllib.parse
import os
import json
import datetime
import argparse
import concurrent.futures
import signal
import sys
import time
from botocore import UNSIGNED
from botocore.config import Config

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(sig, frame):
    """Handle CTRL+C by setting the shutdown flag"""
    global shutdown_requested
    if not shutdown_requested:
        print("\nShutdown requested. Saving progress and exiting gracefully...")
        shutdown_requested = True
    else:
        print("\nForced exit.")
        sys.exit(1)

def count_s3_files(s3_path, extension=".nc", workers=8, format_type="text", cache_dir=None):
    """
    Count files with specified extension in S3 directories and cache results.
    
    Args:
        s3_path (str): Full S3 path (s3://bucket-name/prefix/)
        extension (str): File extension to count
        workers (int): Number of parallel workers
        format_type (str): Output format (text, csv, json)
        cache_dir (str): Directory to store cache files
        
    Returns:
        dict: Directory counts
    """
    # Register signal handler for CTRL+C
    signal.signal(signal.SIGINT, signal_handler)
    
    # Parse the S3 path
    if not s3_path.startswith('s3://'):
        raise ValueError("S3 path must start with 's3://'")
    
    s3_path = s3_path.rstrip('/') + '/'
    parsed_path = s3_path[5:].split('/', 1)
    bucket_name = parsed_path[0]
    prefix = parsed_path[1] if len(parsed_path) > 1 else ''
    
    # Setup cache
    today = datetime.date.today().isoformat()
    if cache_dir is None:
        cache_dir = os.path.expanduser("~/.s3_counter_cache")
    os.makedirs(cache_dir, exist_ok=True)
    
    cache_file = os.path.join(
        cache_dir, 
        f"{bucket_name}_{prefix.replace('/', '_')}_{extension.replace('.', '_')}.cache"
    )
    
    # Load cache
    cache = {}
    if os.path.exists(cache_file):
        with open(cache_file, 'r') as f:
            for line in f:
                if line.strip():
                    parts = line.strip().split(':')
                    if len(parts) >= 3:
                        dir_path, count, date = parts[0], parts[1], parts[2]
                        # Accept results from today or previous days
                        cache[dir_path] = int(count)
    
    print(f"Using bucket: {bucket_name}")
    print(f"Using prefix: {prefix}")
    
    # Get all directories
    print(f"Listing directories in s3://{bucket_name}/{prefix}...")
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )
    
    all_dirs = []
    if 'CommonPrefixes' in response:
        all_dirs = [prefix_obj['Prefix'] for prefix_obj in response['CommonPrefixes']]
    
    total = len(all_dirs)
    if total == 0:
        print("No directories found")
        return {}
    
    # Find directories that need processing
    to_process = [dir_path for dir_path in all_dirs if dir_path not in cache]
    print(f"Found {total} directories, {len(to_process)} require counting...")
    
    # Function for processing a single directory
    def process_directory(dir_path):
        if shutdown_requested:
            return dir_path, None
            
        file_count = 0
        try:
            # List all objects with this prefix and count those ending with the extension
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket_name, Prefix=dir_path):
                if 'Contents' in page:
                    file_count += sum(1 for obj in page['Contents'] 
                                    if obj['Key'].endswith(extension))
                
                # Check for shutdown request during pagination
                if shutdown_requested:
                    break
        except Exception as e:
            print(f"\nError processing {dir_path}: {e}")
            return dir_path, None
            
        return dir_path, file_count
    
    # Process directories in parallel and update cache incrementally
    results = {}
    if to_process:
        # Create a ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_dir = {executor.submit(process_directory, dir_path): dir_path 
                            for dir_path in to_process}
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_dir):
                dir_path, count = future.result()
                
                # Only add valid results to our results dict
                if count is not None:
                    results[dir_path] = count
                    
                    # Write this result to cache immediately
                    with open(cache_file, 'a') as f:
                        f.write(f"{dir_path}:{count}:{today}\n")
                
                # Update progress
                completed += 1
                percent = completed * 100 // len(to_process)
                progress_msg = f"Processing: {percent:3d}% ({completed}/{len(to_process)})"
                print(f"{progress_msg}", end='\r')
                
                # Check for shutdown
                if shutdown_requested:
                    print("\nSaving progress and shutting down...")
                    # Cancel pending futures
                    for f in future_to_dir:
                        if not f.done():
                            f.cancel()
                    break
        
        if not shutdown_requested:
            print("\nCounting complete!")
    
    # Merge results with cache
    final_results = {**cache, **results}
    
    # If we're shutting down, don't display results
    if shutdown_requested:
        print(f"Progress saved to cache file: {cache_file}")
        print(f"Run the command again to resume from where you left off.")
        return final_results
    
    # Rebuild cache file to remove duplicates (keeping only the latest entry for each dir)
    with open(cache_file + '.new', 'w') as f:
        for dir_path, count in final_results.items():
            f.write(f"{dir_path}:{count}:{today}\n")
    
    # Replace old cache with new one
    if os.path.exists(cache_file + '.new'):
        os.replace(cache_file + '.new', cache_file)
    
    # Format and return output
    if format_type == "csv":
        print("directory,count")
        for dir_path, count in sorted(final_results.items(), key=lambda x: x[1]):
            print(f"{dir_path.rstrip('/')},{count}")
    elif format_type == "json":
        json_results = {dir_path.rstrip('/'): count for dir_path, count in final_results.items()}
        print(json.dumps(json_results, indent=2))
    else:  # text format
        print("Results:")
        for dir_path, count in sorted(final_results.items(), key=lambda x: x[1]):
            print(f"{dir_path.rstrip('/'):40} {count}")
    
    return final_results

def main():
    parser = argparse.ArgumentParser(description="Count files in S3 directories")
    parser.add_argument("s3_path", help="S3 path (s3://bucket-name/prefix/)")
    parser.add_argument("--extension", "-e", default=".nc", help="File extension to count (default: .nc)")
    parser.add_argument("--workers", "-w", type=int, default=8, help="Number of worker threads (default: 8)")
    parser.add_argument("--format", "-f", choices=["text", "csv", "json"], default="text", 
                        help="Output format (default: text)")
    parser.add_argument("--cache-dir", "-c", help="Cache directory (default: ~/.s3_counter_cache)")
    
    args = parser.parse_args()
    
    try:
        count_s3_files(
            args.s3_path, 
            extension=args.extension, 
            workers=args.workers, 
            format_type=args.format, 
            cache_dir=args.cache_dir
        )
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
