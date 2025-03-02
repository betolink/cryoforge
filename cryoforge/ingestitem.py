"""Ingest sample data during docker-compose"""

import json
from pathlib import Path
from urllib.parse import urljoin
import argparse
import logging

import requests


def post_or_put(url: str, data: dict):
    """Post or put data to url."""
    logging.info(f"Posting to {url}")
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


def ingest_item(load_collection: bool = False, stac_server: str = "",  collection: str="", stac_item: str = "" ):
    """ingest stac item into the itslive collection."""
    if load_collection:
        with open("./cryoforge/stac-collection.json") as f:
            original_collection = json.load(f)
            post_or_put(urljoin(stac_server, "/collections"), original_collection)

    with open(stac_item) as f:
        item = json.load(f)

    post_or_put(urljoin(stac_server, f"collections/{collection}/items"), item)


def ingest_stac():
    """Ingest sample data during docker-compose"""
    parser = argparse.ArgumentParser(
        description="Generate metadata sidecar files for ITS_LIVE granules"
    )
    parser.add_argument(
        "-i", "--item", required=True, help="Path to a single ITS_LIVE STAC item file"
    )
    parser.add_argument("-t", "--target", required=True, help="STAC endpoint")
    parser.add_argument("-c", "--collection", required=True, help="STAC collection")
    parser.add_argument("-r", "--reload-collection", action="store_true", help="If present will reload/update the collection")

    args = parser.parse_args()

    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )

    logging.info(f"Ingesting {args.item}")
    stac_endpoint = args.target
    ingest_item(args.reload_collection, stac_endpoint, args.collection, args.item)


if __name__ == "__main__":
    ingest_stac()
