import argparse
import sys
import xarray as xr
from pyproj import Transformer
from pystac_client import Client
import json

import logging

from cryoforge.tooling import serverless_search


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_bbox_wgs84(nc_url):
    ds = xr.open_dataset(nc_url, backend_kwargs={"storage_options":{"anon": True}})
    x = ds.coords.get("x")
    y = ds.coords.get("y")
    epsg = ds["mapping"].attrs.get("spatial_epsg")

    if x is None or y is None or epsg is None:
        raise ValueError("x, y coordinates or EPSG code missing")

    minx, maxx = float(x.min()), float(x.max())
    miny, maxy = float(y.min()), float(y.max())

    transformer = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)
    lon_min, lat_min = transformer.transform(minx, miny)
    lon_max, lat_max = transformer.transform(maxx, maxy)

    return [lon_min, lat_min, lon_max, lat_max]

def search_stac(stac_catalog, args: dict  = {}):
    max_items = args.get("max_items", 100)
    percent_valid_pixels = args.get("percent_valid_pixels", None)
    bbox = args.get("bbox", "-180,-90,180,90")

    catalog = Client.open(stac_catalog)
    search_kwargs = {
        "collections": ["itslive-granules"],
        "bbox": bbox,
        "max_items": max_items
    }

    # TODO: add more filters and flexibility 
    if percent_valid_pixels is not None:
        search_kwargs["filter"] = {
            "op": ">=", "args": [{"property": "percent_valid_pixels"}, percent_valid_pixels]
        }
        search_kwargs["filter_lang"] = "cql2-json"

    search = catalog.search(**search_kwargs)
    
    hrefs = []
    for item in search.items():
        for asset in item.assets.values():
            if "data" in asset.roles and asset.href.endswith(".nc"):
                hrefs.append(asset.href)

    return hrefs


def search_duckstac(catalog: str = "s3://its-live-data/test-space/stac/geoparquet/h3r2", args: dict = {}):
    filters = [
        {"op": ">=", "args": [{"property": "percent_valid_pixels"}, args.percent_valid_pixels]},
        {'op': '=', 'args': [{'property': 'proj:code'}, args.epsg]} if args.epsg else {},
    ] 
    if args.geojson:
        try:
            with open(args.geojson, 'r') as f:
                geom = json.loads(f.read())["geometry"]
        except Exception as e:
            raise ValueError(f"Error reading GeoJSON file: {e}")
    elif args.bbox:
        geom = {
            "type": "Polygon",
            "coordinates": [[
            [args.bbox[0], args.bbox[1]],
            [args.bbox[2], args.bbox[1]],
            [args.bbox[2], args.bbox[3]],
            [args.bbox[0], args.bbox[3]],
            [args.bbox[0], args.bbox[1]]
            ]]
        }
    else:
        raise ValueError("Either --geojson, --granule or --bbox must be provided.")

    search_args = {
        "intersects": geom,
        "filter": filters
    }
    if args.datetime:
        search_args["datetime"] = args.datetime

    cache = args.cache if "cache" in args else False

    results = serverless_search(
            base_catalog_href=catalog,
            search_kwargs = search_args,
            engine = "duckdb",
            cache = cache,
            reduce_spatial_search = True,
            partition_type = "h3",
            resolution = 2,
            overlap = "bbox_overlap")
 
    return results

def search_rustac(catalog: str = "s3://its-live-data/test-space/stac/geoparquet/latlon", args: dict = {}):
    print("Rustac search is not implemented yet.")
    return None


def search_items():
    parser = argparse.ArgumentParser(description="Search STAC catalog based on bounding box derived from a NetCDF file.")
    parser.add_argument("--catalog", help="URL of the STAC catalog")
    parser.add_argument("--query-engine", help="Query engine to use (duckstac, rustac, or pystac_client)", default="pystac_client")
    parser.add_argument("--granule", help="URL of an overlapping ITS_LIVE .nc granule file")
    parser.add_argument("--bbox", help="Bounding box in the format 'lon_min,lat_min,lon_max,lat_max'")
    parser.add_argument("--geojson", help="Geojson file with a geometry type to filter items")
    parser.add_argument("--datetime", help="Datetime range in STAC format: 'YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ'")
    parser.add_argument("--cache", help="Cache geoparquet files in disk if present", action="store_true")
    parser.add_argument("--max-items", type=int, default=100, help="Maximum number of items to return (default: 100)")
    parser.add_argument("--percent-valid-pixels", type=int, help="Filter items by minimum percent valid pixels (e.g., 90)")
    parser.add_argument("--epsg", type=str, help="EPSG projection to filter")
    parser.add_argument("--output", type=str, help="Local path to write URLs from matching items, if not used the output will be printed to stdout")
    args = parser.parse_args()
    results = []

    try:
        if args.granule:
            bbox = get_bbox_wgs84(args.granule)
            args.bbox = bbox  # Set bbox from granule
        elif args.bbox:
            bbox = list(map(float, args.bbox.split(",")))
            if len(bbox) != 4:
                raise ValueError("Bounding box must contain exactly four values.")
        elif args.geojson:
            try:
                with open(args.geojson, 'r') as f:
                    geom = json.loads(f.read())["geometry"]
                bbox = geom["coordinates"][0]  # Assuming the first polygon's coordinates
                bbox = [min(coord[0] for coord in bbox), min(coord[1] for coord in bbox),
                max(coord[0] for coord in bbox), max(coord[1] for coord in bbox)]
            except Exception as e:
                raise ValueError(f"Error reading GeoJSON file: {e}")
        else:
            raise ValueError("Either --granule or --bbox must be provided.")
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = "https://stac.itslive.cloud/"


        if args.query_engine == "duckstac":
            results = search_duckstac(catalog, args)
        elif args.query_engine == "rustac":
            results = search_rustac(catalog, args)
        else:
            results = search_stac(catalog, bbox, args)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if args.output and results:
        with open(args.output, "w") as f:
            for href in results:
                f.write(href + "\n")
    elif results:
        for href in results:
            print(href)
    else:
        print("No matching items found.")


if __name__ == "__main__":
    search_items()
