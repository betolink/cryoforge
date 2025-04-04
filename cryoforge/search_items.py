import argparse
import sys
import json
import xarray as xr
from pyproj import Transformer
from pystac_client import Client

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

def search_stac(stac_catalog, bbox, max_items=100, percent_valid_pixels=None):
    catalog = Client.open(stac_catalog)
    search_kwargs = {
        "collections": ["itslive-granules"],
        "bbox": bbox,
        "max_items": max_items
    }

    # TODO: add more filters and flexibility 
    if percent_valid_pixels is not None:
        search_kwargs["filter"] = {
            "op": ">=",
            "args": [{"property": "percent_valid_pixels"}, percent_valid_pixels]
        }
        search_kwargs["filter_lang"] = "cql2-json"

    search = catalog.search(**search_kwargs)
    
    hrefs = []
    for item in search.items():
        for asset in item.assets.values():
            if "data" in asset.roles and asset.href.endswith(".nc"):
                hrefs.append(asset.href)

    return hrefs

def search_items():
    parser = argparse.ArgumentParser(description="Search STAC catalog based on bounding box derived from a NetCDF file.")
    parser.add_argument("--catalog", help="URL of the STAC catalog")
    parser.add_argument("--granule", help="URL of the NetCDF file")
    parser.add_argument("--bbox", help="Bounding box in the format 'lon_min,lat_min,lon_max,lat_max'")
    parser.add_argument("--max-items", type=int, default=100, help="Maximum number of items to return (default: 100)")
    parser.add_argument("--percent-valid-pixels", type=int, help="Filter items by minimum percent valid pixels (e.g., 90)")
    args = parser.parse_args()

    try:
        if args.granule:
            bbox = get_bbox_wgs84(args.granule)
        elif args.bbox:
            bbox = list(map(float, args.bbox.split(",")))
            if len(bbox) != 4:
                raise ValueError("Bounding box must contain exactly four values.")
        else:
            raise ValueError("Either --granule or --bbox must be provided.")
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = "https://stac.itslive.cloud/"
        results = search_stac(catalog, bbox, args.max_items, args.percent_valid_pixels)
        for href in results:
            print(href)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    search_items()
