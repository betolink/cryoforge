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

def search_stac(bbox, max_items=100, percent_valid_pixels=None):
    stac_url = "https://stac.itslive.cloud/"
    catalog = Client.open(stac_url)
    print(f"Searching STAC catalog with bbox: {bbox}")

    search_kwargs = {
        "collections": ["itslive-granules"],
        "bbox": bbox,
        "max_items": max_items
    }

    if percent_valid_pixels is not None:
        search_kwargs["filter"] = {
            "op": ">=",
            "args": [{"property": "percent_valid_pixels"}, percent_valid_pixels]
        }
        search_kwargs["filter_lang"] = "cql2-json"

    search = catalog.search(**search_kwargs)
    print("\nSearch Results:")
    
    hrefs = []
    for item in search.items():
        for asset in item.assets.values():
            if "data" in asset.roles and asset.href.endswith(".nc"):
                hrefs.append(asset.href)

    return hrefs

def main():
    parser = argparse.ArgumentParser(description='Search STAC catalog based on bounding box derived from a NetCDF file.')
    parser.add_argument('nc_url', help='URL of the NetCDF file')
    parser.add_argument('--max-items', type=int, default=100, help='Maximum number of items to return (default: 100)')
    parser.add_argument('--percent-valid-pixels', type=int, help='Filter items by minimum percent valid pixels (e.g., 90)')
    args = parser.parse_args()

    try:
        bbox = get_bbox_wgs84(args.nc_url)
        results = search_stac(bbox, args.max_items, args.percent_valid_pixels)
        print(json.dumps(results, indent=2))
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
