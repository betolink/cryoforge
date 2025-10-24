import sys
import re
import json
import logging
import xarray as xr
from pyproj import Transformer
from pystac_client import Client
import rich_click as click

from cryoforge.tooling import serverless_search

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_CATALOGS = {
    "stac": "https://stac.itslive.cloud",
    "duckstac": "s3://its-live-data/test-space/stac/geoparquet/h3r2",
    "rustac": "s3://its-live-data/test-space/stac/geoparquet/h3r2",
}


def get_bbox_wgs84(nc_url):
    ds = xr.open_dataset(nc_url, backend_kwargs={"storage_options": {"anon": True}})
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


def search_stac(stac_catalog, args: dict):
    max_items = args.get("max_items", 100)
    percent_valid_pixels = args.get("percent_valid_pixels", None)
    bbox = args.get("bbox", "-180,-90,180,90")

    catalog = Client.open(stac_catalog)
    search_kwargs = {
        "collections": ["itslive-granules"],
        "bbox": bbox,
        "max_items": max_items,
    }

    if percent_valid_pixels is not None:
        search_kwargs["filter"] = {
            "op": ">=",
            "args": [{"property": "percent_valid_pixels"}, percent_valid_pixels],
        }
        search_kwargs["filter_lang"] = "cql2-json"

    search = catalog.search(**search_kwargs)

    hrefs = []
    for item in search.items():
        for asset in item.assets.values():
            if "data" in asset.roles and asset.href.endswith(".nc"):
                hrefs.append(asset.href)

    return hrefs


def parse_op_value(raw_value):
    """
    Parse an operator and value from a string like:
      '>10', '<=2020-01-01', '~%foo%', '~~%bar%'
    """
    if isinstance(raw_value, (int, float)):
        return "=", raw_value

    raw_value = str(raw_value)

    if raw_value.startswith("~~"):
        return "ilike", raw_value[2:]
    elif raw_value.startswith("~"):
        return "like", raw_value[1:]
    elif "%" in raw_value and not re.match(r"^(>=|<=|!=|=|>|<)", raw_value):
        return "like", raw_value

    # Standard comparison operators
    match = re.match(r"^(>=|<=|!=|=|>|<)?(.*)$", raw_value)
    if not match:
        raise ValueError(f"Invalid filter syntax: {raw_value}")
    op, val = match.groups()
    op = op or "="

    try:
        val = float(val) if "." in val else int(val)
    except ValueError:
        pass

    return op, val


def search_duckstac(catalog: str, args: dict):
    filters = []

    # --- Build filters with operator support ---
    for prop in [
        "created",
        "date_dt",
        "updated",
        "version",
        "datetime",
        "latitude",
        "longitude",
        "proj:code",
        "scene_1_id",
        "scene_2_id",
        "end_datetime",
        "mid_datetime",
        "start_datetime",
        "sat:orbit_state",
        "scene_1_path_row",
        "scene_2_path_row",
        "percent_valid_pixels",
        "platform",
    ]:
        key = prop.replace(":", "_")
        raw_value = args.get(key)
        if raw_value is not None:
            op, value = parse_op_value(raw_value)
            filters.append({"op": op, "args": [{"property": prop}, value]})

    # --- Geometry selection ---
    if args.get("geojson"):
        try:
            with open(args["geojson"], "r") as f:
                geom = json.load(f)["geometry"]
        except Exception as e:
            raise ValueError(f"Error reading GeoJSON file: {e}")
    elif args.get("bbox"):
        bbox = args["bbox"]
        geom = {
            "type": "Polygon",
            "coordinates": [
                [
                    [bbox[0], bbox[1]],
                    [bbox[2], bbox[1]],
                    [bbox[2], bbox[3]],
                    [bbox[0], bbox[3]],
                    [bbox[0], bbox[1]],
                ]
            ],
        }
    else:
        raise ValueError("Either --geojson or --bbox must be provided.")

    search_args = {"intersects": geom, "filter": filters}

    if args.get("datetime"):
        search_args["datetime"] = args["datetime"]

    if args.get("verbose"):
        print(f"Searching DuckSTAC with args: {json.dumps(search_args, indent=2)}")

    return serverless_search(
        base_catalog_href=catalog,
        search_kwargs=search_args,
        engine="duckdb",
        cache=args.get("cache", False),
        reduce_spatial_search=True,
        partition_type="h3",
        resolution=2,
        overlap="bbox_overlap",
    )


def search_rustac(catalog: str, args: dict):
    print("Rustac search is not implemented yet.")
    return None


@click.command()
@click.option("--catalog", help="URL of the STAC catalog.")
@click.option(
    "--query-engine",
    type=click.Choice(["duckstac", "rustac", "stac"]),
    default="duckstac",
    help="Query engine to use.",
)
@click.option("--granule", help="URL of an overlapping ITS_LIVE .nc granule file.")
@click.option(
    "--bbox", help="Bounding box in the format 'lon_min,lat_min,lon_max,lat_max'."
)
@click.option("--geojson", help="GeoJSON file with geometry to filter items.")
@click.option("--datetime", help="Datetime range in STAC format.")
@click.option(
    "--cache", is_flag=True, help="Cache geoparquet files in disk if present."
)
@click.option(
    "--max-items", type=int, default=100, help="Maximum number of items to return."
)
@click.option("--percent-valid-pixels", help="Minimum percent valid pixels filter.")
@click.option("--epsg", help="EPSG projection to filter.")
@click.option("--output", type=click.Path(), help="Local path to save results.")
# Optional properties
@click.option("--id")
@click.option("--created")
@click.option("--date-dt")
@click.option("--updated")
@click.option("--version")
@click.option("--latitude")
@click.option("--longitude")
@click.option("--proj-code")
@click.option("--scene-1-id")
@click.option("--scene-2-id")
@click.option("--end-datetime")
@click.option("--mid-datetime")
@click.option("--start-datetime")
@click.option("--sat-orbit-state")
@click.option("--scene-1-path-row")
@click.option("--scene-2-path-row")
@click.option("--platform")
@click.option("--verbose", is_flag=True, help="Enable verbose logging.", default=False)
def search_items(**kwargs):
    """Search STAC or DuckSTAC catalogs using rich-click."""
    args = dict(kwargs)
    results = []

    try:
        if args.get("granule"):
            bbox = get_bbox_wgs84(args["granule"])
            args["bbox"] = bbox
        elif args.get("bbox"):
            bbox = list(map(float, args["bbox"].split(",")))
            if len(bbox) != 4:
                raise ValueError("Bounding box must contain exactly four values.")
            args["bbox"] = bbox
        elif args.get("geojson"):
            with open(args["geojson"], "r") as f:
                geom = json.loads(f.read())["geometry"]
            coords = geom["coordinates"][0]
            bbox = [
                min(c[0] for c in coords),
                min(c[1] for c in coords),
                max(c[0] for c in coords),
                max(c[1] for c in coords),
            ]
            args["bbox"] = bbox

        user_engine = args.get("query_engine")
        has_spatial = any(args.get(k) for k in ["bbox", "geojson", "granule"])
        has_id_only = args.get("id") and not has_spatial

        if not user_engine:
            if has_id_only:
                engine = "stac"
            elif has_spatial:
                engine = "duckstac"
            else:
                raise ValueError(
                    "Must provide either an ID or spatial parameters (bbox, geojson, granule)."
                )
        else:
            engine = user_engine

        catalog = args.get("catalog") or DEFAULT_CATALOGS.get(engine)
        if not catalog:
            raise ValueError(f"No catalog available for engine '{engine}'")
        args["catalog"] = catalog

        if args.get("verbose"):
            logger.info(f"Using engine '{engine}' with catalog '{catalog}'")

        if engine == "duckstac":
            results = search_duckstac(catalog, args)
        elif engine == "stac":
            results = search_stac(catalog, args)
        elif engine == "rustac":
            results = search_rustac(catalog, args)
        else:
            raise ValueError(f"Unknown query engine: {engine}")

    except Exception as e:
        click.secho(f"Error: {e}", fg="red", err=True)
        sys.exit(1)

    if results:
        if args.get("output"):
            with open(args["output"], "w") as f:
                for href in results:
                    f.write(href + "\n")
            click.secho(f"Wrote {len(results)} results to {args['output']}", fg="green")
        else:
            for href in results:
                click.echo(href)
    else:
        click.secho("No matching items found.", fg="yellow")


if __name__ == "__main__":
    search_items()
