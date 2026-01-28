#!/usr/bin/env python
import json
import argparse
import os
import uuid
from datetime import datetime


def generate_cube_stac_items(input_filename: str, output_filename: str):
    """
    Reads a cube catalog GeoJSON FeatureCollection, transforms each feature into a complete
    STAC Item for datacubes.
    """

    # Load collection configuration
    try:
        with open("cubes.json", "r") as f:
            collection = json.load(f)
    except FileNotFoundError:
        print("Error: Collection file 'cubes.json' not found.")
        return
    except json.JSONDecodeError:
        print("Error: Could not decode JSON from 'cubes.json'. Check file integrity.")
        return

    # Load the input data
    try:
        with open(input_filename, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Input file '{input_filename}' not found.")
        return
    except json.JSONDecodeError:
        print(
            f"Error: Could not decode JSON from '{input_filename}'. Check file integrity."
        )
        return

    # Extract collection info
    collection_id = collection["id"]
    collection_href = f"https://stac.itslive.cloud/collections/{collection_id}"

    stac_items = []

    for feature in data.get("features", []):
        props = feature["properties"]
        
        # Skip features without datacubes
        if not props.get("datacube_exist", 0):
            continue

        # Extract basic properties
        epsg_code = props["epsg"]
        zarr_url = props["zarr_url"]
        granule_count = props.get("granule_count", 0)
        roi_percent_coverage = props.get("roi_percent_coverage", 0)

        # Generate unique ID for the item
        item_id = f"itslive-{str(uuid.uuid4())}"

        # Extract geometry and bbox
        geometry = feature.get("geometry", None)
        bbox = feature.get("bbox", None)

        # Ensure geometry is not null - if missing, create from bbox or geometry_epsg
        if geometry is None:
            # Try to create geometry from geometry_epsg if available
            if "geometry_epsg" in props:
                geom_epsg = props["geometry_epsg"]
                geometry = geom_epsg
            else:
                # Create a minimal geometry as fallback
                geometry = {
                    "type": "Polygon",
                    "coordinates": [[[-180, -90], [180, -90], [180, 90], [-180, 90], [-180, -90]]]
                }

        # If no bbox in feature, calculate from geometry
        if bbox is None and geometry:
            coords = geometry["coordinates"][0]
            lons = [coord[0] for coord in coords]
            lats = [coord[1] for coord in coords]
            bbox = [min(lons), min(lats), max(lons), max(lats)]

        # Extract projection information from geometry_epsg
        proj_bbox = None
        proj_shape = None
        if "geometry_epsg" in props:
            geom_epsg = props["geometry_epsg"]
            if geom_epsg["type"] == "Polygon":
                coords = geom_epsg["coordinates"][0]
                x_coords = [coord[0] for coord in coords]
                y_coords = [coord[1] for coord in coords]
                proj_bbox = [min(x_coords), min(y_coords), max(x_coords), max(y_coords)]
                
                # Calculate shape (assuming regular grid)
                x_range = max(x_coords) - min(x_coords)
                y_range = max(y_coords) - min(y_coords)
                # Assuming 120m resolution based on the example
                proj_shape = [int(y_range / 120), int(x_range / 120)]

        # Parse zarr URL to extract cube information
        zarr_filename = zarr_url.split("/")[-1].replace(".zarr", "")
        
        # Extract temporal extent from collection (default)
        start_datetime = "2016-04-28T23:36:30.722953216+00:00"
        end_datetime = "2021-12-16T23:36:33.557084928+00:00"

        # Create cube dimensions based on the example
        cube_dimensions = {
            "x": {
                "type": "spatial",
                "axis": "x",
                "extent": proj_bbox[:2] if proj_bbox else [0, 100000],
                "reference_system": epsg_code
            },
            "y": {
                "type": "spatial", 
                "axis": "y",
                "extent": proj_bbox[2:] if proj_bbox else [0, 100000],
                "reference_system": epsg_code
            },
            "mid_date": {
                "type": "temporal",
                "extent": [start_datetime, end_datetime]
            }
        }

        # Create cube variables based on the example
        cube_variables = {
            "v": {
                "type": "data",
                "dimensions": ["mid_date", "y", "x"],
                "description": "Ice velocity measurement in meters per year",
                "unit": "meter/year"
            },
            "v_error": {
                "type": "data",
                "dimensions": ["mid_date", "y", "x"],
                "description": "Ice velocity magnitude error",
                "unit": "meter/year"
            },
            "vx": {
                "type": "data",
                "dimensions": ["mid_date", "y", "x"],
                "description": "Ice velocity component in x direction",
                "unit": "meter/year"
            },
            "vy": {
                "type": "data",
                "dimensions": ["mid_date", "y", "x"],
                "description": "Ice velocity component in y direction",
                "unit": "meter/year"
            },
            "interp_mask": {
                "type": "data",
                "dimensions": ["mid_date", "y", "x"],
                "description": "True where velocity values have been interpolated"
            },
            "img_pair_info": {
                "type": "data",
                "dimensions": ["mid_date"],
                "description": "File attributes"
            }
        }

        # Create assets
        assets = {}

        # Main zarr asset
        assets["zarr"] = {
            "href": zarr_url.replace("http://", "https://"),
            "type": "application/vnd.zarr",
            "title": "ITS_LIVE Zarr datacube",
            "roles": ["data"]
        }

        # Add individual variable assets based on cube_variables
        for var_name, var_info in cube_variables.items():
            assets[var_name] = {
                "href": zarr_url.replace("http://", "https://"),
                "type": "application/vnd.zarr",
                "title": var_name,
                "raster:bands": [{
                    "name": var_name,
                    "description": var_info.get("description", ""),
                    "sampling": "point",
                    "data_type": "float32",
                    "units": var_info.get("unit", "")
                }] if var_info.get("unit") else [{
                    "name": var_name,
                    "description": var_info.get("description", ""),
                    "sampling": "point",
                    "data_type": "float32"
                }],
                "cube:dimensions": {
                    dim: {"type": dim_info["type"]} 
                    for dim, dim_info in cube_dimensions.items() 
                    if dim in var_info["dimensions"]
                },
                "roles": ["data"]
            }

        # Create STAC item
        stac_item = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "stac_extensions": [
                "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
                "https://stac-extensions.github.io/datacube/v2.2.0/schema.json",
                "https://stac-extensions.github.io/raster/v1.1.0/schema.json"
            ],
            "id": item_id,
            "collection": collection_id,
            "geometry": geometry,
            "bbox": bbox,
            "properties": {
                "start_datetime": start_datetime,
                "end_datetime": end_datetime,
                "title": "ITS_LIVE datacube of image pair velocities",
                "description": "ITS_LIVE datacube of image pair velocities",
                "proj:code": f"EPSG:{epsg_code}",
                "proj:bbox": proj_bbox,
                "proj:shape": proj_shape,
                "cube:dimensions": cube_dimensions,
                "cube:variables": cube_variables,
                "datetime": None,
                "granule_count": granule_count,
                "roi_percent_coverage": roi_percent_coverage
            },
            "links": [
                {
                    "rel": "root",
                    "href": "./collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "parent",
                    "href": "./collection.json",
                    "type": "application/json"
                },
                {
                    "rel": "self",
                    "href": f"./{item_id}/{item_id}.json",
                    "type": "application/json"
                },
                {
                    "rel": "collection",
                    "type": "application/json",
                    "href": collection_href,
                }
            ],
            "assets": assets
        }

        stac_items.append(stac_item)

    # Save as NDJSON (newline-delimited JSON)
    with open(output_filename, "w") as f:
        for item in stac_items:
            json.dump(item, f, separators=(',', ':'))
            f.write('\n')

    print(f"\n✅ Successfully generated {len(stac_items)} STAC Items for datacubes.")
    print(f"Output saved to: {os.path.abspath(output_filename)}")
    print(f"Collection ID: {collection_id}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate STAC Items for ITS_LIVE datacubes from a GeoJSON catalog."
    )
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to the input cube catalog GeoJSON file (e.g., cube_catalog.json).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="cube_stac_items.json",
        help="Name of the output STAC GeoJSON file.",
    )

    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found.")
        return

    if not os.path.exists("cubes.json"):
        print("Error: Collection file 'cubes.json' not found.")
        return

    generate_cube_stac_items(args.input_file, args.output)


if __name__ == "__main__":
    main()