#!/usr/bin/env python
import json
import argparse
from datetime import datetime
import os


def clean_version(version_string: str) -> str:
    """Converts V02.1 or V02 to a standard numeric format like 2.1 or 2.0."""
    if version_string and version_string.startswith("V"):
        # V02.1 -> 2.1
        return version_string.replace("V", "").lstrip("0")
    return version_string


def generate_stac_items_cli(input_filename: str, output_filename: str):
    """
    Reads a GeoJSON FeatureCollection, transforms each feature into a complete
    STAC Item, applying the final, specific URL and property versioning rules.
    """

    # --- Configuration ---
    # COG variables for annual mosaics
    ANNUAL_COG_VARIABLES = {
        "v": {
            "title": "Velocity Magnitude (v) COG",
            "roles": ["data", "cog", "velocity-magnitude"],
        },
        "vx": {
            "title": "Velocity X-Component (vx) COG",
            "roles": ["data", "cog", "velocity-x"],
        },
        "vy": {
            "title": "Velocity Y-Component (vy) COG",
            "roles": ["data", "cog", "velocity-y"],
        },
        "v_error": {
            "title": "Velocity Magnitude Error (v_error) COG",
            "roles": ["data", "cog", "error-magnitude"],
        },
        "vx_error": {
            "title": "Velocity X-Error (vx_error) COG",
            "roles": ["data", "cog", "error-x"],
        },
        "vy_error": {
            "title": "Velocity Y-Error (vy_error) COG",
            "roles": ["data", "cog", "error-y"],
        },
        "count": {
            "title": "Image Pair Count (count) COG",
            "roles": ["data", "cog", "quality-indicator"],
        },
    }

    # COG variables for static mosaics (expanded list)
    STATIC_COG_VARIABLES = {
        "v": {
            "title": "Velocity Magnitude (v) COG",
            "roles": ["data", "cog", "velocity-magnitude"],
        },
        "v_amp": {
            "title": "Velocity Amplitude (v_amp) COG",
            "roles": ["data", "cog", "velocity-amplitude"],
        },
        "v_amp_error": {
            "title": "Velocity Amplitude Error (v_amp_error) COG",
            "roles": ["data", "cog", "error-amplitude"],
        },
        "v_error": {
            "title": "Velocity Magnitude Error (v_error) COG",
            "roles": ["data", "cog", "error-magnitude"],
        },
        "v_phase": {
            "title": "Velocity Phase (v_phase) COG",
            "roles": ["data", "cog", "velocity-phase"],
        },
        "vx": {
            "title": "Velocity X-Component (vx) COG",
            "roles": ["data", "cog", "velocity-x"],
        },
        "vx_amp": {
            "title": "Velocity X Amplitude (vx_amp) COG",
            "roles": ["data", "cog", "velocity-x-amplitude"],
        },
        "vx_amp_error": {
            "title": "Velocity X Amplitude Error (vx_amp_error) COG",
            "roles": ["data", "cog", "error-x-amplitude"],
        },
        "vx_error": {
            "title": "Velocity X-Error (vx_error) COG",
            "roles": ["data", "cog", "error-x"],
        },
        "vx_phase": {
            "title": "Velocity X Phase (vx_phase) COG",
            "roles": ["data", "cog", "velocity-x-phase"],
        },
        "vy": {
            "title": "Velocity Y-Component (vy) COG",
            "roles": ["data", "cog", "velocity-y"],
        },
        "vy_amp": {
            "title": "Velocity Y Amplitude (vy_amp) COG",
            "roles": ["data", "cog", "velocity-y-amplitude"],
        },
        "vy_amp_error": {
            "title": "Velocity Y Amplitude Error (vy_amp_error) COG",
            "roles": ["data", "cog", "error-y-amplitude"],
        },
        "vy_error": {
            "title": "Velocity Y-Error (vy_error) COG",
            "roles": ["data", "cog", "error-y"],
        },
        "vy_phase": {
            "title": "Velocity Y Phase (vy_phase) COG",
            "roles": ["data", "cog", "velocity-y-phase"],
        },
        "count": {
            "title": "Image Pair Count (count) COG",
            "roles": ["data", "cog", "quality-indicator"],
        },
        "dv_dt": {
            "title": "Velocity Change Rate (dv_dt) COG",
            "roles": ["data", "cog", "velocity-change-rate"],
        },
        "dvx_dt": {
            "title": "Velocity X Change Rate (dvx_dt) COG",
            "roles": ["data", "cog", "velocity-x-change-rate"],
        },
        "dvy_dt": {
            "title": "Velocity Y Change Rate (dvy_dt) COG",
            "roles": ["data", "cog", "velocity-y-change-rate"],
        },
        "floatingice": {
            "title": "Floating Ice Mask (floatingice) COG",
            "roles": ["data", "cog", "mask-floating-ice"],
        },
        "landice": {
            "title": "Land Ice Mask (landice) COG",
            "roles": ["data", "cog", "mask-land-ice"],
        },
        "outlier_percent": {
            "title": "Outlier Percentage (outlier_percent) COG",
            "roles": ["data", "cog", "quality-outlier-percent"],
        },
    }

    COG_COMMON_METADATA = {
        "type": "image/tiff; application=geotiff; profile=cloud-optimized"
    }

    # Endpoints
    S3_ENDPOINT_HTTP_REGION = "https://its-live-data.s3.amazonaws.com/"
    S3_ENDPOINT_HTTP_GLOBAL = "https://its-live-data.s3.amazonaws.com/"
    S3_BUCKET_NAME = "s3://its-live-data/"

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

    stac_items = []

    # --- Version Extraction ---
    if (
        data.get("features")
        and data["features"][0].get("properties")
        and data["features"][0]["properties"].get("url")
    ):
        version_netcdf_full_raw = (
            data["features"][0]["properties"]["url"].split("_")[-1].replace(".nc", "")
        )
        version_netcdf_item_prop = clean_version(version_netcdf_full_raw)
        version_path_full = f"v{version_netcdf_item_prop}"
        version_path_browse = f"v{version_netcdf_item_prop.split('.')[0]}"
        version_browse_filename = "v02"
    else:
        version_netcdf_full_raw = "V02.1"
        version_netcdf_item_prop = "2.1"
        version_path_full = "v2.1"
        version_path_browse = "v2"
        version_browse_filename = "v02"

    collection_id = "velocity-mosaics"
    collection_href = f"https://stac.itslive.cloud/collections/{collection_id}"

    for feature in data.get("features", []):
        props = feature["properties"]
        rgi_id = props["rgi"]
        epsg_code = props["epsg"]
        netcdf_s3_url = props["url"]

        # --- URL Construction Helpers ---
        netcdf_path_suffix = netcdf_s3_url.replace(S3_BUCKET_NAME, "")
        netcdf_http_url = S3_ENDPOINT_HTTP_REGION + netcdf_path_suffix

        url_parts = netcdf_s3_url.split("/")

        url_path_segment = url_parts[-2]

        # FIX: Extract period_id correctly from the filename
        filename = netcdf_s3_url.split("/")[-1]
        filename_parts = filename.replace(".nc", "").split("_")
        period_id = filename_parts[5]  # This gets the year or "0000"

        # --- ID Generation Fix (Final Logic) ---
        if url_path_segment == "static":
            # Static ID should be [RGI ID]_STATIC (e.g., RGI01A_STATIC)
            item_id = f"{rgi_id}_STATIC"
            temporal_type = "static"
        else:
            # Annual ID uses the period ID (e.g., RGI01A_ANNUAL_1982)
            item_id = f"{rgi_id}_ANNUAL_{period_id}"
            temporal_type = "annual"

        # --- Path/Filename Construction ---
        cog_path_suffix_base = (
            f"velocity_mosaic/{version_path_full}/{url_path_segment}/"
        )
        cog_http_base = S3_ENDPOINT_HTTP_REGION + cog_path_suffix_base + "cog/"
        cog_s3_base = S3_BUCKET_NAME + cog_path_suffix_base + "cog/"

        cog_base_filename_netcdf = (
            f"ITS_LIVE_velocity_120m_{rgi_id}_{period_id}_{version_netcdf_full_raw}"
        )

        browse_path_suffix_base = (
            f"velocity_mosaic/{version_path_browse}/{url_path_segment}/"
        )
        browse_http_base = S3_ENDPOINT_HTTP_GLOBAL + browse_path_suffix_base + "browse/"
        browse_s3_base = S3_BUCKET_NAME + browse_path_suffix_base + "browse/"

        # FIX: Use period_id in browse filename
        browse_filename_base = (
            f"ITS_LIVE_velocity_120m_{rgi_id}_{period_id}_{version_browse_filename}"
        )

        try:
            start_dt = datetime.strptime(props["startTime"], "%d-%b-%Y")
            end_dt = datetime.strptime(props["endTime"], "%d-%b-%Y")

            # For static mosaics, use end date as datetime; for annual, use start date
            if temporal_type == "static":
                datetime_str = end_dt.isoformat() + "+00:00"
            else:
                datetime_str = start_dt.isoformat() + "+00:00"
            
            start_datetime_str = start_dt.isoformat() + "+00:00"
            end_datetime_str = end_dt.isoformat() + "T23:59:59+00:00"
        except ValueError:
            datetime_str = None
            start_datetime_str = None
            end_datetime_str = None

        # --- Assets Dictionary ---
        assets = {}

        # a. NetCDF Asset (Primary Data)
        assets["data_netcdf"] = {
            "title": "Full Velocity Mosaic Data (NetCDF)",
            "href": netcdf_http_url,
            "type": "application/netcdf",
            "roles": ["data", "composite"],
            "alternate": {"s3": {"href": netcdf_s3_url}},
        }

        # b. COG Assets (different sets for static vs annual)
        cog_variables = STATIC_COG_VARIABLES if temporal_type == "static" else ANNUAL_COG_VARIABLES
        for var, metadata in cog_variables.items():
            cog_filename = f"{cog_base_filename_netcdf}_{var}.tif"
            asset_key = f"cog_{var}"

            assets[asset_key] = {
                **metadata,
                **COG_COMMON_METADATA,
                "href": cog_http_base + cog_filename,
                "alternate": {"s3": {"href": cog_s3_base + cog_filename}},
            }

        # c. Browse Image Asset
        browse_filename = f"{browse_filename_base}_v_browse.png"
        browse_http_url = browse_http_base + browse_filename
        browse_s3_url = browse_s3_base + browse_filename

        assets["browse_png"] = {
            "title": "Browse Image (Velocity Magnitude)",
            "href": browse_http_url,
            "type": "image/png",
            "roles": ["overview", "browse"],
            "alternate": {"s3": {"href": browse_s3_url}},
        }

        # --- Final STAC Item Construction ---
        stac_item = {
            "id": item_id,
            "type": "Feature",
            "stac_version": "1.1.0",
            "collection": collection_id,
            "bbox": feature["bbox"],
            "geometry": feature.get("geometry", {
                "type": "Polygon",
                "coordinates": [[
                    [feature["bbox"][0], feature["bbox"][1]],
                    [feature["bbox"][2], feature["bbox"][1]],
                    [feature["bbox"][2], feature["bbox"][3]],
                    [feature["bbox"][0], feature["bbox"][3]],
                    [feature["bbox"][0], feature["bbox"][1]]
                ]]
            }),
            "properties": {
                "datetime": datetime_str,
                "start_datetime": start_datetime_str,
                "end_datetime": end_datetime_str,
                "rgi": rgi_id,
                "epsg": epsg_code,
                "temporal_coverage_type": temporal_type,
                "version": version_netcdf_item_prop,
            },
            "assets": assets,
            "links": [
                {
                    "rel": "collection",
                    "type": "application/json",
                    "href": collection_href,
                }
            ],
            "stac_extensions": [
                "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
                "https://stac-extensions.github.io/version/v1.2.0/schema.json",
            ],
        }
        stac_items.append(stac_item)

    # Save as NDJSON (newline-delimited JSON)
    with open(output_filename, "w") as f:
        for item in stac_items:
            json.dump(item, f, separators=(',', ':'))
            f.write('\n')

    print(f"\n✅ Successfully generated {len(stac_items)} STAC Items.")
    print(f"Output saved to: {os.path.abspath(output_filename)}")
    print(
        f"Collection ID: {collection_id} (Version property: {version_netcdf_item_prop})"
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate STAC Items for ITS_LIVE velocity mosaics from a GeoJSON catalog."
    )
    parser.add_argument(
        "input_file",
        type=str,
        help="Path to the input GeoJSON catalog file (e.g., mosaics_catalog_v2.1.json).",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="stac_velocity_mosaics_items.json",
        help="Name of the output STAC GeoJSON file.",
    )

    args = parser.parse_args()

    if not os.path.exists(args.input_file):
        print(f"Error: Input file '{args.input_file}' not found.")
        return

    generate_stac_items_cli(args.input_file, args.output)


if __name__ == "__main__":
    main()
