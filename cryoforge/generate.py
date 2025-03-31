"""
Script to generate STAC catalog files for ITS_LIVE granule dataset using xstac.

Authors: Original by Mark Fahnestock, Masha Liukis
Modified to use stac by Luis Lopez
"""

import argparse
import collections
import io
import json
import logging
from pathlib import Path

import fsspec
import geojson
import kerchunk.hdf
import numpy as np
import pandas as pd
import pystac
import xarray as xr
from pyproj import CRS, Transformer
from shapely.geometry import Polygon

from .ingestitem import ingest_item

# Date format as it appears in granules filenames of optical format:
# LC08_L1TP_011002_20150821_20170405_01_T1_X_LC08_L1TP_011002_20150720_20170406_01_T1_G0240V01_P038.nc
DATE_FORMAT = "%Y%m%d"

# Date and time format as it appears in granules filenames in radar format:
# S1A_IW_SLC__1SSH_20170221T204710_20170221T204737_015387_0193F6_AB07_X_S1B_IW_SLC__1SSH_20170227T204628_20170227T204655_004491_007D11_6654_G0240V02_P094.nc
DATE_TIME_FORMAT = "%Y%m%dT%H%M%S"

# Number of retries for AWS S3 operations
_NUM_AWS_COPY_RETRIES = 3
_AWS_COPY_SLEEP_SECONDS = 3


def generate_nsidc_metadata_files(ds, filename, version):
    """
    Example of premet file:
    =======================
    FileName=LC08_L1GT_001111_20140217_20170425_01_T2_X_LC08_L1GT_001111_20131113_20170428_01_T2_G0240V01_P006.nc
    VersionID_local=001
    Begin_date=2013-11-13
    End_date=2017-04-28
    Begin_time=00:00:01.000
    End_time=23:59:59.
    Container=AssociatedPlatformInstrumentSensor
    AssociatedPlatformShortName=LANDSAT-8
    AssociatedInstrumentShortName=OLI
    AssociatedSensorShortName=OLI
    Container=AssociatedPlatformInstrumentSensor
    AssociatedPlatformShortName=LANDSAT-8
    AssociatedInstrumentShortName=TIRS
    AssociatedSensorShortName=TIRS

    Example of spatial file:
    ========================
    -94.32	71.86
    -99.41	71.67
    -94.69	73.3
    -100.22	73.09
    """

    # Dictionary of metadata values based on the mission+sensor token
    # Optical data:
    LC9 = "LC09"
    LO9 = "LO09"
    LC8 = "LC08"
    LO8 = "LO08"
    L7 = "LE07"
    L5 = "LT05"
    L4 = "LT04"
    S2A = "S2A"
    S2B = "S2B"

    # Radar data:
    S1A = "S1A"
    S1B = "S1B"

    PlatformSensor = collections.namedtuple("PM", ["platform", "sensor"])

    short_names = {
        LC9: PlatformSensor("LANDSAT-9", "OLI"),
        LO9: PlatformSensor("LANDSAT-9", "OLI"),
        LC8: PlatformSensor("LANDSAT-8", "OLI"),
        LO8: PlatformSensor("LANDSAT-8", "OLI"),
        L7: PlatformSensor("LANDSAT-7", "ETM+"),
        L5: PlatformSensor("LANDSAT-5", "TM"),
        L4: PlatformSensor("LANDSAT-4", "TM"),
        S1A: PlatformSensor("SENTINEL-1", "Sentinel-1A"),
        S1B: PlatformSensor("SENTINEL-1", "Sentinel-1B"),
        S2A: PlatformSensor("SENTINEL-2", "Sentinel-2A"),
        S2B: PlatformSensor("SENTINEL-2", "Sentinel-2B"),
    }

    def get_sensor_tokens_from_filename(filename: str):
        """
        Extract sensor tokens for two images from the granule
        filename. The filename is expected to have the following format:
        <image1_tokens>_X_<image2_tokens>_<granule_tokens>.nc.
        """
        #  url_files = os.path.basename(filename).split('_X_')
        url_files = Path(filename).name.split("_X_")

        # Get tokens for the first image
        url_tokens_1 = url_files[0].split("_")

        # Extract info from second part of the granule's filename: corresponds to the second image
        url_tokens_2 = url_files[1].split("_")

        # Return sensor tokens for both images
        return (url_tokens_1[0], url_tokens_2[0])

    def create_premet_file(ds: xr.Dataset, infile: str, version: str):
        """
        Create premet file that corresponds to the input image pair velocity granule.

        Inputs
        ======
        ds: xarray.Dataset object that represents the granule.
        infile: Filename of the input ITS_LIVE granule
        """
        # Extract tokens from the filename
        sensor1, sensor2 = get_sensor_tokens_from_filename(infile)

        if sensor1 not in short_names:
            raise RuntimeError(
                f"create_premet_file(): got unexpected mission+sensor "
                f"{sensor1} for image#1 of {infile}: one of "
                f"{list(short_names.keys())} is supported."
            )

        if sensor2 not in short_names:
            raise RuntimeError(
                f"create_premet_file() got unexpected mission+sensor "
                f"{sensor2} for image#2 of {infile}: one of "
                f"{list(short_names.keys())} is supported."
            )

        # Get acquisition dates for both images
        begin_date = pd.to_datetime(ds["img_pair_info"].acquisition_date_img1)
        end_date = pd.to_datetime(ds["img_pair_info"].acquisition_date_img2)

        file_content = f"""
        FileName={infile}
        VersionID_local={version}
        Begin_date={begin_date.strftime("%Y-%m-%d")}
        End_date={end_date.strftime("%Y-%m-%d")}
        Begin_time={begin_date.strftime("%H:%M:%S")}.{begin_date.microsecond // 1000:03d}
        End_time={end_date.strftime("%H:%M:%S")}.{end_date.microsecond // 1000:03d}
        """

        # Append premet with sensor info
        for sensor in [sensor1, sensor2]:
            file_content = (
                file_content
                + f"""Container=AssociatedPlatformInstrumentSensor
            AssociatedPlatformShortName={short_names[sensor].platform}
            AssociatedInstrumentShortName={short_names[sensor].sensor}
            AssociatedSensorShortName={short_names[sensor].sensor}
            """
            )
        return file_content

    return create_premet_file(ds, filename, version)


def get_geom(ds, precision, projection):
    """
    Extracts a polygon from an ITS_LIVE xarray dataset using available projection metadata.

    Returns:
        shapely.Polygon object if found, otherwise None.
    """
    # Look for known projection keys in dataset attributes
    projection_keys = ["mapping", "UTM_Projection", "Polar_Stereographic"]

    for key in projection_keys:
        if key in ds.attrs:
            projection_cf = ds.attrs[key]
            break
        elif key in ds:
            projection_cf = ds[key]
            break
    else:
        return None

    crs = CRS.from_wkt(projection_cf.crs_wkt)
    transformer = Transformer.from_crs(crs, CRS.from_epsg(projection), always_xy=True)
    xvals = ds["x"].values
    yvals = ds["y"].values
    minval_x, pix_size_x, rot_x_ignored, maxval_y, rot_y_ignored, pix_size_y = [
        float(x) for x in projection_cf.attrs["GeoTransform"].split()
    ]

    # NOTE: these are pixel center values, need to modify by half the grid size to get bounding box/geotransform values
    projection_cf_minx = xvals[0] - pix_size_x / 2.0
    projection_cf_maxx = xvals[-1] + pix_size_x / 2.0
    projection_cf_miny = yvals[-1] + pix_size_y / 2.0  # pix_size_y is negative!
    projection_cf_maxy = yvals[0] - pix_size_y / 2.0  # pix_size_y is negative!

    ll_lonlat = np.round(
        transformer.transform(projection_cf_minx, projection_cf_miny),
        decimals=precision,
    ).tolist()
    lr_lonlat = np.round(
        transformer.transform(projection_cf_maxx, projection_cf_miny),
        decimals=precision,
    ).tolist()
    ur_lonlat = np.round(
        transformer.transform(projection_cf_maxx, projection_cf_maxy),
        decimals=precision,
    ).tolist()
    ul_lonlat = np.round(
        transformer.transform(projection_cf_minx, projection_cf_maxy),
        decimals=precision,
    ).tolist()

    # find center lon lat for inclusion in feature (to determine lon lat grid cell directory)
    center_lonlat = np.round(
        transformer.transform(
            (xvals[0] + xvals[-1]) / 2.0, (yvals[0] + yvals[-1]) / 2.0
        ),
        decimals=4,
    ).tolist()

    fracs = [0.25, 0.5, 0.75]
    polylist = []  # ring in counterclockwise order

    polylist.append(ll_lonlat)
    dx = projection_cf_maxx - projection_cf_minx
    dy = projection_cf_miny - projection_cf_miny
    for frac in fracs:
        polylist.append(
            np.round(
                transformer.transform(
                    projection_cf_minx + (frac * dx), projection_cf_miny + (frac * dy)
                ),
                decimals=precision,
            ).tolist()
        )

    polylist.append(lr_lonlat)
    dx = projection_cf_maxx - projection_cf_maxx
    dy = projection_cf_maxy - projection_cf_miny
    for frac in fracs:
        polylist.append(
            np.round(
                transformer.transform(
                    projection_cf_maxx + (frac * dx), projection_cf_miny + (frac * dy)
                ),
                decimals=precision,
            ).tolist()
        )

    polylist.append(ur_lonlat)
    dx = projection_cf_minx - projection_cf_maxx
    dy = projection_cf_maxy - projection_cf_maxy
    for frac in fracs:
        polylist.append(
            np.round(
                transformer.transform(
                    projection_cf_maxx + (frac * dx), projection_cf_maxy + (frac * dy)
                ),
                decimals=precision,
            ).tolist()
        )

    polylist.append(ul_lonlat)
    dx = projection_cf_minx - projection_cf_minx
    dy = projection_cf_miny - projection_cf_maxy
    for frac in fracs:
        polylist.append(
            np.round(
                transformer.transform(
                    projection_cf_minx + (frac * dx), projection_cf_maxy + (frac * dy)
                ),
                decimals=precision,
            ).tolist()
        )

    polylist.append(ll_lonlat)
    poly = Polygon(polylist)
    spatial_epsg = projection_cf.attrs["spatial_epsg"]
    return {
        "polygon": poly,
        "bbox": list(poly.boundary.bounds),
        "center": center_lonlat,
        "epsg": spatial_epsg,
        "corners": [ul_lonlat, ur_lonlat, lr_lonlat, ll_lonlat],
    }


def open_netcdf(url):
    # Choose storage options based on protocol
    so = {}
    if url.startswith("s3://"):
        so = {"anon": True, "skip_instance_cache": True}  # Disable caching for S3
    elif url.startswith("http"):
        so = {"cache_type": "none"}  # Disable caching for HTTP
    else:
        so = {}

    # Open the file with fsspec
    with fsspec.open(url, mode="rb", **so) as f:  # type: ignore
        file_content = io.BytesIO(f.read())  # type: ignore
        h5chunks = kerchunk.hdf.SingleHdf5ToZarr(
            file_content, url=url, inline_threshold=100
        )

    # Open dataset from memory
    ds = xr.open_dataset(file_content, engine="h5netcdf")
    return ds, h5chunks.translate()


def s3_to_https_link(s3_path):
    """
    Convert an S3 URL to an HTTPS link for public access.
    
    Args:
        s3_path (str): S3 URL in the format 's3://bucket/key'
    
    Returns:
        str: HTTPS link to the S3 object
    """
    # Remove 's3://' prefix
    path_without_prefix = s3_path.replace('s3://', '')
    
    # Split into bucket and key
    bucket, key = path_without_prefix.split('/', 1)
    
    # Construct HTTPS link
    return f"https://{bucket}.s3.amazonaws.com/{key}"


def create_stac_item(ds, geom, url):
    """Create STAC item from dataset and geometry."""
    # Extract basic properties
    start_date = pd.to_datetime(ds["img_pair_info"].acquisition_date_img1).tz_localize(
        "UTC"
    ).isoformat().replace("+00:00", "Z")
    end_date = pd.to_datetime(ds["img_pair_info"].acquisition_date_img2).tz_localize(
        "UTC"
    ).isoformat().replace("+00:00", "Z")
    mid_date = pd.to_datetime(ds["img_pair_info"].date_center).tz_localize("UTC")

    filename = url.split("/")[-1].replace(".nc", "")
    mission = ds["img_pair_info"].id_img1.split("_")[0]
    sat_orbit_direction = ds["img_pair_info"].attrs.get("flight_direction_img1", "N/A")
    scene_1_id = ds["img_pair_info"].id_img1
    scene_2_id = ds["img_pair_info"].id_img2
    version = url.split("/")[-3].replace("v", "")
    path_scene_1 = ds["img_pair_info"].attrs.get("path_img1", "N/A")
    row_scene_1 = ds["img_pair_info"].attrs.get("row_img1", "N/A")
    path_scene_2 = ds["img_pair_info"].attrs.get("path_img2", "N/A")
    row_scene_2 = ds["img_pair_info"].attrs.get("row_img2", "N/A")
    scene_1_path_row = f"{path_scene_1}{row_scene_1}" if path_scene_1 != "N/A" else "N/A"
    scene_2_path_row = f"{path_scene_2}{row_scene_2}" if path_scene_2 != "N/A" else "N/A"

    # Create STAC item
    item = pystac.Item(
        id=filename,
        collection="itslive-granules",  # Add collection field
        stac_extensions=[
            "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
            "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json",
            "https://stac-extensions.github.io/version/v1.2.0/schema.json",
            "https://stac-extensions.github.io/sat/v1.1.0/schema.json"
        ],
        geometry={
            "type": "Polygon",
            "coordinates": geojson.Feature(geometry=geom["polygon"])["geometry"][
                "coordinates"
            ],
        },
        bbox=geom["bbox"],
        datetime=mid_date,
        properties={
            "mid_datetime": pd.to_datetime(mid_date).isoformat().replace("+00:00", "Z"),
            "created": pd.Timestamp.now(tz="UTC").isoformat().replace("+00:00", "Z"),
            "latitude": round(geom["center"][1], 4),
            "longitude": round(geom["center"][0], 4),
            "dt_days": round(float(ds["img_pair_info"].date_dt), 0),
            "platform": mission,
            "scene_1_id": scene_1_id,
            "scene_2_id": scene_2_id,
            "scene_1_path_row": scene_1_path_row,
            "scene_2_path_row": scene_2_path_row,
            "sat:orbit_state": sat_orbit_direction,
            "start_datetime": str(start_date),
            "end_datetime": str(end_date),
            "percent_valid_pixels": int(
                round(float(ds["img_pair_info"].roi_valid_percentage), 0)
            ),
            "proj:code": f"EPSG:{geom['epsg']}",
            "version": str(version),
        },
    )

    # Add assets
    for key, ext, media_type in [
        ("data", ".nc", pystac.MediaType.NETCDF),
        ("virtualzarr", ".ref.json", pystac.MediaType.JSON),
        ("overview", ".png", pystac.MediaType.PNG),
        ("thumbnail", "_thumb.png", pystac.MediaType.PNG),
    ]:
        canonical_url = url.replace(".nc", ext)
        if media_type == pystac.MediaType.NETCDF or media_type == pystac.MediaType.JSON:
            s3_url = canonical_url.replace(".s3.amazonaws.com", "").replace("https", "s3")
            extra_fields = {
                    "alternate": {
                        "s3": {
                            "href": s3_url,
                            "alternate:name": "S3",
                        }
                    }
            }
            role = ["data"]
        else:
            extra_fields = {}
            role = ["overview" if key == "overview" else "thumbnail"]

        item.add_asset(
            key=key,
            asset=pystac.Asset(
                href=s3_to_https_link(canonical_url),
                media_type=media_type,
                roles=role,
                extra_fields=extra_fields,
            ),
        )

    return item


def generate_itslive_metadata(url):
    original_ds, kerchunks = open_netcdf(url)
    if original_ds is None:
        raise ValueError(f"Could not open {url}")

    original_ds.coords["time"] = pd.to_datetime(
        original_ds["img_pair_info"].date_center
    )
    ds = original_ds.expand_dims(dim="time", axis=0)
    geom = get_geom(ds, precision=4, projection=4326)
    if geom is None:
        raise ValueError(f"Could not extract geometry from {url}")
    geom["url"] = url
    item = create_stac_item(ds, geom, url)
    # item.validate() # <- will break because the schema is wrong for the collection property.
    nsidc_meta = generate_nsidc_metadata_files(ds, item.id, item.properties["version"])
    nsidc_spatial = "\n".join([
        f"{round(coord[0], 2)}\t{round(coord[1], 2)}" for coord in geom["corners"]
    ])
    return {
        "ds": ds,
        "url": url,
        "stac": item,
        "kerchunk": kerchunks,
        "nsidc_meta": nsidc_meta.strip().replace(" ", "") + "\n",
        "nsidc_spatial": nsidc_spatial + "\n",
    }


def save_metadata(metadata: dict, outdir: str = "."):
    """Save STAC item to filesystem or S3"""
    fs = fsspec.filesystem(outdir.split("://")[0] if "://" in outdir else "file")
    if outdir.startswith("s3"):
        stac_s3_url = (
            metadata["stac"]
            .assets.get("data")
            .extra_fields.get("alternate", [])["s3"]["href"]
        )
        bucket_path = "/".join(stac_s3_url.split("/")[0:-1])
        granule_path = f"{bucket_path}/{metadata['stac'].id}"
        logging.info(f"Saving metadata to {bucket_path}/")
        with fs.open(f"{granule_path}.stac.json", "w") as f:
            json.dump(metadata["stac"].to_dict(), f, indent=2)

        with fs.open(f"{granule_path}.nc.premet", "w") as f:
            f.write(metadata["nsidc_meta"])
        with fs.open(f"{granule_path}.nc.spatial", "w") as f:
            f.write(metadata["nsidc_spatial"])

        with fs.open(f"{granule_path}.ref.json", "w") as f:
            json.dump(metadata["kerchunk"], f, indent=2)

    else:
        granule_path = Path(outdir)

        with fs.open(granule_path / Path(f"{metadata['stac'].id}.stac.json"), "w") as f:
            json.dump(metadata["stac"].to_dict(), f, indent=2)

        with fs.open(granule_path / Path(f"{metadata['stac'].id}.nc.premet"), "w") as f:
            f.write(metadata["nsidc_meta"])

        with fs.open(
            granule_path / Path(f"{metadata['stac'].id}.nc.spatial"), "w"
        ) as f:
            f.write(metadata["nsidc_spatial"])

        with fs.open(granule_path / Path(f"{metadata['stac'].id}.ref.json"), "w") as f:
            json.dump(metadata["kerchunk"], f, indent=2)

    logging.info(f"Saving metadata to {granule_path}")

    # save stac item


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate metadata sidecar files for ITS_LIVE granules"
    )
    parser.add_argument(
        "-g", "--granule", required=True, help="Path to a single ITS_LIVE NetCDF file"
    )
    parser.add_argument("-o", "--outdir", required=True, help="Output directory")

    parser.add_argument(
        "-i",
        "--ingest",
        action="store_true",
        help="Path to the input file (optional)",
        default=None,  # Default value if not provided
    )
    parser.add_argument("-t", "--target", help="STAC endpoint")
    parser.add_argument(
        "-r",
        "--reload-collection",
        action="store_true",
        help="If present will reload/update the collection",
    )

    args = parser.parse_args()
    return args


def main():
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        level=logging.INFO,
    )
    args = parse_args()

    logging.info(f"Processing {args.granule}")
    metadata = generate_itslive_metadata(args.granule)
    save_metadata(metadata, args.outdir)

    logging.info(f"Done processing {args.granule}")

    if args.ingest:
        stac_item = Path(args.outdir) / Path(metadata["stac"].id).name.replace(
            ".nc", ".stac.json"
        )
        ingest_item(args.reload_collection, args.target, str(stac_item))
        logging.info(f"Ingested {metadata['stac'].id}")


if __name__ == "__main__":
    main()
