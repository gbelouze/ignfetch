"""
Downloads BD Ortho and BD Haie
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import tempfile
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import geopandas as gpd
import multivolumefile
import numpy as np
import pandas as pd
import py7zr
import rasterio as rio
import requests
from cyclopts import App
from joblib import Parallel, delayed
from pyproj import Transformer
from requests.exceptions import HTTPError
from retry import retry
from rich.progress import Progress, TaskID
from shapely.geometry import MultiPolygon, Polygon

from ignfetch.assets import years_bdortho_irc, years_bdortho_rvb
from ignfetch.const import CODE_INSEE
from ignfetch.utils import bisect, default_bar

app = App(help="Download BD-Haie, BD-Ortho and LidarHD from IGN for hedge monitoring.")


log = logging.getLogger(__name__)


def download_file(
    url: str,
    output_path: Path,
    progress_task: tuple[Progress, TaskID] | None = None,
) -> None:
    """
    Download a file from a URL with streaming and progress logging.
    The file is first written to a temporary file and moved to output_path
    only if the download completes successfully.

    Parameters
    ----------
    url : str
        Remote file URL.
    output_path : Path
        Destination path for the downloaded file.
    progress_task : tuple[Progress, TaskID] | None
        Optional progress and task id to update at the end of the download. Uses _progress_lock
        for thread safety.
    """
    log.info(f"Downloading {url} to {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Create temp file in same directory to allow atomic move
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        temp_path = Path(tmp_file.name)
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        tmp_file.write(chunk)
            tmp_file.flush()
            tmp_file.close()
            shutil.move(str(temp_path), str(output_path))
            log.info(f"Saved to {output_path}")
        except HTTPError as e:
            log.error(f"Downloading failed for {url} : {e}.")
        finally:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)

    if progress_task is not None:
        progress, task = progress_task
        progress.advance(task)


def extract_7z(archive_path: Path, output_dir: Path) -> None:
    """Extract a .7z (possibly multi-part) archive.

    Parameters
    ----------
    archive_path : Path
        Path to .7z. If there are multiple part (e.g., 'file.7z.001', 'file.7z.002', etc),
        only the base name should be given (e.g. 'file.7z').
    output_dir : Path
        Directory where contents are extracted.
    """
    log.info(f"Extracting {archive_path}")
    output_dir.mkdir(parents=True, exist_ok=True)
    with (
        multivolumefile.open(archive_path, mode="rb") as archive,
        py7zr.SevenZipFile(archive, mode="r") as z,  # ty:ignore[invalid-argument-type]
    ):
        z.extractall(path=output_dir)
    log.info(f"Extracted to {output_dir}")


@retry(exceptions=requests.RequestException, tries=5, backoff=1.5)
def url_exists(url: str, timeout: float = 5.0) -> bool:
    """
    Check if a remote file exists using a partial GET request.

    Parameters
    ----------
    url : str
        Target URL.
    timeout : float
        Request timeout in seconds. Defaults to 5.0.

    Returns
    -------
    bool
        True if resource exists, False otherwise.
    """
    headers = {"Range": "bytes=0-0"}

    with requests.get(url, headers=headers, timeout=timeout, stream=True) as r:
        if r.status_code in (200, 206):
            return True
        if r.status_code in (403, 404):
            return False
        if 500 <= r.status_code < 600 or r.status_code == 429:
            raise requests.RequestException(f"Server error {r.status_code}")
        raise ValueError(f"Don't know how to handle status code {r.status_code}.")


@app.command()
def bdortho(
    output: Path,
    year: int = 2024,
    department: str = "017",
    in_parallel: bool = True,
    irc: bool = False,
) -> None:
    """Download BD-Ortho data

    Parameters
    ----------
    output : Path
        Path to the download directory.
    year : int
        Target year to download, in the 2010-. range. Not all years
        are available for all department, see the [reference file](
        https://geoservices.ign.fr/sites/default/files/2026-03/Tableaux_Ann%C3%A9es_PVA_BDORTHO_20260312.zip
        )
    department : str
        Insee code of the department, e.g. 75 for Paris or 2A for North Corse-du-Sud.
    in_parallel : bool
        Whether to download in parallel. This usually should be True, except for debugging.
        Defaults to True.
    irc : bool
        If True, download IRC data. Otherwise download RGB. Defaults to True.

    """
    department = department.zfill(3)
    if department not in CODE_INSEE:
        raise ValueError(
            f"Unknown department code {department}. "
            "Check ignfetch.const.CODE_INSEE for allowed values."
        )

    color = "IRC" if irc else "RVB"
    versions_to_try = (2, 1) if year >= 2023 else (1, 2)  # simple heuristics

    for version in versions_to_try:
        BDORTHO_BASE_URL = (
            f"https://data.geopf.fr/telechargement/download/BDORTHO/BDORTHO_{version}-0_{color}-"
            f"0M20_JP2-E080_LAMB93_D{department}_{year}-01-01/BDORTHO_{version}-0_{color}-"
            f"0M20_JP2-E080_LAMB93_D{department}_{year}-01-01.7z"
        )
        base_urls = [f"{BDORTHO_BASE_URL}.{str(i).zfill(3)}" for i in range(1, 11)]

        if url_exists(base_urls[0]):
            bad_url_idx = bisect(base_urls, url_exists)
            log.info(f"Found {bad_url_idx} .7z parts on the server.")
            base_urls = base_urls[:bad_url_idx]
            break

        log.debug(f"No data for version {version}.")

    else:
        msg = f"No BD-Ortho data found for {year=}, {department=}, {irc=}"
        log.error(msg)
        if not irc:
            times_gdf = years_bdortho_irc() if irc else years_bdortho_rvb()
            row = times_gdf.loc[times_gdf.code_insee == department]
            available_years = row.columns[row.eq(1).iloc[0]].tolist()
            log.info(
                f"Available years for {'IRC' if irc else 'RVB'} {department=} are "
                f"{available_years}."
            )

        raise ValueError(msg)

    output.mkdir(parents=True, exist_ok=True)

    log.info(f"Downloading each of {base_urls}")

    def _download_file(url):
        filename = Path(url).name
        dest = output / filename
        if dest.exists():
            return
        download_file(url, dest)

    Parallel(n_jobs=-1 if in_parallel else 1, prefer="threads")(
        delayed(_download_file)(url) for url in base_urls
    )

    extract_7z(output / Path(BDORTHO_BASE_URL).name, output)


@app.command()
def departement(output: Path) -> None:
    """Download département boundaries shapefile or GeoJSON.

    Parameters
    ----------
    output : Path
        Directory where the dataset will be stored.
    """
    url = "http://osm13.openstreetmap.fr/~cquest/openfla/export/departements-20190101-shp.zip"
    name = Path(url).name
    zip_path = output / name

    download_file(url, zip_path)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(output)

    log.info(f"Département data extracted to {output}")
    log.info(f"Département data written to {output}")


@app.command()
def bdhaie(output: Path) -> None:
    """Download BD Haie dataset.

    Parameters
    ----------
    output : Path
        Path for the downloaded BD Haie file as a `.parquet`.
    """
    url = "https://data.geopf.fr/telechargement/download/BD-HAIE/HAIE_2-0__GPKG_LAMB93_FXX_2024-03-15/HAIE_2-0__GPKG_LAMB93_FXX_2024-03-15.7z"
    output = output.with_suffix(".parquet")
    download_output = output.with_suffix(".7z")
    download_file(url, download_output)
    extract_7z(download_output, download_output.parent)
    log.info(f"BD Haie data written to {download_output.parent}")

    log.debug("Extracting as parquet")
    gdf = gpd.read_file(
        download_output.parent
        / "HAIE"
        / "1_DONNEES_LIVRAISON_2024-06-00079"
        / "HAIE_2-0__GPKG_LAMB93_FXX_2024-03-15"
        / "haie.gpkg"
    )
    df = pd.DataFrame(gdf)
    df.to_parquet(output, index=False)

    log.info(f"Parquet written: {output}")
    log.debug("Removing downloaded assets...")
    download_output.unlink()
    shutil.rmtree(download_output.parent / "MASQUE-FORET")


# unsupported yet
def _ocs_ge(output: Path, departement: int = 17, year: int = 2021) -> None:
    url = f"https://data.geopf.fr/telechargement/download/OCSGE/OCS-GE_2-0__GPKG_LAMB93_D0{departement}_{year}-01-01/OCS-GE_2-0__GPKG_LAMB93_D0{departement}_{year}-01-01.7z"
    output = output.with_suffix(".parquet")
    download_output = output.with_suffix(".7z")
    download_file(url, download_output)
    extract_7z(download_output, download_output.parent)
    log.info(f"OCS GE data written to {download_output.parent}")

    log.debug("Extracting as parquet")
    gdf = gpd.read_file(next((download_output.parent / "OCS-GE").rglob("ZONE_CONSTRUITE.gpkg")))
    df = pd.DataFrame(gdf)
    df.to_parquet(output, index=False)

    log.info(f"Parquet written: {output}")
    log.debug("Removing downloaded assets...")
    download_output.unlink()
    shutil.rmtree(download_output.parent / "OCS-GE")


@app.command()
def foretv1(output: Path):
    """Download Foret V1 dataset.

    Parameters
    ----------
    output : Path
        Path for the downloaded Foret V1 file as a `.parquet`.
    """
    url = "https://data.geopf.fr/telechargement/download/IGNF_MASQUE-FORET/MASQUE-FORET_1-0_2021-2023_GPKG_LAMB93_FXX_2025-09-25/MASQUE-FORET_1-0_2021-2023_GPKG_LAMB93_FXX_2025-09-25.7z"
    output = output.with_suffix(".parquet")
    download_output = output.with_suffix(".7z")
    download_file(url, download_output)
    extract_7z(download_output, download_output.parent)
    log.info(f"Foret V1 data written to {download_output.parent}")

    log.debug("Extracting as parquet")
    gdf = gpd.read_file(
        download_output.parent
        / "MASQUE-FORET"
        / "MASQUE-FORET_1-0_2021-2023_GPKG_LAMB93_FXX_2025-09-25"
        / "masque-foret1.gpkg"
    )
    df = pd.DataFrame(gdf)
    df.to_parquet(output, index=False)

    log.info(f"Parquet written: {output}")
    log.debug("Removing downloaded assets...")
    download_output.unlink()
    shutil.rmtree(download_output.parent / "MASQUE-FORET")


def lidar_tiles_from_polygon(aoi: Polygon | MultiPolygon) -> list[tuple[int, int]]:
    """
    Compute 1 km × 1 km tile indices intersecting a given Polygon or MultiPolygon.

    Parameters
    ----------
    aoi : Polygon | MultiPolygon
        Polygon(s) in WGS84 coordinates (EPSG:4326).

    Returns
    -------
    list[tuple[int, int]]
        List of (x, y) lower-left tile coordinates in EPSG:2154.
    """
    transformer = Transformer.from_crs(4326, 2154, always_xy=True)

    # Ensure we have a single geometry for iteration
    polys = [aoi] if isinstance(aoi, Polygon) else list(aoi.geoms)

    tiles = []

    for poly in polys:
        x, y = poly.exterior.coords.xy
        x2154, y2154 = transformer.transform(x, y)
        poly_2154 = Polygon(zip(x2154, y2154, strict=True))

        minx, miny, maxx, maxy = poly_2154.bounds
        minx = int(minx // 1000 * 1000)
        miny = int(miny // 1000 * 1000)
        maxx = int(maxx // 1000 * 1000)
        maxy = int(maxy // 1000 * 1000)

        for x0 in range(minx, maxx + 1000, 1000):
            for y0 in range(miny, maxy + 1000, 1000):
                tile_poly = Polygon(
                    [
                        (x0, y0),
                        (x0 + 1000, y0),
                        (x0 + 1000, y0 + 1000),
                        (x0, y0 + 1000),
                    ]
                )
                if poly_2154.intersects(tile_poly):
                    tiles.append((x0, y0))

    # Remove duplicates if multiple polygons cover the same tile
    return list(sorted(set(tiles)))


def lidar_tile_urls(tiles: list[tuple[int, int]]) -> list[str]:
    """
    Build WMS-R download links for each tile.

    Parameters
    ----------
    tiles : list[tuple[int, int]]
        List of (x, y) lower-left tile coordinates in EPSG:2154.

    Returns
    -------
    list[str]
        Download URLs for each tile.
    """
    base = (
        "https://data.geopf.fr/wms-r?"
        "SERVICE=WMS&VERSION=1.3.0&EXCEPTIONS=text/xml&REQUEST=GetMap&"
        "LAYERS=IGNF_LIDAR-HD_MNH_ELEVATION.ELEVATIONGRIDCOVERAGE.LAMB93&"
        "FORMAT=image/geotiff&STYLES=&CRS=EPSG:2154&"
    )

    urls = []
    for x0, y0 in tiles:
        bbox = f"{x0 - 0.25},{y0 - 0.25},{x0 + 999.75},{y0 + 999.75}"
        filename = f"LHD_FXX_{x0 // 1000:04d}_{(y0 + 1000) // 1000:04d}_MNH_O_0M50_LAMB93_IGN69.tif"
        url = f"{base}BBOX={bbox}&WIDTH=2000&HEIGHT=2000&FILENAME={filename}"
        urls.append(url)
    return urls


def compress_lidar_tif(tif_path: Path) -> Path:
    """
    Compress a GeoTIFF into a JPEG2000 (.jp2) file using Rasterio (JP2OpenJPEG driver).
    The file is first written to a temporary file and moved to the final location
    only if the compression succeeds.

    Parameters
    ----------
    tif_path : Path
        Path to the input GeoTIFF file to compress.

    Returns
    -------
    Path
        Path to the compressed JPEG2000 file (same name, .jp2 extension).

    Notes
    -----
    - Thread safety: JP2OpenJPEG compression is **not guaranteed to be thread-safe**.
      For parallel processing, use multiple processes rather than threads.
    """

    jp2_path = tif_path.with_suffix(".jp2")
    if jp2_path.exists():
        log.info(f"{jp2_path} already exists. Skipping")
        return jp2_path

    jp2_path.parent.mkdir(parents=True, exist_ok=True)

    with rio.open(tif_path) as src:
        arr = src.read(1, out_dtype="float32")
        profile = src.profile

        # Clip and scale
        arr = np.clip(arr, 0, 40)
        arr = (arr / 40 * 255).astype("uint8")

        profile.update(
            driver="JP2OpenJPEG",
            dtype="uint8",
            count=1,
            compress="JPEG2000",
            quality=40,
            tiled=True,
            blockxsize=256,
            blockysize=256,
        )

        # Write to temporary JP2
        with tempfile.NamedTemporaryFile(suffix=".jp2", delete=False) as tmp_jp2:
            tmp_jp2_path = Path(tmp_jp2.name)
        with rio.open(tmp_jp2_path, "w", **profile) as dst:
            dst.write(arr, 1)

    # Move to final destination
    shutil.move(str(tmp_jp2_path), str(jp2_path))
    tif_path.unlink()  # remove original
    log.info(f"Saved compressed file to {jp2_path}")
    return jp2_path


@app.command()
def lidar_hd(geojson_file: Path, output: Path, compress: bool = False) -> None:
    """
    Download LiDAR tiles intersecting the given AOI GeoJSON.

    Parameters
    ----------
    geojson_file : Path
        Path to a GeoJSON file defining the AOI polygon.
    output : Path
        Directory where files will be saved.
    compress : bool
        Whether to compress downloaded files to .jp2 or keep as .tif. Defaults to False.
    """
    output.mkdir(parents=True, exist_ok=True)

    # Load AOI
    gdf = gpd.read_file(geojson_file)
    gdf = gdf.to_crs(epsg=4326)
    poly = gdf.union_all()

    tiles = lidar_tiles_from_polygon(poly)
    urls = lidar_tile_urls(tiles)

    download_paths = {}
    all_tile_paths = []
    FILENAME_RE = re.compile(r"[?&]FILENAME=([^&]+)")
    for url in urls:
        filename_match = FILENAME_RE.search(url)
        filename = filename_match.group(1) if filename_match else Path(url).name
        download_path = output / filename
        all_tile_paths.append(download_path)
        if download_path.exists():
            log.info(f"{download_path} already exists. Skipping.")
            continue
        if download_path.with_suffix(".jp2").exists() and compress:
            log.info(f"{download_path.with_suffix('.jp2')} already exists. Skipping download.")
            continue
        download_paths[output / filename] = url

    # Download in parallel
    with default_bar() as progress:
        task = progress.add_task("Downloading LiDAR files", total=len(download_paths))
        Parallel(n_jobs=20, backend="threading")(
            delayed(download_file)(url, download_path, (progress, task))
            for download_path, url in download_paths.items()
        )

    log.info(f"Downloaded {len(download_paths)} LiDAR files to {output}")

    # Compress in parallel
    if compress:
        log.info("Compressing to .jp2.")
        files_to_compress = [p for p in all_tile_paths if p.exists()]

        with default_bar() as progress, ProcessPoolExecutor() as executor:
            task = progress.add_task("Compressing TIFs", total=len(files_to_compress))
            futures = {executor.submit(compress_lidar_tif, p): p for p in files_to_compress}

            for future in as_completed(futures):
                _ = future.result()  # raises if compress_tif failed
                progress.advance(task)

        log.info("Compression complete")


def merge_rgb_ir(
    rgb_path: Path,
    ir_path: Path,
    output_dir: Path | None = None,
    codec: str = "JP2OpenJPEG",
    quality: int = 20,
) -> None:
    """
    Merge RGB and single-band IR JP2 files into a 4-band JP2 (R,G,B,IR).

    Parameters
    ----------
    rgb_path : Path
        Path to RGB JP2 file or directory of files.
    ir_path : Path
        Path to IR JP2 file or directory of files.
    output_dir : Path | None
        Directory to write merged outputs. Auto-created if None.
    codec : str
        Rasterio driver to use (JP2OpenJPEG)
    quality : int
        Compression quality (1-100)
    """

    rgb_path = rgb_path.expanduser().absolute()
    ir_path = ir_path.expanduser().absolute()

    if output_dir is None:
        output_dir = (
            rgb_path.parent.parent / "bdortho_rgb_ir"
            if rgb_path.is_file()
            else rgb_path.parent / "bdortho_rgb_ir"
        )
    output_dir.mkdir(exist_ok=True, parents=True)

    # Handle directories recursively
    if rgb_path.is_dir() and ir_path.is_dir():
        rgb_files = [p for p in rgb_path.glob("[!.]*.jp2") if p.is_file()]
        ir_files = [p for p in ir_path.glob("[!.]*.jp2") if p.is_file()]

        def prefix_up_to_la93(p: Path) -> str | None:
            parts = p.stem.split("-")
            try:
                return "-".join(parts[: parts.index("LA93") + 1])
            except ValueError:
                return None

        rgb_map = {prefix: p for p in rgb_files if (prefix := prefix_up_to_la93(p)) is not None}
        ir_map = {prefix: p for p in ir_files if (prefix := prefix_up_to_la93(p)) is not None}

        common_prefixes = sorted(rgb_map.keys() & ir_map.keys())
        if not common_prefixes:
            raise ValueError("No matching RGB / IR pairs found.")

        with ProcessPoolExecutor(max_workers=min(os.cpu_count() or 1, 8)) as executor:
            futures = {
                executor.submit(
                    merge_rgb_ir,
                    rgb_map[prefix],
                    ir_map[prefix],
                    output_dir,
                    codec,
                    quality,
                ): prefix
                for prefix in common_prefixes
            }
            for future in as_completed(futures):
                future.result()
        return

    # Check filename consistency
    rgb_parts = rgb_path.stem.split("-")
    ir_parts = ir_path.stem.split("-")
    try:
        rgb_prefix = "-".join(rgb_parts[: rgb_parts.index("LA93") + 1])
        ir_prefix = "-".join(ir_parts[: ir_parts.index("LA93") + 1])
    except ValueError as e:
        raise ValueError("Both filenames must contain 'LA93'.") from e

    if rgb_prefix != ir_prefix:
        raise ValueError(f"Inconsistent inputs: {rgb_prefix} ≠ {ir_prefix}")

    out_path = output_dir / f"{rgb_prefix}-RGBIR.jp2"
    if out_path.exists():
        log.info(f"{out_path} exists. Skipping.")
        return

    tmp_path = out_path.with_suffix(".tmp.jp2")
    tmp_path.unlink(missing_ok=True)

    # Read RGB
    with rio.open(rgb_path) as rgb_ds:
        log.info(f"Reading {rgb_path}")
        rgb_data = rgb_ds.read()  # shape = (3, H, W)
        profile = rgb_ds.profile.copy()
        height, width = rgb_ds.height, rgb_ds.width

    # Read IR
    with rio.open(ir_path) as ir_ds:
        log.info(f"Reading {ir_path}")
        ir_data = ir_ds.read(1)  # shape = (H, W)
        if ir_ds.height != height or ir_ds.width != width:
            raise ValueError("RGB and IR dimensions mismatch.")

    # Stack as 4 bands
    merged_data = np.vstack([rgb_data, ir_data[None, :, :]])

    # Update profile for 4-band JP2
    profile.update(
        driver=codec,
        count=4,
        dtype=merged_data.dtype,
        tiled=True,
        compress="JPEG2000",
        quality=quality,
    )

    # Write temp JP2
    with rio.open(tmp_path, "w", **profile) as dst:
        log.info(f"Writing {tmp_path}")
        dst.write(merged_data)

    tmp_path.rename(out_path)
    log.info(f"Merged JP2 written: {out_path}")
