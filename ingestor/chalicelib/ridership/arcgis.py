import logging
from tempfile import NamedTemporaryFile
from typing import Tuple

import requests

from .sharepoint import SharepointConnection
from .config import (
    CR_UPDATE_CACHE_URL,
    FERRY_UPDATE_CACHE_URL,
    THE_RIDE_UPDATE_CACHE_URL,
    CR_RIDERSHIP_ARCGIS_URL,
    FERRY_RIDERSHIP_ARCGIS_URL,
    THE_RIDE_RIDERSHIP_ARCGIS_URL,
    RIDERSHIP_SUBWAY_STATION_CSV_REGEX,
)

logger = logging.getLogger(__name__)

# Kept well inside update_ridership's 300s Lambda timeout across three refreshes and three downloads.
ARCGIS_CACHE_TIMEOUT = 15  # seconds
ARCGIS_DOWNLOAD_TIMEOUT = 30  # seconds


def _update_cache(url: str, name: str) -> None:
    """Ask ArcGIS Hub to refresh a dataset's cache. Best effort: the last cached file still works."""
    try:
        requests.get(url, timeout=ARCGIS_CACHE_TIMEOUT).raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Couldn't refresh the {name} ridership cache: {e}")


def _download_arcgis_csv(url: str, name: str) -> str | None:
    """Download an ArcGIS CSV to a temp file, or return None so that mode is skipped."""
    try:
        response = requests.get(url, timeout=ARCGIS_DOWNLOAD_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Couldn't download {name} ridership; skipping it: {e}")
        return None
    path = NamedTemporaryFile().name
    with open(path, "wb") as file:
        file.write(response.content)
    return path


def cr_update_cache():
    """
    This function is used to update the cache for the CR ridership data
    on the ArcGIS Hub. This is necessary because the data is large enough
    where the cache isn't updated automatically.
    """
    _update_cache(CR_UPDATE_CACHE_URL, "CR")


def ferry_update_cache():
    """
    This function is used to update the cache for the Ferry ridership data
    on the ArcGIS Hub. This is necessary because the data is large enough
    where the cache isn't updated automatically.
    """
    _update_cache(FERRY_UPDATE_CACHE_URL, "Ferry")


def ride_update_cache():
    """
    This function is used to update the cache for the The RIDE ridership data
    on the ArcGIS Hub. This is necessary because the data is large enough
    where the cache isn't updated automatically.
    """
    _update_cache(THE_RIDE_UPDATE_CACHE_URL, "The RIDE")


def download_latest_ridership_files() -> Tuple[str | None, str | None, str | None, str | None, str | None, str | None]:
    """Download the latest ridership files for all transit modes.

    Fetches subway (by line and by station) and bus data from SharePoint, and
    commuter rail, ferry, and The RIDE data from ArcGIS.

    Returns:
        Tuple of file paths (subway, subway by station, bus, commuter rail,
        ferry, The RIDE), where each element may be None if the download failed.
    """
    sharepoint = SharepointConnection()

    subway_tmp_path = sharepoint.fetch_sharepoint_file(bus_data=False)
    subway_station_tmp_path = sharepoint.fetch_sharepoint_file(
        file_regex=RIDERSHIP_SUBWAY_STATION_CSV_REGEX, bus_data=False
    )
    bus_tmp_path = sharepoint.fetch_sharepoint_file(bus_data=True)

    cr_tmp_path = _download_arcgis_csv(CR_RIDERSHIP_ARCGIS_URL, "CR")
    ferry_tmp_path = _download_arcgis_csv(FERRY_RIDERSHIP_ARCGIS_URL, "Ferry")
    ride_tmp_path = _download_arcgis_csv(THE_RIDE_RIDERSHIP_ARCGIS_URL, "The RIDE")
    return subway_tmp_path, subway_station_tmp_path, bus_tmp_path, cr_tmp_path, ferry_tmp_path, ride_tmp_path
