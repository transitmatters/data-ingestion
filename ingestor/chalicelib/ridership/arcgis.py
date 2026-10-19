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
)


def cr_update_cache():
    """
    This function is used to update the cache for the CR ridership data
    on the ArcGIS Hub. This is necessary because the data is large enough
    where the cache isn't updated automatically.
    """
    requests.get(CR_UPDATE_CACHE_URL)


def ferry_update_cache():
    """
    This function is used to update the cache for the Ferry ridership data
    on the ArcGIS Hub. This is necessary because the data is large enough
    where the cache isn't updated automatically.
    """
    requests.get(FERRY_UPDATE_CACHE_URL)


def ride_update_cache():
    """
    This function is used to update the cache for the The RIDE ridership data
    on the ArcGIS Hub. This is necessary because the data is large enough
    where the cache isn't updated automatically.
    """
    requests.get(THE_RIDE_UPDATE_CACHE_URL)


def download_latest_ridership_files() -> Tuple[str | None, str | None, str | None, str | None, str | None]:
    """Download the latest ridership files for all transit modes.

    Fetches subway and bus data from SharePoint, and commuter rail, ferry,
    and The RIDE data from ArcGIS.

    Returns:
        Tuple of file paths (subway, bus, commuter rail, ferry, The RIDE),
        where each element may be None if the download failed.
    """
    sharepoint = SharepointConnection()

    cr_tmp_path = NamedTemporaryFile().name
    ferry_tmp_path = NamedTemporaryFile().name
    ride_tmp_path = NamedTemporaryFile().name
    subway_tmp_path = sharepoint.fetch_sharepoint_file(bus_data=False)
    bus_tmp_path = sharepoint.fetch_sharepoint_file(bus_data=True)

    with open(cr_tmp_path, "wb") as file:
        req = requests.get(CR_RIDERSHIP_ARCGIS_URL, timeout=15)
        file.write(req.content)
    with open(ferry_tmp_path, "wb") as file:
        req = requests.get(FERRY_RIDERSHIP_ARCGIS_URL)
        file.write(req.content)
    with open(ride_tmp_path, "wb") as file:
        req = requests.get(THE_RIDE_RIDERSHIP_ARCGIS_URL)
        file.write(req.content)
    return subway_tmp_path, bus_tmp_path, cr_tmp_path, ferry_tmp_path, ride_tmp_path
