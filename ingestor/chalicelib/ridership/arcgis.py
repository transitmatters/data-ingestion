from tempfile import NamedTemporaryFile
from typing import Tuple
import requests

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


def download_latest_ridership_files() -> Tuple[None, None, str, str, str]:
    cr_tmp_path = NamedTemporaryFile().name
    ferry_tmp_path = NamedTemporaryFile().name
    ride_tmp_path = NamedTemporaryFile().name

    with open(cr_tmp_path, "wb") as file:
        req = requests.get(CR_RIDERSHIP_ARCGIS_URL, timeout=15)
        file.write(req.content)
    with open(ferry_tmp_path, "wb") as file:
        req = requests.get(FERRY_RIDERSHIP_ARCGIS_URL)
        file.write(req.content)
    with open(ride_tmp_path, "wb") as file:
        req = requests.get(THE_RIDE_RIDERSHIP_ARCGIS_URL)
        file.write(req.content)
    return None, None, cr_tmp_path, ferry_tmp_path, ride_tmp_path
