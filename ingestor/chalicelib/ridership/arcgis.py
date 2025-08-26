from tempfile import NamedTemporaryFile
from typing import Tuple
import requests

from .config import (
    CR_RIDERSHIP_ARCGIS_URL,
    CR_UPDATE_CACHE_URL,
)


def cr_update_cache():
    """
    This function is used to update the cache for the CR ridership data
    on the ArcGIS Hub. This is necessary because the data is large enough
    where the cache isn't updated automatically.
    """
    requests.get(CR_UPDATE_CACHE_URL)


def download_latest_ridership_files() -> Tuple[None, None, str]:
    cr_tmp_path = NamedTemporaryFile().name

    with open(cr_tmp_path, "wb") as file:
        req = requests.get(CR_RIDERSHIP_ARCGIS_URL, timeout=15)
        file.write(req.content)
    return None, None, cr_tmp_path
