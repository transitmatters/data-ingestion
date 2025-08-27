from tempfile import NamedTemporaryFile
import requests

from .config import FERRY_UPDATE_CACHE_URL, FERRY_RIDERSHIP_ARCGIS_URL


def ferry_update_cache():
    """
    This function is used to update the cache for the Ferry ridership data
    on the ArcGIS Hub. This is necessary because the data is large enough
    where the cache isn't updated automatically.
    """
    requests.get(FERRY_UPDATE_CACHE_URL)


def download_latest_ferry_data() -> str:
    ferry_tmp_path = NamedTemporaryFile().name

    with open(ferry_tmp_path, "wb") as file:
        req = requests.get(FERRY_RIDERSHIP_ARCGIS_URL, timeout=15)
        file.write(req.content)
    return ferry_tmp_path
