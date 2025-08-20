import requests

from .config import CR_UPDATE_CACHE_URL, FERRY_UPDATE_CACHE_URL


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
