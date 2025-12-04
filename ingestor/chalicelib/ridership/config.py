import re

RIDERSHIP_BUS_XLSX_REGEX = re.compile(r"Weekly_Bus_Ridership_by_Route_(\d{4})\.(\d{1,2})\.(\d{1,2})", re.I)

RIDERSHIP_SUBWAY_CSV_REGEX = re.compile(r"(\d{4})\.(\d{1,2})\.(\d{1,2}) MBTA Gated Station Validations by line", re.I)

CR_RIDERSHIP_ARCGIS_URL = "https://www.arcgis.com/sharing/rest/content/items/59b5c61e8e9f42f9a3745f7ad63d07d6/data?format=csv&spatialRefId=4326&where=1=1"

CR_UPDATE_CACHE_URL = "https://hub.arcgis.com/api/download/v1/items/59b5c61e8e9f42f9a3745f7ad63d07d6/csv?redirect=false&layers=0&updateCache=true"

FERRY_UPDATE_CACHE_URL = "https://hub.arcgis.com/api/download/v1/items/ae21643bbe60488db8520cc694f882aa/csv?redirect=false&layers=0&updateCache=true"

FERRY_RIDERSHIP_ARCGIS_URL = "https://opendata.arcgis.com/api/v3/datasets/ae21643bbe60488db8520cc694f882aa_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"

THE_RIDE_UPDATE_CACHE_URL = "https://hub.arcgis.com/api/download/v1/items/e93e4e4820ca4719b3c4134ae0865053/csv?redirect=false&layers=0&updateCache=true"

THE_RIDE_RIDERSHIP_ARCGIS_URL = "https://opendata.arcgis.com/api/v3/datasets/e93e4e4820ca4719b3c4134ae0865053_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"
