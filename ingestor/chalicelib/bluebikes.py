import json
import requests
import pandas as pd
import datetime

from chalicelib import s3

BUCKET = "tm-mbta-performance"
KEY = "ingestTest/tm-bluebikes/{}/{}/bluebikes.csv"

def store_station_status():
    resp = requests.get("https://gbfs.bluebikes.com/gbfs/en/station_status.json")

    datajson = json.loads(resp.content)

    df = pd.DataFrame.from_records(datajson.get('data').get('stations'))

    timestamp = datajson.get('last_updated')
    date = datetime.datetime.fromtimestamp(timestamp).date()
    
    key = KEY.format(date, timestamp)
    
    s3.upload_df_as_csv(BUCKET, key, df)

#### TODO:
# we should probably store the daily station configuration for neighbor calcs, painful as that is.
