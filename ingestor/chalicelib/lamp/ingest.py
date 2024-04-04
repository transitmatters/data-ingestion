from datetime import datetime
import requests

from .utils import get_current_service_date


def download_lamp_file(date: datetime.date):
    """
    LAMP (Lightweight Application for Measuring Performance) is data from the MBTA for system performance.

    Files are parquet files and are made available daily, and updated on a regular schedule.

    Details available at https://performancedata.mbta.com/
    """
    url = f"https://performancedata.mbta.com/lamp/subway-on-time-performance-v1/{date.strftime('%Y-%m-%d')}-subway-on-time-performance-v1.parquet"

    response = requests.get(url)
    if response.status_code == 200:
        with open(f"{date.strftime('%Y-%m-%d')}-subway-on-time-performance-v1.parquet", "wb") as file:
            file.write(response.content)
        print("File downloaded successfully.")
    else:
        print("Failed to download file.")


def ingest_lamp_data():
    # download today's file
    download_lamp_file(get_current_service_date())

    # process it

    pass


if __name__ == "__main__":
    ingest_lamp_data()
