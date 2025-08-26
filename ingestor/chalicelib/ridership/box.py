import boto3
import json
from datetime import date
from re import Pattern
from tempfile import NamedTemporaryFile
from typing import List, Tuple
from boxsdk import OAuth2, Client
from boxsdk.object.file import File
import requests

from .config import (
    CR_RIDERSHIP_ARCGIS_URL,
    FERRY_RIDERSHIP_ARCGIS_URL,
    RIDERSHIP_BOX_URL,
    RIDERSHIP_BUS_XLSX_REGEX,
    RIDERSHIP_SUBWAY_CSV_REGEX,
)


def get_secret_access_token():
    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId="MassDotBoxAccessToken")
    response_string = secret.get("SecretString")
    response = json.loads(response_string)
    return response["key"]


def get_box_client():
    oauth = OAuth2(
        client_id=None,
        client_secret=None,
        access_token=get_secret_access_token(),
    )
    return Client(oauth=oauth)


def get_file_matching_date_pattern(files: List[File], pattern: Pattern):
    for file in files:
        match = pattern.match(file.name)
        if match:
            year = match[1]
            month = match[2]
            day = match[3]
            file_date = date(year=int(year), month=int(month), day=int(day))
            return file, file_date


def download_latest_ridership_files(
    require_matching_dates=False,
) -> Tuple[str, str, str, str]:
    client = get_box_client()
    folder = client.get_shared_item(RIDERSHIP_BOX_URL)
    files = list(folder.get_items())
    maybe_bus_file_and_date = get_file_matching_date_pattern(
        files,
        RIDERSHIP_BUS_XLSX_REGEX,
    )
    maybe_subway_file_and_date = get_file_matching_date_pattern(
        files,
        RIDERSHIP_SUBWAY_CSV_REGEX,
    )
    if maybe_bus_file_and_date and maybe_subway_file_and_date:
        subway_file, subway_date = maybe_subway_file_and_date
        bus_file, bus_date = maybe_bus_file_and_date
        assert (
            not require_matching_dates or bus_date == subway_date
        ), f"Mismatched file dates: {bus_date} and {subway_date}"
        subway_tmp_path = NamedTemporaryFile().name
        bus_tmp_path = NamedTemporaryFile().name
        cr_tmp_path = NamedTemporaryFile().name
        ferry_tmp_path = NamedTemporaryFile().name
        with open(subway_tmp_path, "wb") as file:
            subway_file.download_to(file)
        with open(bus_tmp_path, "wb") as file:
            bus_file.download_to(file)
        with open(cr_tmp_path, "wb") as file:
            req = requests.get(CR_RIDERSHIP_ARCGIS_URL)
            file.write(req.content)
        with open(ferry_tmp_path, "wb") as file:
            req = requests.get(FERRY_RIDERSHIP_ARCGIS_URL)
            file.write(req.content)
        return subway_tmp_path, bus_tmp_path, cr_tmp_path, ferry_tmp_path
    raise Exception("Could not find ridership data files")
