import boto3
import json
from datetime import date
from re import Pattern
from boxsdk import OAuth2, Client
from boxsdk.object.file import File
from typing import List


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
