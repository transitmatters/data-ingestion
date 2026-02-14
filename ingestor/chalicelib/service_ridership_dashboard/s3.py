import json
from datetime import date

import boto3

from .types import DashJSON
from .util import date_to_string

bucket = boto3.resource("s3").Bucket("tm-service-ridership-dashboard")


def put_dashboard_json_to_s3(today: date, dash_json: DashJSON) -> None:
    """Upload dashboard JSON data to S3 as both a dated file and latest.json.

    Args:
        today: The date used to name the dated JSON file.
        dash_json: The dashboard JSON data to upload.
    """
    print("Uploading dashboard JSON to S3")
    contents = json.dumps(dash_json)
    bucket.put_object(Key=f"{date_to_string(today)}.json", Body=contents)
    bucket.put_object(Key="latest.json", Body=contents)
