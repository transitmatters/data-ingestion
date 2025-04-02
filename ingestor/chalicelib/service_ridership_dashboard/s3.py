import boto3
import json
from datetime import date

from .util import date_to_string
from .types import DashJSON

bucket = boto3.resource("s3").Bucket("tm-service-ridership-dashboard")


def put_dashboard_json_to_s3(today: date, dash_json: DashJSON) -> None:
    print("Uploading dashboard JSON to S3")
    contents = json.dumps(dash_json)
    bucket.put_object(Key=f"{date_to_string(today)}.json", Body=contents)
    bucket.put_object(Key="latest.json", Body=contents)
