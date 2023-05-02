import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

from ..ingest import ingest_gtfs_feeds_to_dynamo_and_s3

load_dotenv()

env_start_date = datetime.strptime(os.environ["BACKFILL_START_DATE"], "%Y-%m-%d").date()
env_end_date = datetime.strptime(os.environ["BACKFILL_END_DATE"], "%Y-%m-%d").date()
env_local_archive_path = os.environ.get("LOCAL_ARCHIVE_PATH", "./feeds")

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

ingest_gtfs_feeds_to_dynamo_and_s3(
    start_date=env_start_date,
    end_date=env_end_date,
    local_archive_path=env_local_archive_path,
    boto3_session=session,
)
