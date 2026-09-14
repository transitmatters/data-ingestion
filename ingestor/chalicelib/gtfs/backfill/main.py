import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

from ..ingest import ingest_gtfs_feeds_to_dynamo_and_s3

load_dotenv()

env_start_date = datetime.strptime(os.environ["BACKFILL_START_DATE"], "%Y-%m-%d").date()
env_end_date = datetime.strptime(os.environ["BACKFILL_END_DATE"], "%Y-%m-%d").date()
# Feeds are cached here between runs. Objects in s3://tm-gtfs transition to
# Glacier Instant Retrieval 180 days after upload, and retrieval is billed at
# $0.03/GB against ~450MB per feed. Access stays millisecond -- there is no
# restore step -- but you pay per read, and reading an object does NOT promote
# it back to Standard.
#
# So if you are about to run several backfills over data older than ~6 months,
# either keep this directory between runs or promote the feeds you need first:
#
#   aws s3 cp s3://tm-gtfs/<feed_key>/ s3://tm-gtfs/<feed_key>/ \
#       --recursive --storage-class STANDARD
#
# For scale: re-reading the entire 205GB archive once costs about $6, against
# roughly $3.67/month saved by the lifecycle rule.
env_local_archive_path = os.environ.get("LOCAL_ARCHIVE_PATH", "./feeds")

session = boto3.Session()

ingest_gtfs_feeds_to_dynamo_and_s3(
    date_range=(env_start_date, env_end_date),
    local_archive_path=env_local_archive_path,
    boto3_session=session,
)
