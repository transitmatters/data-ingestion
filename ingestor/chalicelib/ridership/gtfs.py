import boto3
from typing import Dict
from tempfile import TemporaryDirectory
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import Route

from ..gtfs.utils import bucket_by


def get_routes_by_line_id() -> Dict[str, Route]:
    """Fetch GTFS route data from S3 and group routes by their line ID.

    Returns:
        Mapping of line IDs to lists of Route objects from the latest GTFS feed.
    """
    s3 = boto3.resource("s3")
    archive = MbtaGtfsArchive(
        local_archive_path=TemporaryDirectory().name,
        s3_bucket=s3.Bucket("tm-gtfs"),
    )
    feed = archive.get_latest_feed()
    feed.use_compact_only()
    feed.download_or_build()
    session = feed.create_sqlite_session(compact=True)
    return bucket_by(session.query(Route).all(), lambda r: r.line_id)
