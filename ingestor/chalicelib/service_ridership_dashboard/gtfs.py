import boto3
from typing import Dict
from tempfile import TemporaryDirectory
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import Route, Line, RouteType

from ..gtfs.utils import bucket_by, index_by

from .config import IGNORE_LINE_IDS


def get_routes_by_line() -> Dict[Line, Route]:
    s3 = boto3.resource("s3")
    archive = MbtaGtfsArchive(
        local_archive_path=TemporaryDirectory().name,
        s3_bucket=s3.Bucket("tm-gtfs"),
    )
    feed = archive.get_latest_feed()
    feed.use_compact_only()
    feed.download_or_build()
    session = feed.create_sqlite_session(compact=True)
    lines_by_id = index_by(session.query(Line).all(), lambda line: line.line_id)
    all_routes_with_line_ids = [
        route for route in session.query(Route).all() if route.line_id and route.line_id not in IGNORE_LINE_IDS
    ]
    return bucket_by(all_routes_with_line_ids, lambda route: lines_by_id[route.line_id])