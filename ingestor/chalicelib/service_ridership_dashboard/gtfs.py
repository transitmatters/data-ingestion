from tempfile import TemporaryDirectory
from typing import Optional

import boto3
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import Line, Route

from ..gtfs.utils import bucket_by, index_by
from .config import IGNORE_LINE_IDS

RoutesByLine = dict[Line, Route]


def get_routes_by_line(include_only_line_ids: Optional[list[str]]) -> dict[Line, Route]:
    """Fetch routes from the latest GTFS feed and group them by their parent line.

    Args:
        include_only_line_ids: If provided, only include routes belonging to these line IDs.

    Returns:
        A dictionary mapping Line objects to their associated Route objects.
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
    lines_by_id = index_by(
        session.query(Line).all(),
        lambda line: line.line_id,
    )
    all_routes_with_line_ids = [
        route
        for route in session.query(Route).all()
        if route.line_id
        and route.line_id not in IGNORE_LINE_IDS
        and (not include_only_line_ids or route.line_id in include_only_line_ids)
    ]
    return bucket_by(all_routes_with_line_ids, lambda route: lines_by_id[route.line_id])
