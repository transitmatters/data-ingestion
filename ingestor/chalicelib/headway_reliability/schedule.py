"""Trips the GTFS schedule sends through each subway stop on a day.

This needs the full feed (~350 MB from s3://tm-gtfs, once a run): the compact one the GTFS ingest
reads has no stop_times. A feed the GTFS ingest hasn't published yet leaves its days out, and the
next run, which redoes the day before, fills them in.
"""

import os
import tempfile
from datetime import date
from itertools import groupby

import boto3
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import CalendarService, CalendarServiceException, StopTime, Trip
from sqlalchemy import distinct, func

from ..gtfs.models import SessionModels
from ..gtfs.utils import bucket_by, get_service_ids_for_date_to_has_exceptions, index_by
from .trips import LINE_ROUTES, Scheduled

GTFS_BUCKET = "tm-gtfs"
SUBWAY_ROUTES = [route for routes in LINE_ROUTES.values() for route in routes]

# A local backfill can point this at a folder of feeds it keeps (one subfolder per feed key).
ARCHIVE_PATH = os.environ.get("GTFS_ARCHIVE_PATH") or os.path.join(tempfile.gettempdir(), "gtfs_feeds")


def _calendar(session) -> SessionModels:
    return SessionModels(
        calendar_services=index_by(session.query(CalendarService).all(), lambda s: s.service_id),
        calendar_attributes={},
        calendar_service_exceptions=bucket_by(session.query(CalendarServiceException).all(), lambda e: e.service_id),
        trips_by_route_id={},
        routes={},
    )


def _scheduled_on(session, calendar: SessionModels, day: date, stops: set[str]) -> Scheduled:
    services = list(get_service_ids_for_date_to_has_exceptions(calendar, day))
    rows = (
        session.query(StopTime.stop_id, Trip.route_id, func.count(distinct(StopTime.trip_id)))
        .join(Trip, Trip.trip_id == StopTime.trip_id)
        .filter(Trip.route_id.in_(SUBWAY_ROUTES), Trip.service_id.in_(services), StopTime.stop_id.in_(stops))
        .group_by(StopTime.stop_id, Trip.route_id)
        .all()
    )
    return {(stop, route): trips for stop, route, trips in rows}


def scheduled_trips(days: list[date], stops: set[str]) -> dict[date, Scheduled]:
    """{day: {(stop, route): trips}} for the subway routes. Days whose feed isn't available are left out."""
    archive = MbtaGtfsArchive(local_archive_path=ARCHIVE_PATH, s3_bucket=boto3.resource("s3").Bucket(GTFS_BUCKET))
    result = {}
    for feed, feed_days in groupby(days, key=archive.get_feed_for_date):
        feed_days = list(feed_days)
        if feed is None:
            print(f"No GTFS feed covers {feed_days[0]} in the MBTA's archive index")
            continue
        if not feed.exists_locally():
            if not feed.exists_remotely():
                print(f"GTFS feed {feed.key} isn't in s3://{GTFS_BUCKET} yet; leaving out trip counts for {feed_days}")
                continue
            feed.download_from_s3()
        session = feed.create_sqlite_session(compact=False)
        try:
            calendar = _calendar(session)
            for day in feed_days:
                result[day] = _scheduled_on(session, calendar, day, stops)
        finally:
            # An open handle keeps the feed's disk space in use.
            session.close()
            session.bind.dispose()
    return result
