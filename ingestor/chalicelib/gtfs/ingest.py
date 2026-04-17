import boto3
from tempfile import TemporaryDirectory
from datetime import date
from typing import List, Tuple, Union
from sqlalchemy.orm import Session
from mbta_gtfs_sqlite import MbtaGtfsArchive, GtfsFeed
from mbta_gtfs_sqlite.models import (
    CalendarService,
    CalendarAttribute,
    CalendarServiceException,
    Trip,
    Route,
)

from .utils import (
    bucket_by,
    bucket_trips_by_hour,
    date_range,
    index_by,
    is_valid_route_id,
    get_service_ids_for_date_to_has_exceptions,
    get_total_service_minutes,
)
from .models import SessionModels, RouteDateTotals


def load_session_models(session: Session) -> SessionModels:
    """Query all GTFS models from a SQLAlchemy session and index them into dicts keyed by ID.

    Args:
        session: A SQLAlchemy session connected to a GTFS SQLite database.

    Returns:
        A SessionModels instance with all models indexed by their respective keys.
    """
    calendar_services = session.query(CalendarService).all()
    calendar_attributes = session.query(CalendarAttribute).all()
    calendar_service_exceptions = session.query(CalendarServiceException).all()
    trips = session.query(Trip).all()
    routes = session.query(Route).all()
    return SessionModels(
        calendar_services=index_by(calendar_services, lambda x: x.service_id),
        calendar_attributes=index_by(calendar_attributes, lambda x: x.service_id),
        calendar_service_exceptions=bucket_by(
            calendar_service_exceptions,
            lambda x: x.service_id,
        ),
        trips_by_route_id=bucket_by(trips, lambda x: x.route_id),
        routes=index_by(routes, lambda x: x.route_id),
    )


def create_gl_route_date_totals(totals: List[RouteDateTotals]) -> RouteDateTotals:
    """Aggregate Green Line branch totals into a single combined Green Line total.

    Args:
        totals: List of RouteDateTotals for all routes on a given date.

    Returns:
        A single RouteDateTotals representing the combined Green Line service.
    """
    gl_totals = [t for t in totals if t.route_id.startswith("Green-")]
    total_by_hour = [0] * 24
    for total in gl_totals:
        for i in range(24):
            total_by_hour[i] += total.by_hour[i]
    total_count = sum(t.count for t in gl_totals)
    total_service_minutes = sum(t.service_minutes for t in gl_totals)
    has_service_exceptions = any((t.has_service_exceptions for t in gl_totals))
    return RouteDateTotals(
        route_id="Green",
        line_id="Green",
        date=totals[0].date,
        count=total_count,
        service_minutes=total_service_minutes,
        by_hour=total_by_hour,
        has_service_exceptions=has_service_exceptions,
    )


def create_route_date_totals(today: date, models: SessionModels) -> List[RouteDateTotals]:
    """Create scheduled service totals for all valid routes on a given date.

    Args:
        today: The date to compute totals for.
        models: The SessionModels containing all GTFS data.

    Returns:
        A list of RouteDateTotals for each valid route, including a combined
        Green Line entry.
    """
    all_totals = []
    service_ids_and_exception_status_for_today = get_service_ids_for_date_to_has_exceptions(models, today)
    for route_id, route in models.routes.items():
        if not is_valid_route_id(route_id):
            continue
        trips = [
            trip
            for trip in models.trips_by_route_id.get(route_id, [])
            if trip.service_id in service_ids_and_exception_status_for_today.keys()
        ]
        has_service_exceptions = any(
            (service_ids_and_exception_status_for_today.get(trip.service_id, False) for trip in trips)
        )
        totals = RouteDateTotals(
            route_id=route_id,
            line_id=route.line_id,
            date=today,
            count=len(trips),
            by_hour=bucket_trips_by_hour(trips),
            has_service_exceptions=has_service_exceptions,
            service_minutes=get_total_service_minutes(trips),
        )
        all_totals.append(totals)
    all_totals.append(create_gl_route_date_totals(all_totals))
    return all_totals


def ingest_feed_to_dynamo(
    dynamodb,
    session: Session,
    start_date: date,
    end_date: date,
) -> None:
    """Compute and write scheduled service totals to DynamoDB for a date range.

    Args:
        dynamodb: A boto3 DynamoDB resource.
        session: A SQLAlchemy session connected to a GTFS SQLite database.
        start_date: The first date to ingest (inclusive).
        end_date: The last date to ingest (inclusive).
    """
    ScheduledServiceDaily = dynamodb.Table("ScheduledServiceDaily")
    models = load_session_models(session)
    for today in date_range(start_date, end_date):
        totals = create_route_date_totals(today, models)
        with ScheduledServiceDaily.batch_writer() as batch:
            for total in totals:
                item = {
                    "date": total.date.isoformat(),
                    "timestamp": int(total.timestamp),
                    "routeId": total.route_id,
                    "lineId": total.line_id,
                    "count": total.count,
                    "serviceMinutes": total.service_minutes,
                    "hasServiceExceptions": total.has_service_exceptions,
                    "byHour": {"totals": total.by_hour},
                }
                batch.put_item(Item=item)


def ingest_feeds(
    dynamodb,
    feeds: List[GtfsFeed],
    start_date: date,
    end_date: date,
    force_rebuild_feeds: bool = False,
) -> None:
    """Process a list of GTFS feeds by building/downloading them and ingesting to DynamoDB.

    Each feed is either built locally, downloaded from S3, or reused if already
    present. The resulting SQLite database is then ingested into DynamoDB.

    Args:
        dynamodb: A boto3 DynamoDB resource.
        feeds: List of GtfsFeed objects to process.
        start_date: The first date to ingest (inclusive).
        end_date: The last date to ingest (inclusive).
        force_rebuild_feeds: If True, forces all feeds to be rebuilt locally
            and re-uploaded to S3. Defaults to False.
    """
    for feed in feeds:
        feed.use_compact_only()
        try:
            if force_rebuild_feeds:
                print(f"[{feed.key}] Forcing rebuild locally")
                feed.build_locally()
                print(f"[{feed.key}] Uploading to S3")
                feed.upload_to_s3()
            else:
                exists_locally = feed.exists_locally()
                exists_remotely = feed.exists_remotely()
                if exists_locally:
                    print(f"[{feed.key}] Exists locally")
                elif exists_remotely:
                    print(f"[{feed.key}] Downloading from S3")
                    feed.use_compact_only()
                    feed.download_from_s3()
                else:
                    print(f"[{feed.key}] Building locally")
                    feed.build_locally()
                if not exists_remotely:
                    print(f"[{feed.key}] Uploading to S3")
                    feed.upload_to_s3()
            session = feed.create_sqlite_session(compact=True)
            ingest_feed_to_dynamo(
                dynamodb,
                session,
                max(feed.start_date, start_date),
                min(feed.end_date, end_date, date.today()),
            )
        except Exception as ex:
            print(f"[{feed.key}] Failed to retrieve")
            print(ex)


def ingest_gtfs_feeds_to_dynamo_and_s3(
    date_range: Union[None, Tuple[date, date]] = None,
    feed_key: Union[None, str] = None,
    local_archive_path: str | None = None,
    boto3_session: boto3.Session | None = None,
    force_rebuild_feeds: bool = False,
) -> None:
    """Orchestrate the full GTFS ingestion pipeline from archive to DynamoDB and S3.

    Either a date_range or a feed_key must be provided to identify which feeds
    to process.

    Args:
        date_range: A tuple of (start_date, end_date) to select feeds covering
            that range. Defaults to None.
        feed_key: A specific feed key to ingest. Defaults to None.
        local_archive_path: Path to store local feed files. If None, a temporary
            directory is used. Defaults to None.
        boto3_session: An optional boto3 Session to use for AWS operations. If
            None, a new session is created. Defaults to None.
        force_rebuild_feeds: If True, forces all feeds to be rebuilt locally.
            Defaults to False.

    Raises:
        Exception: If neither date_range nor feed_key is provided.
    """
    if not boto3_session:
        boto3_session = boto3.Session()
    if not local_archive_path:
        local_archive_path = TemporaryDirectory().name
    archive = MbtaGtfsArchive(
        local_archive_path=local_archive_path,
        s3_bucket=boto3_session.resource("s3").Bucket("tm-gtfs"),
    )
    if date_range:
        start_date, end_date = date_range
        feeds = archive.get_feeds_for_dates(start_date, end_date)
    elif feed_key:
        feed = archive.get_feed_by_key(feed_key)
        start_date = feed.start_date
        end_date = feed.end_date
        feeds = [feed]
    else:
        raise Exception("Must provide either date_range or feed_key")
    ingest_feeds(
        dynamodb=boto3_session.resource("dynamodb"),
        feeds=feeds,
        start_date=start_date,
        end_date=end_date,
        force_rebuild_feeds=force_rebuild_feeds,
    )


def get_feed_keys_for_date_range(start_date: date, end_date: date) -> List[str]:
    """Retrieve the feed keys for all GTFS feeds that cover a given date range.

    Args:
        start_date: The start of the date range (inclusive).
        end_date: The end of the date range (inclusive).

    Returns:
        A list of feed key strings for feeds overlapping the date range.
    """
    archive = MbtaGtfsArchive(local_archive_path=TemporaryDirectory().name)
    feeds = archive.get_feeds_for_dates(start_date, end_date)
    return [feed.key for feed in feeds]
