import boto3
from tempfile import TemporaryDirectory
from datetime import date
from sqlalchemy.orm import Session
from mbta_gtfs_sqlite import MbtaGtfsArchive
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
    get_services_for_date,
)
from .models import SessionModels, RouteDateTotals


def load_session_models(session: Session):
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


def create_route_date_totals(today: date, models: SessionModels):
    all_totals = []
    services_for_today = get_services_for_date(models, today)
    for route_id, route in models.routes.items():
        if not is_valid_route_id(route_id):
            continue
        trips = [
            trip
            for trip in models.trips_by_route_id.get(route_id, [])
            if trip.service_id in services_for_today
        ]
        totals = RouteDateTotals(
            route_id=route_id,
            line_id=route.line_id,
            date=today,
            count=len(trips),
            by_hour=bucket_trips_by_hour(trips),
        )
        all_totals.append(totals)
    return all_totals


def ingest_feed_to_dynamo(
    dynamodb,
    session: Session,
    start_date: date,
    end_date: date,
):
    TripCounts = dynamodb.Table("TripCounts")
    models = load_session_models(session)
    for today in date_range(start_date, end_date):
        totals = create_route_date_totals(today, models)
        with TripCounts.batch_writer() as batch:
            for total in totals:
                item = {
                    "date": total.date.isoformat(),
                    "timestamp": int(total.timestamp),
                    "routeId": total.route_id,
                    "lineId": total.line_id,
                    "count": total.count,
                    "byHour": {"totals": total.by_hour},
                }
                batch.put_item(Item=item)


def ingest_feeds(dynamodb, archive: MbtaGtfsArchive, start_date: date, end_date: date):
    for feed in archive.get_feeds_for_dates(start_date=start_date, end_date=end_date):
        try:
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
    start_date: date,
    end_date: date,
    local_archive_path: str = None,
    boto3_session=None,
):
    if not boto3_session:
        boto3_session = boto3.Session()
    if not local_archive_path:
        local_archive_path = TemporaryDirectory().name
    ingest_feeds(
        dynamodb=boto3_session.resource("dynamodb"),
        archive=MbtaGtfsArchive(
            local_archive_path=local_archive_path,
            s3_bucket=boto3_session.resource("s3").Bucket("tm-gtfs"),
        ),
        start_date=start_date,
        end_date=end_date,
    )
