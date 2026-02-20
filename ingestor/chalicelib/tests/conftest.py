"""
Shared pytest fixtures for integration tests.

The ``gtfs_session`` fixture provides an in-memory SQLite session seeded with
minimal but realistic MBTA GTFS shuttle data.  It avoids any network calls or
real S3/DynamoDB access and is safe to run in CI without AWS credentials.

Schema notes
------------
Every mbta_gtfs_sqlite model inherits two columns from Base:
  - id             Integer PK (autoincrement)
  - feed_info_id   Integer FK → FeedInfo.id  (NOT NULL)

FeedInfo itself also inherits feed_info_id from Base, making it a
self-referential FK.  SQLite does not enforce FK constraints by default, so we
set feed_info_id=1 on the FeedInfo row itself (pointing to its own eventual
id=1) without any circular-insert problem.
"""

from datetime import date

import boto3
import pytest
from moto import mock_aws
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from mbta_gtfs_sqlite.models import (
    Base,
    FeedInfo,
    RoutePattern,
    RoutePatternTypicality,
    ShapePoint,
    Stop,
    Trip,
)

# All rows share this feed_info_id value (matches the auto-assigned FeedInfo.id=1).
_FEED_ID = 1

# ---------------------------------------------------------------------------
# Row builder helpers — provide required non-nullable defaults so individual
# tests only need to specify fields they care about.
# ---------------------------------------------------------------------------


def _make_feed_info() -> FeedInfo:
    return FeedInfo(
        feed_info_id=_FEED_ID,  # self-referential; SQLite won't enforce
        feed_publisher_name="MBTA",
        feed_publisher_url="https://www.mbta.com",
        feed_lang="en",
        feed_start_date=date(2024, 1, 1),
        feed_end_date=date(2024, 12, 31),
        feed_version="2024-01-01T00:00:00+00:00",
        retrieved_from_url="https://cdn.mbta.com/MBTA_GTFS.zip",
        zip_md5_checksum="deadbeefdeadbeef",
    )


def make_stop(
    stop_id: str,
    *,
    platform_name: str = "Shuttle Bay",
    stop_lat: float = 42.39,
    stop_lon: float = -71.12,
) -> Stop:
    """Build a Stop row.  ``platform_name`` defaults to contain 'Shuttle'."""
    return Stop(
        feed_info_id=_FEED_ID,
        stop_id=stop_id,
        stop_code=stop_id,
        stop_name=f"Stop {stop_id}",
        stop_desc="",
        platform_name=platform_name,
        stop_lat=stop_lat,
        stop_lon=stop_lon,
        zone_id="",
        parent_station="",
    )


def make_route_pattern(
    route_pattern_id: str,
    route_id: str,
    representative_trip_id: str,
    *,
    typicality: RoutePatternTypicality = RoutePatternTypicality.DIVERSION,
) -> RoutePattern:
    """Build a RoutePattern row."""
    return RoutePattern(
        feed_info_id=_FEED_ID,
        route_pattern_id=route_pattern_id,
        route_id=route_id,
        direction_id="0",
        route_pattern_name=f"Pattern for {route_id}",
        route_pattern_time_desc="",
        route_pattern_typicality=typicality,
        route_pattern_sort_order=0,
        representative_trip_id=representative_trip_id,
    )


def make_trip(trip_id: str, shape_id: str, route_id: str) -> Trip:
    """Build a Trip row."""
    return Trip(
        feed_info_id=_FEED_ID,
        route_id=route_id,
        service_id="service-weekday",
        trip_id=trip_id,
        trip_headsign="Park Street",
        trip_short_name="",
        direction_id="0",
        block_id="",
        shape_id=shape_id,
        start_time=25200,   # 07:00
        end_time=32400,     # 09:00
        stop_count=2,
    )


def make_shape_point(shape_id: str, lon: float, lat: float, sequence: int) -> ShapePoint:
    """Build a ShapePoint row."""
    return ShapePoint(
        feed_info_id=_FEED_ID,
        shape_id=shape_id,
        shape_pt_lat=lat,
        shape_pt_lon=lon,
        shape_pt_sequence=sequence,
    )


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------

# Shape polygon: a square in (lon, lat) space.
# Interior:  lon ∈ (−71.15, −71.10),  lat ∈ (42.37, 42.42)
# The two shuttle stops (−71.12 / 42.39) and (−71.11 / 42.38) both fall
# inside this square, which is useful for is_in_shape integration tests.
_SHAPE_ID = "shape-shuttle-alw-pkst"
_SHAPE_VERTICES = [
    (-71.15, 42.37),
    (-71.10, 42.37),
    (-71.10, 42.42),
    (-71.15, 42.42),
]


@pytest.fixture
def gtfs_session():
    """
    Yield a live SQLAlchemy Session backed by an in-memory SQLite database,
    seeded with the following shuttle GTFS data:

    Stops
    -----
    stop-1   platform_name="Shuttle Bay"   lat=42.39  lon=-71.12  (inside polygon)
    stop-2   platform_name="Shuttle Stop"  lat=42.38  lon=-71.11  (inside polygon)
    stop-3   platform_name="Bus Bay"       lat=42.00  lon=-71.00  (non-shuttle, filtered out)

    RoutePatterns
    -------------
    Shuttle-AlewifeParkSt-0-0   typicality=DIVERSION  → trip-shuttle-alw  (included)
    Red-1-0                     typicality=TYPICAL    → trip-red-1        (excluded)

    Trips
    -----
    trip-shuttle-alw   shape_id=shape-shuttle-alw-pkst
    (trip-red-1 is NOT seeded — representative_trip lookup will return None,
     exercising the None-guard in get_shuttle_shapes)

    ShapePoints
    -----------
    4 vertices forming a square polygon around the two shuttle stops.
    Ordered by shape_pt_sequence 0..3.
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        # FeedInfo must come first; every other row references feed_info_id=1.
        session.add(_make_feed_info())
        session.flush()  # assigns id=1

        # Stops
        session.add(make_stop("stop-1", platform_name="Shuttle Bay", stop_lat=42.39, stop_lon=-71.12))
        session.add(make_stop("stop-2", platform_name="Shuttle Stop", stop_lat=42.38, stop_lon=-71.11))
        session.add(make_stop("stop-3", platform_name="Bus Bay"))  # non-shuttle

        # Route patterns
        session.add(
            make_route_pattern(
                route_pattern_id="Shuttle-AlewifeParkSt-0-0",
                route_id="Shuttle-AlewifeParkSt",
                representative_trip_id="trip-shuttle-alw",
                typicality=RoutePatternTypicality.DIVERSION,
            )
        )
        session.add(
            make_route_pattern(
                route_pattern_id="Red-1-0",
                route_id="Red",
                representative_trip_id="trip-red-1",
                typicality=RoutePatternTypicality.TYPICAL,
            )
        )

        # Trip for the DIVERSION pattern only.
        # "trip-red-1" is intentionally absent to exercise the None guard in
        # get_shuttle_shapes() (representative_trip query returns None → skipped).
        session.add(make_trip("trip-shuttle-alw", _SHAPE_ID, "Shuttle-AlewifeParkSt"))

        # Shape points (square polygon)
        for seq, (lon, lat) in enumerate(_SHAPE_VERTICES):
            session.add(make_shape_point(_SHAPE_ID, lon, lat, seq))

        session.commit()
        yield session

    # Dispose the connection pool so SQLite releases file handles.
    engine.dispose()


# ---------------------------------------------------------------------------
# DynamoDB fixture (moto)
# ---------------------------------------------------------------------------

# Key schemas match the KeyConditionExpressions used in the production queries:
#   ScheduledServiceDaily : hash=routeId (S),  sort=date (S)
#   Ridership             : hash=lineId  (S),  sort=date (S)
#   ShuttleTravelTimes    : hash=routeId (S),  sort=name (S)
_DYNAMODB_TABLES = [
    {
        "TableName": "ScheduledServiceDaily",
        "KeySchema": [
            {"AttributeName": "routeId", "KeyType": "HASH"},
            {"AttributeName": "date", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "routeId", "AttributeType": "S"},
            {"AttributeName": "date", "AttributeType": "S"},
        ],
    },
    {
        "TableName": "Ridership",
        "KeySchema": [
            {"AttributeName": "lineId", "KeyType": "HASH"},
            {"AttributeName": "date", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "lineId", "AttributeType": "S"},
            {"AttributeName": "date", "AttributeType": "S"},
        ],
    },
    {
        "TableName": "ShuttleTravelTimes",
        "KeySchema": [
            {"AttributeName": "routeId", "KeyType": "HASH"},
            {"AttributeName": "name", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": [
            {"AttributeName": "routeId", "AttributeType": "S"},
            {"AttributeName": "name", "AttributeType": "S"},
        ],
    },
]


@pytest.fixture
def dynamodb_tables(monkeypatch):
    """
    Yield a dict of moto-backed DynamoDB Table objects keyed by table name.

    Tables created:
      ScheduledServiceDaily  hash=routeId / sort=date
      Ridership              hash=lineId  / sort=date
      ShuttleTravelTimes     hash=routeId / sort=name

    Fake AWS credentials are set so boto3 does not attempt real IAM validation.
    The mock_aws context intercepts all botocore HTTP calls for the fixture's
    lifetime, including calls made through module-level boto3 resources created
    before the mock started (botocore is patched at the transport layer).
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")

    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        tables = {}
        for spec in _DYNAMODB_TABLES:
            table = resource.create_table(BillingMode="PAY_PER_REQUEST", **spec)
            tables[spec["TableName"]] = table
        yield tables
