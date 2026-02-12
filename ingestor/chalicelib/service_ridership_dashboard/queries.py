from datetime import date
from typing import TypedDict

import boto3
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util as ddb_json

# Create a DynamoDB resource
dynamodb = boto3.resource("dynamodb")


class ByHour(TypedDict):
    totals: list[int]


class ScheduledServiceRow(TypedDict):
    routeId: str
    date: str
    byHour: ByHour
    count: int
    hasServiceExceptions: bool
    lineId: str
    serviceMinutes: int
    timestamp: int


class RidershipRow(TypedDict):
    lineId: str
    count: int
    date: str
    timestamp: int


def query_scheduled_service(start_date: date, end_date: date, route_id: str) -> list[ScheduledServiceRow]:
    """Query the ScheduledServiceDaily DynamoDB table for a route within a date range.

    Args:
        start_date: The start date of the query range.
        end_date: The end date of the query range.
        route_id: The MBTA route identifier to query.

    Returns:
        A list of ScheduledServiceRow dicts from DynamoDB.
    """
    table = dynamodb.Table("ScheduledServiceDaily")
    date_condition = Key("date").between(start_date.isoformat(), end_date.isoformat())
    route_condition = Key("routeId").eq(route_id)
    condition = date_condition & route_condition
    response = table.query(KeyConditionExpression=condition)
    return ddb_json.loads(response["Items"])


def query_ridership(start_date: date, end_date: date, line_id: str) -> list[RidershipRow]:
    """Query the Ridership DynamoDB table for a line within a date range.

    Args:
        start_date: The start date of the query range.
        end_date: The end date of the query range.
        line_id: The MBTA line identifier to query.

    Returns:
        A list of RidershipRow dicts from DynamoDB.
    """
    table = dynamodb.Table("Ridership")
    date_condition = Key("date").between(start_date.isoformat(), end_date.isoformat())
    line_condition = Key("lineId").eq(line_id)
    condition = date_condition & line_condition
    response = table.query(KeyConditionExpression=condition)
    return ddb_json.loads(response["Items"])
