import boto3
from datetime import date
from boto3.dynamodb.conditions import Key
from dynamodb_json import json_util as ddb_json
from typing import TypedDict

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
    table = dynamodb.Table("ScheduledServiceDaily")
    date_condition = Key("date").between(start_date.isoformat(), end_date.isoformat())
    route_condition = Key("routeId").eq(route_id)
    condition = date_condition & route_condition
    response = table.query(KeyConditionExpression=condition)
    return ddb_json.loads(response["Items"])


def query_ridership(start_date: date, end_date: date, line_id: str) -> list[RidershipRow]:
    table = dynamodb.Table("Ridership")
    date_condition = Key("date").between(start_date.isoformat(), end_date.isoformat())
    line_condition = Key("lineId").eq(line_id)
    condition = date_condition & line_condition
    response = table.query(KeyConditionExpression=condition)
    return ddb_json.loads(response["Items"])
