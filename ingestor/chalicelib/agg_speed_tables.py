from decimal import Decimal
import numpy as np
from chalicelib import constants, dynamo
from datetime import datetime, timedelta

def populate_table(line, table_type):
    print(f"Populating {table_type} table")
    table = constants.TABLE_MAP[table_type]
    current_date = table["start_date"]
    today = datetime.now()
    delta = table["delta"]
    tt_objects = []
    while current_date <= today:
        print(current_date)
        params = {
            "line": line,
            "start_date": datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND),
            "end_date": datetime.strftime(current_date + delta - timedelta(days=1), constants.DATE_FORMAT_BACKEND),
        }
        data = dynamo.query_daily_speeds(params)
        if len(data) == 0:
            current_date += delta
            continue
        tt = np.percentile(np.array([float(entry["value"]) for entry in data]), 50)
        count = np.percentile(np.array([int(entry["count"]) for entry in data]), 50)
        tt_objects.append({
            "line": line,
            "date": datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND),
            "value": Decimal(tt),
            "count": Decimal(count),
        })
        current_date += delta
    dynamo.dynamo_batch_write(tt_objects, table["table_name"])
    print("Done")


def get_daily_speeds(params):
    query_params = {
        'KeyConditionExpression': '#pk = :pk and #date BETWEEN :start_date and :end_date',
        'ExpressionAttributeNames': {
            '#pk': 'line',
            '#date': 'date'
        },
        'ExpressionAttributeValues': {
            ':pk': params["line"],
            ':start_date': params["start_date"],
            ':end_date': params["end_date"]
        }
    }
    return dynamo.query_dynamo(query_params, "DailySpeeds")


def update_tables(table_type):
    print(f"Updating {table_type} table")
    table = constants.TABLE_MAP[table_type]
    yesterday = datetime.now() - timedelta(days=1)
    tt_objects = []
    for line in constants.LINES:
        start = table["update_start"]
        params = {
            "line": line,
            "start_date": datetime.strftime(start, constants.DATE_FORMAT_BACKEND),
            "end_date": datetime.strftime(yesterday, constants.DATE_FORMAT_BACKEND),
        }
        data = get_daily_speeds(params)
        if len(data) == 0:
            print("No data.")
            return
        tt = np.percentile(np.array([float(entry["value"]) for entry in data]), 50)
        count = np.percentile(np.array([int(entry["count"]) for entry in data]), 50) 
        table_input = {
                "line": line,
                "date": datetime.strftime(start, constants.DATE_FORMAT_BACKEND),
                "value": tt,
                "count": count,
        }
        tt_objects.append(table_input)
    dynamo.dynamo_batch_write(tt_objects, table["table_name"])
    print("Done")