from decimal import Decimal
import numpy as np
from chalicelib import constants, dynamo
from datetime import datetime, timedelta

def populate_table(line, table_type):
    ''' Populate weekly or monthly aggregate speed table for a given line. Ran manually as a lambda in AWS console'''
    print(f"Populating {table_type} table")
    table = constants.TABLE_MAP[table_type]
    current_date = table["start_date"]
    delta = table["delta"]
    today = datetime.now()
    speed_objects = []
    # Increment date by range, fetching daily speeds from DailySpeed table and calculating p50.
    while current_date <= today:
        print(current_date)
        params = {
            "line": line,
            "start_date": datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND),
            "end_date": datetime.strftime(current_date + delta - timedelta(days=1), constants.DATE_FORMAT_BACKEND),
        }
        data = get_daily_speeds(params)
        if len(data) == 0:
            current_date += delta
            continue
        tt = np.percentile(np.array([float(entry["value"]) for entry in data]), 50)
        count = np.percentile(np.array([int(entry["count"]) for entry in data]), 50)
        speed_objects.append({
            "line": line,
            "date": datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND),
            "value": Decimal(tt),
            "count": Decimal(count),
        })
        current_date += delta
    dynamo.dynamo_batch_write(speed_objects, table["table_name"])
    print("Done")


def get_daily_speeds(params):
    ''' Format and send query for DailySpeed table'''
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
    return dynamo.query_dynamo(query_params, "DailySpeed")


def update_tables(table_type):
    ''' Update weekly and monthly speed tables '''
    print(f"Updating {table_type} table")
    table = constants.TABLE_MAP[table_type]
    yesterday = datetime.now() - timedelta(days=1)
    speed_objects = []
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
        # Calculate p50 speed and number of trips.
        median_speed = np.percentile(np.array([float(entry["value"]) for entry in data]), 50)
        count = np.percentile(np.array([int(entry["count"]) for entry in data]), 50) 
        table_input = {
                "line": line,
                "date": datetime.strftime(start, constants.DATE_FORMAT_BACKEND),
                "value": Decimal(median_speed),
                "count": Decimal(count),
        }
        speed_objects.append(table_input)
    dynamo.dynamo_batch_write(speed_objects, table["table_name"])
    print("Done")