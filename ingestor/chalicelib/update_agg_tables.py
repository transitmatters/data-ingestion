from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
from chalicelib import constants, dynamo, line_traversal



def get_monthly_table_update_start():
    yesterday = datetime.today() - timedelta(days=1)
    first_of_month = datetime(yesterday.year, yesterday.month, 1)
    return first_of_month


def get_weekly_table_update_start():
    yesterday = datetime.now() - timedelta(days=1)
    days_since_sunday = (yesterday.weekday() + 1) % 7
    most_recent_sunday = yesterday - timedelta(days=days_since_sunday)
    return most_recent_sunday


table_map = {
    "weekly": {
        "table_name": "WeeklySpeed",
        "delta": timedelta(days=7),
        "start_date": datetime.strptime("2016-01-10T08:00:00", constants.DATE_FORMAT),  # Start on first Sunday with data.
        "update_start": get_weekly_table_update_start()
    },
    "monthly": {
        "table_name": "MonthlySpeed",
        "delta": relativedelta(months=1),
        "start_date": datetime.strptime("2016-01-01T08:00:00", constants.DATE_FORMAT),  # Start on 1st of first month with data.
        "update_start": get_monthly_table_update_start()
    }
}


def update_tables(table_type):
    print(f"Updating {table_type} table")
    table = table_map[table_type]
    yesterday = datetime.now() - timedelta(days=1)
    tt_objects = []
    for line in constants.LINES:
        start = table["update_start"]
        params = {
            "line": line,
            "start_date": datetime.strftime(start, constants.DATE_FORMAT_BACKEND),
            "end_date": datetime.strftime(yesterday, constants.DATE_FORMAT_BACKEND),
        }
        data = dynamo.query_line_travel_times(params)
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
    dynamo.write_to_traversal_table(tt_objects, table["table_name"])
    print("Done")