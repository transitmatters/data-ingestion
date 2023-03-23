from decimal import Decimal
import numpy as np
from chalicelib import update_agg_tables, constants, dynamo
from datetime import datetime, timedelta

def populate_table(line, table_type):
    print(f"Populating {table_type} table")
    table = update_agg_tables.table_map[table_type]
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
        data = dynamo.query_line_travel_times(params)
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
    dynamo.write_to_traversal_table(tt_objects, table["table_name"])
    print("Done")
