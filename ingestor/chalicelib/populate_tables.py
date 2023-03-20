import numpy as np
from chalicelib import update_agg_tables, constants, dynamo
from datetime import datetime, timedelta

def populate_table(line, table_type):
    print(f"Populating {table_type} table")
    table = update_agg_tables.table_map[table_type]
    current_date = table["start_date"]
    today = datetime.now()
    delta = table["delta"]
    current_batch_of_tt_objects = {}
    num_entries = 0
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
        current_batch_of_tt_objects[datetime.strftime(current_date, constants.DATE_FORMAT_BACKEND)] = {
            "median": tt,
            "count": count,
        }
        if num_entries > 23 or current_date + delta > today:
            print('uploading...')
            dynamo.write_to_traversal_table(list(current_batch_of_tt_objects.items()), line, table["table_name"])
            num_entries = 0
            current_batch_of_tt_objects = {}
        current_date += delta
        num_entries += 1
    print("Done")
