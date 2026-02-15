import os
from datetime import datetime, timedelta

from tqdm import tqdm

from .. import agg_speed_tables, constants, daily_speeds
from .ingest import get_date_ranges, ingest_trip_metrics

START_DATE = datetime.strptime(os.environ["BACKFILL_START_DATE"], "%Y-%m-%d").date()
END_DATE = datetime.strptime(os.environ["BACKFILL_END_DATE"], "%Y-%m-%d").date()
MAX_RANGE_SIZE = 90

if __name__ == "__main__":
    date_ranges = get_date_ranges(START_DATE, END_DATE, MAX_RANGE_SIZE)
    for start_date, end_date in (progress := tqdm(date_ranges)):
        progress.set_description(f"{start_date} to {end_date}...")
        ingest_trip_metrics(start_date, end_date)

    for d in tqdm(range((END_DATE - START_DATE).days + 1), desc="Updating daily speeds..."):
        current_date = START_DATE + timedelta(days=d)
        daily_speeds.update_daily_table(current_date)

    start_str = START_DATE.strftime("%Y-%m-%d")
    for line in tqdm(constants.LINES, desc="Rebuilding weekly/monthly aggregates..."):
        agg_speed_tables.populate_table(line, "weekly", start_str)
        agg_speed_tables.populate_table(line, "monthly", start_str)
