import os
from datetime import datetime, timedelta

from tqdm import tqdm

from .. import agg_speed_tables, constants, daily_speeds
from .ingest import get_date_ranges, ingest_trip_metrics

START_DATE = datetime.strptime(os.environ["BACKFILL_START_DATE"], "%Y-%m-%d").date()
END_DATE = datetime.strptime(os.environ["BACKFILL_END_DATE"], "%Y-%m-%d").date()
BACKFILL_LINE = os.environ.get("BACKFILL_LINE")  # e.g. "line-orange", defaults to all lines
MAX_RANGE_SIZE = 90

lines = [BACKFILL_LINE] if BACKFILL_LINE else constants.LINES
routes = [r for r in constants.ALL_ROUTES if not BACKFILL_LINE or r[0] == BACKFILL_LINE]

if __name__ == "__main__":
    if BACKFILL_LINE:
        print(f"Backfilling single line: {BACKFILL_LINE}")
    else:
        print("Backfilling all lines")

    date_ranges = get_date_ranges(START_DATE, END_DATE, MAX_RANGE_SIZE)
    for start_date, end_date in (progress := tqdm(date_ranges)):
        progress.set_description(f"{start_date} to {end_date}...")
        ingest_trip_metrics(start_date, end_date)

    for d in tqdm(range((END_DATE - START_DATE).days + 1), desc="Updating daily speeds..."):
        current_date = START_DATE + timedelta(days=d)
        daily_speeds.update_daily_table(current_date, routes=routes)

    start_str = START_DATE.strftime("%Y-%m-%d")
    for line in tqdm(lines, desc="Rebuilding weekly/monthly aggregates..."):
        agg_speed_tables.populate_table(line, "weekly", start_str)
        agg_speed_tables.populate_table(line, "monthly", start_str)
