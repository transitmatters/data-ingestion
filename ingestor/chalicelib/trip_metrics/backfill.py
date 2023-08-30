from datetime import date
from tqdm import tqdm

from .ingest import ingest_trip_metrics, get_date_ranges

START_DATE = date(2023, 8, 29)
END_DATE = date(2023, 8, 30)
MAX_RANGE_SIZE = 90

if __name__ == "__main__":
    date_ranges = get_date_ranges(START_DATE, END_DATE, MAX_RANGE_SIZE)
    for start_date, end_date in (progress := tqdm(date_ranges)):
        progress.set_description(f"{start_date} to {end_date}...")
        ingest_trip_metrics(start_date, end_date)
