from dataclasses import dataclass
from datetime import date

from tqdm import tqdm

from .queries import RidershipRow, query_ridership
from .util import date_from_string


@dataclass
class RidershipEntry:
    date: date
    ridership: float


RidershipByDate = dict[date, RidershipEntry]
RidershipByLineId = dict[str, RidershipByDate]


def _get_ridership_for_line_id(start_date: date, end_date: date, line_id: str) -> RidershipByDate:
    ridership_by_date: RidershipByDate = {}
    entries: list[RidershipRow] = query_ridership(
        start_date=start_date,
        end_date=end_date,
        line_id=line_id,
    )
    for entry in entries:
        date = date_from_string(entry["date"])
        ridership_by_date[date] = RidershipEntry(
            date=date,
            ridership=entry["count"],
        )
    return ridership_by_date


def ridership_by_line_id(start_date: date, end_date: date, line_ids: list[str]) -> RidershipByLineId:
    ridership_by_line_id: RidershipByLineId = {}
    for line_id in (progress := tqdm(line_ids)):
        progress.set_description(f"Loading ridership for {line_id}")
        entries_for_line_id = _get_ridership_for_line_id(
            start_date=start_date,
            end_date=end_date,
            line_id=line_id,
        )
        ridership_by_line_id[line_id] = entries_for_line_id
    return ridership_by_line_id
