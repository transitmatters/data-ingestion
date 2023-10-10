import requests
import zipfile
import csv
import boto3
from datetime import date, datetime
from dataclasses import dataclass
from io import BytesIO, TextIOWrapper
from typing import Union, Tuple, List, Dict, Iterator

CSV_ZIP_URL = "https://www.arcgis.com/sharing/rest/content/items/d73ed67e4cc84a84b818ea2c5caef696/data"

EntryKey = Tuple[str, date]


@dataclass
class SpeedRestrictionEntry:
    id: str
    date: date
    line_id: str
    description: str
    direction: str
    reason: str
    from_stop_id: str
    to_stop_id: str
    reported: date
    speed_mph: int
    track_feet: int

    def to_json(self):
        return {
            "id": self.id,
            "date": self.date.isoformat(),
            "lineId": self.line_id,
            "description": self.description,
            "direction": self.direction,
            "reason": self.reason,
            "fromStopId": self.from_stop_id,
            "toStopId": self.to_stop_id,
            "reported": self.reported.isoformat(),
            "speedMph": self.speed_mph,
            "trackFeet": self.track_feet,
        }

    def entry_key(self) -> EntryKey:
        return (self.line_id, self.date)


def parse_restriction_row_to_entry(row: Dict[str, str]) -> Union[None, SpeedRestrictionEntry]:
    try:
        [from_stop_id, to_stop_id] = [s.strip() for s in row["Loc_GTFS_Stop_ID"].split("|")]
    except ValueError:
        from_stop_id = row["Loc_GTFS_Stop_ID"].strip()
        to_stop_id = None
    line_raw = row["Line"].replace("Line", "").strip()
    date = datetime.strptime(row["Calendar_Date"], "%Y-%m-%d").date()
    reported = datetime.strptime(row["Date_Restriction_Reported"], "%Y-%m-%d").date()
    speed_mph = int(row["Restriction_Speed_MPH"].replace("mph", "").strip())
    track_feet = int(float(row["Restriction_Distance_Feet"]))
    status = row["Restriction_Status"].lower()
    if "clear" in status:
        return None
    return SpeedRestrictionEntry(
        id=row["ID"],
        line_id=f"line-{line_raw}",
        reported=reported,
        date=date,
        speed_mph=speed_mph,
        track_feet=track_feet,
        from_stop_id=from_stop_id,
        to_stop_id=to_stop_id,
        description=row["Location_Description"],
        direction=row["Track_Direction"],
        reason=row["Restriction_Reason"],
    )


def bucket_entries_by_key(entries: Iterator[SpeedRestrictionEntry]) -> Dict[EntryKey, List[SpeedRestrictionEntry]]:
    buckets = {}
    for entry in entries:
        key = entry.entry_key()
        if key not in buckets:
            buckets[key] = []
        buckets[key].append(entry)
    return buckets


def csv_is_too_old(csv_file_name: str, max_lookback_months: Union[None, int]) -> bool:
    if not max_lookback_months:
        return False
    csv_date = datetime.strptime(csv_file_name[:7], "%Y-%m").date()
    print(csv_date)
    return (date.today() - csv_date).days > (1 + max_lookback_months) * 30


def load_speed_restriction_entries(max_lookback_days: Union[None, int]) -> Iterator[SpeedRestrictionEntry]:
    req = requests.get(CSV_ZIP_URL)
    zip_file = zipfile.ZipFile(BytesIO(req.content))
    for csv_file_name in zip_file.namelist():
        if not csv_file_name.endswith(".csv") or csv_is_too_old(csv_file_name, max_lookback_days):
            continue
        print(csv_file_name)
        csv_file = zip_file.open(csv_file_name)
        rows = csv.DictReader(TextIOWrapper(csv_file), delimiter=",")
        for row in rows:
            entry = parse_restriction_row_to_entry(row)
            if entry:
                yield entry


def update_speed_restrictions(max_lookback_months: Union[None, int]):
    entries = load_speed_restriction_entries(max_lookback_months)
    buckets = bucket_entries_by_key(entries)
    dynamodb = boto3.resource("dynamodb")
    SpeedRestrictions = dynamodb.Table("SpeedRestrictions")
    with SpeedRestrictions.batch_writer() as batch:
        for (line_id, current_date), entries in buckets.items():
            zones = [entry.to_json() for entry in entries]
            batch.put_item(
                Item={
                    "lineId": line_id,
                    "date": current_date.isoformat(),
                    "zones": {"zones": zones},
                }
            )


if __name__ == "__main__":
    update_speed_restrictions(max_lookback_months=None)
