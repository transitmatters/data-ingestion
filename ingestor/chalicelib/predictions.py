import csv
import io
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, Iterator, List, Tuple, Union

import boto3
import requests

CSV_URL = "https://massdot.maps.arcgis.com/sharing/rest/content/items/155ab68df00145cabddfb90377201b0e/data"


EntryKey = Tuple[date, str]


@dataclass
class PredictionAccuracyEntry:
    weekly: date
    mode: str
    route_id: str
    bin: str
    arrival_departure: str
    num_predictions: str
    num_accurate_predictions: str

    def to_json(self):
        return {
            "weekly": self.weekly.isoformat(),
            "mode": self.mode,
            "route_id": self.route_id,
            "bin": self.bin,
            "arrival_departure": self.arrival_departure,
            "num_predictions": self.num_predictions,
            "num_accurate_predictions": self.num_accurate_predictions,
        }

    def entry_key(self) -> EntryKey:
        return (self.weekly, self.route_id)


def bucket_entries_by_key(entries: Iterator[PredictionAccuracyEntry]) -> Dict[EntryKey, List[PredictionAccuracyEntry]]:
    buckets = {}
    for entry in entries:
        key = entry.entry_key()
        if key not in buckets:
            buckets[key] = []
        buckets[key].append(entry)
    return buckets


def parse_prediction_row_to_entry(row: Dict[str, str]) -> Union[None, PredictionAccuracyEntry]:
    weekly = datetime.strptime(row["weekly"][:10], "%Y-%m-%d").date()
    num_predictions = int(row["num_predictions"])
    num_accurate_predictions = int(row["num_accurate_predictions"])
    # Bus routeId is "", use mode when routeId isn't present
    route_id = row["route_id"] if row["route_id"] != "" else row["mode"]

    return PredictionAccuracyEntry(
        weekly=weekly,
        mode=row["mode"],
        route_id=route_id,
        bin=row["bin"],
        arrival_departure=row["arrival_departure"],
        num_predictions=num_predictions,
        num_accurate_predictions=num_accurate_predictions,
    )


def load_prediction_entries() -> Iterator[PredictionAccuracyEntry]:
    req = requests.get(CSV_URL)

    # Weirdly the csv starts with 3 strange chars
    rows = csv.DictReader(io.StringIO(req.text), delimiter=",")

    for row in rows:
        entry = parse_prediction_row_to_entry(row)
        if entry:
            yield entry


def update_predictions():
    entries = load_prediction_entries()
    buckets = bucket_entries_by_key(entries)
    dynamodb = boto3.resource("dynamodb")
    TimePredictions = dynamodb.Table("TimePredictions")
    with TimePredictions.batch_writer() as batch:
        for (weekly, route_id), entries in buckets.items():
            prediction = [entry.to_json() for entry in entries]
            batch.put_item(Item={"routeId": route_id, "week": weekly.isoformat(), "prediction": prediction})


if __name__ == "__main__":
    update_predictions()
