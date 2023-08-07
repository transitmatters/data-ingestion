import requests
import zipfile
import csv
import boto3
from datetime import date, datetime
from dataclasses import dataclass
from io import BytesIO, TextIOWrapper
from typing import Union, Tuple, List, Dict, Iterator

CSV_URL = "https://opendata.arcgis.com/api/v3/datasets/d126b4ce6d764493a8ddd7b30822fa8d_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"


EntryKey = Tuple[str, date]


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
            "direction": self.direction,
            "bin": self.bin,
            "arrival_departure": self.arrival_departure,
            "num_predictions": self.num_predictions,
            "num_accurate_predictions": self.num_accurate_predictions,
        }

    def entry_key(self) -> EntryKey:
        return (self.weekly, self.route_id)


def parse_prediction_row_to_entry(row: Dict[str, str]) -> Union[None, PredictionAccuracyEntry]:
    weekly = datetime.strptime(row["weekly"], "%Y-%m-%d").date()
    arrival_departure = int(float(row["arrival_departure"]))
    num_predictions = int(float(row["num_predictions"]))
    num_accurate_predictions = int(float(row["num_accurate_predictions"]))
    return PredictionAccuracyEntry(
        weekly=weekly,
        mode=row["mode"],
        route_id=row["route_id"],
        direction=row["direction"],
        bin=row["bin"],
        arrival_departure=arrival_departure,
        num_predictions=num_predictions,
        num_accurate_predictions=num_accurate_predictions,
    )
