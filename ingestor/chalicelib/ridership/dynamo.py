import boto3
from typing import Dict, List
from datetime import datetime

DYNAMO_TABLE_NAME = "Ridership"


def ingest_ridership_to_dynamo(entries_by_line_id: Dict[str, List[Dict]]):
    dynamodb = boto3.resource("dynamodb")
    Ridership = dynamodb.Table(DYNAMO_TABLE_NAME)
    with Ridership.batch_writer() as batch:
        for line_id, entries in entries_by_line_id.items():
            for entry in entries:
                dt = datetime.strptime(entry["date"], "%Y-%m-%d")
                batch.put_item(
                    Item={
                        "lineId": line_id,
                        "count": int(entry["count"]),
                        "date": entry["date"],
                        "timestamp": int(dt.timestamp()),
                    }
                )
