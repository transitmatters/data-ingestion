import boto3
import json
from typing import List


def enqueue_feed_keys_to_sqs(feed_keys: List[str], force_rebuild_feeds: bool = False) -> None:
    """Send GTFS feed keys to an SQS queue for asynchronous ingestion.

    Args:
        feed_keys: List of GTFS feed key strings to enqueue.
        force_rebuild_feeds: If True, forces feeds to be rebuilt even if they
            already exist. Defaults to False.
    """
    sqs = boto3.resource("sqs")
    queue = sqs.get_queue_by_name(QueueName="gtfs-ingest-keys")
    for feed_key in feed_keys:
        queue.send_message(
            MessageBody=json.dumps(
                {
                    "feed_key": feed_key,
                    "force_rebuild_feeds": force_rebuild_feeds,
                }
            )
        )
