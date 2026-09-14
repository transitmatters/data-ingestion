from datetime import date, timedelta
from unittest import mock

import pytest

from ..gtfs.ingest import ingest_feeds


def _feed(key: str, start: date, end: date, exists_remotely: bool = True):
    """A GtfsFeed stand-in that records which lifecycle calls it received."""
    feed = mock.MagicMock()
    feed.key = key
    feed.start_date = start
    feed.end_date = end
    feed.exists_locally.return_value = False
    feed.exists_remotely.return_value = exists_remotely
    return feed


@mock.patch("chalicelib.gtfs.ingest.ingest_feed_to_dynamo")
def test_future_feed_is_built_but_not_ingested(mock_dynamo):
    """A feed selected by the forward-looking window has no elapsed service dates.

    date_range() asserts start <= end, so without a guard this raised -- and since
    failures now propagate, it would have failed the whole invocation.
    """
    today = date.today()
    feed = _feed("future", today + timedelta(days=5), today + timedelta(days=90))

    ingest_feeds(
        dynamodb=mock.MagicMock(),
        feeds=[feed],
        start_date=today - timedelta(days=3),
        end_date=today + timedelta(days=14),
    )

    feed.download_from_s3.assert_called_once()
    mock_dynamo.assert_not_called()


@mock.patch("chalicelib.gtfs.ingest.ingest_feed_to_dynamo")
def test_active_feed_is_ingested_up_to_today(mock_dynamo):
    """An active feed still ingests, clamped to dates that have actually happened."""
    today = date.today()
    feed = _feed("active", today - timedelta(days=10), today + timedelta(days=90))

    ingest_feeds(
        dynamodb=mock.MagicMock(),
        feeds=[feed],
        start_date=today - timedelta(days=3),
        end_date=today + timedelta(days=14),
    )

    mock_dynamo.assert_called_once()
    _, _, start, end = mock_dynamo.call_args[0]
    assert start == today - timedelta(days=3)
    assert end == today, "must not write service levels for dates that have not happened"


@mock.patch("chalicelib.gtfs.ingest.ingest_feed_to_dynamo")
def test_failures_are_collected_and_reraised(mock_dynamo):
    """One bad feed must not block the rest, but the invocation must still fail."""
    today = date.today()
    good = _feed("good", today - timedelta(days=10), today + timedelta(days=90))
    bad = _feed("bad", today - timedelta(days=10), today + timedelta(days=90))
    bad.download_from_s3.side_effect = RuntimeError("s3 exploded")

    with pytest.raises(RuntimeError, match="bad"):
        ingest_feeds(
            dynamodb=mock.MagicMock(),
            feeds=[bad, good],
            start_date=today - timedelta(days=3),
            end_date=today,
        )

    mock_dynamo.assert_called_once()  # the good feed still processed
