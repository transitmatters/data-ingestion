from datetime import date, timedelta
from unittest import mock

import pytest

from ..gtfs.ingest import ingest_feeds, ingest_gtfs_feeds_to_dynamo_and_s3


def _feed(key: str, start: date, end: date, exists_remotely: bool = True):
    """A GtfsFeed stand-in that records which lifecycle calls it received."""
    feed = mock.MagicMock()
    feed.key = key
    feed.start_date = start
    feed.end_date = end
    feed.exists_locally.return_value = False
    feed.exists_remotely.return_value = exists_remotely
    # Cleanup removes this directory; keep it pointing nowhere unless a test sets it.
    feed.local_subdirectory = f"/nonexistent/gtfs-test/{key}"
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


@mock.patch("chalicelib.gtfs.ingest.ingest_feed_to_dynamo")
def test_published_feed_downloads_only_the_compact_db(mock_dynamo):
    """The full ~423MB gtfs.sqlite3 is never read here, so it should not be pulled."""
    today = date.today()
    feed = _feed("published", today - timedelta(days=10), today + timedelta(days=90))

    ingest_feeds(
        dynamodb=mock.MagicMock(),
        feeds=[feed],
        start_date=today - timedelta(days=3),
        end_date=today,
    )

    feed.use_compact_only.assert_called_once()
    feed.download_from_s3.assert_called_once()
    feed.build_locally.assert_not_called()
    feed.upload_to_s3.assert_not_called()
    mock_dynamo.assert_called_once()


@mock.patch("chalicelib.gtfs.ingest.ingest_feed_to_dynamo")
def test_missing_feed_builds_the_full_bundle(mock_dynamo):
    """use_compact_only() must NOT leak onto the build path -- it makes
    build_local_feed_entry delete the full DB before upload, which is the bug
    that left s3://tm-gtfs with compact-only bundles in the first place."""
    today = date.today()
    feed = _feed("missing", today - timedelta(days=10), today + timedelta(days=90), exists_remotely=False)

    ingest_feeds(
        dynamodb=mock.MagicMock(),
        feeds=[feed],
        start_date=today - timedelta(days=3),
        end_date=today,
    )

    feed.use_compact_only.assert_not_called()
    feed.build_locally.assert_called_once()
    feed.upload_to_s3.assert_called_once()


@mock.patch("chalicelib.gtfs.ingest.ingest_feed_to_dynamo")
def test_cached_compact_feed_is_not_downloaded_again(mock_dynamo):
    """A cache holding only the compact DB must count, or every backfill re-reads s3://tm-gtfs."""
    today = date.today()
    feed = _feed("cached", today - timedelta(days=10), today + timedelta(days=90))
    # Only the compact DB is on disk: the feed exists locally once narrowed to it
    feed.exists_locally.side_effect = lambda: feed.use_compact_only.called

    ingest_feeds(
        dynamodb=mock.MagicMock(),
        feeds=[feed],
        start_date=today - timedelta(days=3),
        end_date=today,
        keep_local_feeds=True,
    )

    feed.download_from_s3.assert_not_called()
    mock_dynamo.assert_called_once()


@pytest.mark.parametrize("keep_local_feeds, deleted", [(False, True), (True, False)])
@mock.patch("chalicelib.gtfs.ingest.ingest_feed_to_dynamo")
def test_feed_directory_is_removed_unless_kept(mock_dynamo, tmp_path, keep_local_feeds, deleted):
    """Cleanup must remove the build's data.zip and unzipped feed/, not just the sqlite files.

    The library's delete_locally() left those behind, filling a warm container's /tmp.
    """
    today = date.today()
    feed = _feed("built", today - timedelta(days=10), today + timedelta(days=90), exists_remotely=False)
    feed_dir = tmp_path / "built"
    (feed_dir / "feed").mkdir(parents=True)
    for name in ("data.zip", "gtfs.sqlite3", "gtfs_compact.sqlite3", "feed/stops.txt"):
        (feed_dir / name).write_text("x")
    feed.local_subdirectory = str(feed_dir)

    ingest_feeds(
        dynamodb=mock.MagicMock(),
        feeds=[feed],
        start_date=today - timedelta(days=3),
        end_date=today,
        keep_local_feeds=keep_local_feeds,
    )

    assert feed_dir.exists() != deleted


@mock.patch("chalicelib.gtfs.ingest.ingest_feed_to_dynamo")
def test_partial_build_is_cleaned_up(mock_dynamo, tmp_path):
    """A build that fails partway leaves files behind but no complete feed; clean those up too."""
    today = date.today()
    feed = _feed("partial", today - timedelta(days=10), today + timedelta(days=90), exists_remotely=False)
    feed_dir = tmp_path / "partial"
    feed_dir.mkdir()
    (feed_dir / "data.zip").write_text("x")
    feed.local_subdirectory = str(feed_dir)
    feed.build_locally.side_effect = OSError("No space left on device")

    with pytest.raises(RuntimeError, match="partial"):
        ingest_feeds(
            dynamodb=mock.MagicMock(),
            feeds=[feed],
            start_date=today - timedelta(days=3),
            end_date=today,
        )

    assert not feed_dir.exists()


@mock.patch("chalicelib.gtfs.ingest.ingest_feeds")
@mock.patch("chalicelib.gtfs.ingest.MbtaGtfsArchive")
def test_unlisted_feed_key_raises_a_clear_error(mock_archive, mock_ingest_feeds):
    """MBTA's archive list can lag the live feed, so an enqueued key may not resolve yet.

    This used to surface as an AttributeError on None; it should name the missing key.
    """
    mock_archive.return_value.get_feed_by_key.return_value = None

    with pytest.raises(LookupError, match="20261020"):
        ingest_gtfs_feeds_to_dynamo_and_s3(feed_key="20261020", boto3_session=mock.MagicMock())

    mock_ingest_feeds.assert_not_called()
