from datetime import date, timedelta
from decimal import Decimal
import json
import sys
import pandas as pd

from .segment_speed import StopPair, format_tt_df, get_aggregate_data_dates, pair_from_fullpair, get_stop_pairs

from ..parallel import make_parallel
from .. import dynamo
from .constants import COLORS

"""
To run when backfilling long periods of data

poetry run python -m ingestor.chalicelib.speeds.backfill [first_date]

Example:
poetry run python -m ingestor.chalicelib.speeds.backfill 2025-01-01

It will run until TODAY

If running from 2016, will take close to 1 hour
"""


def get_date_chunks(start: str, end: str, delta: int):
    interval = (end - start).days + 1
    cur = start
    while interval != 0:
        inc = min(interval, delta)
        yield (cur, cur + timedelta(days=inc - 1))
        interval -= inc
        cur += timedelta(days=inc)


def _gather_tts(date_range, pair: StopPair):
    (start, stop) = date_range
    return get_aggregate_data_dates(pair, start, stop, verbose=True, raw=True)


gather_tts = make_parallel(
    _gather_tts, THREAD_COUNT=10
)  # not as ambitious as it sounds, since we're mostly just waiting for the server.


def backfill_daily_median_travel_time(start: date, end: date):
    pairs = [(color, pair) for color in COLORS for pair in get_stop_pairs(color)]

    row_dicts = pd.concat(
        [
            format_tt_df(
                pd.DataFrame.from_records(gather_tts(get_date_chunks(start, end, delta=240), pair_from_fullpair(o[1]))),
                o[0],
                pair_from_fullpair(o[1]),
            )
            for o in pairs
        ]
    ).to_dict(orient="records")

    unique_dict = {(item["date_stop_pair"], item["route"]): item for item in row_dicts}
    row_dicts = list(unique_dict.values())

    dynamo.dynamo_batch_write(json.loads(json.dumps(row_dicts), parse_float=Decimal), "SegmentTravelTimes")


def main(args=sys.argv):
    if len(args) > 1:
        start = date.fromisoformat(args[1])
    else:
        start = date(2016, 1, 15)

    end = date.today()
    backfill_daily_median_travel_time(start, end)


if __name__ == "__main__":
    main()
