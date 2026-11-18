from datetime import date, timedelta

from .. import constants
from ..trip_metrics.ingest import generate_requests, get_date_ranges, is_plausible

POST_GLX = date(2026, 9, 1)
PRE_GLX = date(2023, 1, 9)


def _route_ids(reqs):
    return {req.route_id for req in reqs}


def test_generate_requests_covers_all_routes():
    reqs = generate_requests(POST_GLX, date(2026, 9, 7))
    assert _route_ids(reqs) == {
        "line-red-a",
        "line-red-b",
        "line-orange",
        "line-blue",
        "line-green-b",
        "line-green-c",
        "line-green-d",
        "line-green-e",
        "line-mattapan",
    }
    # 2 directions x inclusive/exclusive per route
    assert len(reqs) == 9 * 4


def test_generate_requests_routes_filter():
    reqs = generate_requests(POST_GLX, date(2026, 9, 7), routes=[("line-mattapan", None)])
    assert _route_ids(reqs) == {"line-mattapan"}
    assert len(reqs) == 4


# Terminal-to-terminal pairs validated against the aggregate travel times API.
# Swapping arrival/departure platforms here makes the API return no data and silently drops the line.
def test_green_post_glx_inclusive_stops():
    stops = {r: constants.get_route_metadata("line-green", POST_GLX, True, r)["stops"] for r in "bcde"}
    assert stops == {
        "b": [[70106, 70201], [70202, 70107]],
        "c": [[(70237, 70238), 70201], [70202, (70237, 70238)]],
        "d": [[(70160, 70161), (70503, 70504)], [(70503, 70504), (70160, 70161)]],
        "e": [[70260, 70511], [70512, 70260]],
    }


def test_green_pre_glx_inclusive_stops():
    assert constants.get_route_metadata("line-green", PRE_GLX, True, "b")["stops"] == [
        [70106, 70200],
        [(70196, 70197, 70198, 70199), 70107],
    ]


def test_mattapan_inclusive_matches_exclusive_direction():
    inclusive = constants.get_route_metadata("line-mattapan", POST_GLX, True)["stops"]
    exclusive = constants.get_route_metadata("line-mattapan", POST_GLX, False)["stops"]
    # Direction 0 runs Mattapan -> Ashmont in both
    assert inclusive == [[70276, 70261], [70261, 70275]]
    assert exclusive == [[70274, 70264], [70263, 70273]]


def test_green_pre_glx_c_inclusive_stops():
    assert constants.get_route_metadata("line-green", PRE_GLX, True, "c")["stops"] == [
        [(70237, 70238), 70205],
        [70206, (70237, 70238)],
    ]


def test_is_plausible():
    exclusive = {"dir_0_exclusive_mean": 2900.0}
    assert is_plausible(exclusive, {"dir_0_inclusive_mean": 3100.0}, "0")
    assert not is_plausible(exclusive, {"dir_0_inclusive_mean": 160.0}, "0")


def _assert_contiguous_cover(ranges, start, end):
    assert ranges[0][0] == start
    assert ranges[-1][1] == end
    for s, e in ranges:
        assert s <= e, f"backwards range {s} -> {e}"
    for (_, prev_end), (next_start, _) in zip(ranges, ranges[1:]):
        assert next_start == prev_end + timedelta(days=1)


def test_get_date_ranges_one_day():
    d = date(2026, 9, 1)
    assert get_date_ranges(d, d, 50) == [(d, d)]
    assert len(generate_requests(d, d, routes=[("line-mattapan", None)])) == 4


def test_get_date_ranges_chunk_size():
    ranges = get_date_ranges(date(2026, 1, 1), date(2026, 1, 25), 10)
    assert ranges == [
        (date(2026, 1, 1), date(2026, 1, 10)),
        (date(2026, 1, 11), date(2026, 1, 20)),
        (date(2026, 1, 21), date(2026, 1, 25)),
    ]


def test_get_date_ranges_splits_at_glx():
    glx = constants.GLX_EXTENSION_DATE
    start, end = glx - timedelta(days=5), glx + timedelta(days=5)
    ranges = get_date_ranges(start, end, 50, [glx])
    assert ranges == [(start, glx - timedelta(days=1)), (glx, end)]


def test_get_date_ranges_chunk_starting_on_glx_is_not_backwards():
    """Repro from #270: 90-day chunks from 2022-12-19 start a chunk exactly on 2023-03-19."""
    glx = constants.GLX_EXTENSION_DATE
    start, end = date(2022, 12, 19), date(2023, 9, 1)
    ranges = get_date_ranges(start, end, 90, [glx])
    _assert_contiguous_cover(ranges, start, end)
    assert any(s == glx for s, _ in ranges)
    assert not any(s < glx <= e for s, e in ranges)


def test_get_date_ranges_empty_when_backwards():
    assert get_date_ranges(date(2026, 1, 2), date(2026, 1, 1), 10) == []
