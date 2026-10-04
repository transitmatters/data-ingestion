from datetime import date

from .. import constants
from ..trip_metrics.ingest import generate_requests

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
