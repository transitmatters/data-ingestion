import json
from typing import Dict
from os import path
from mbta_gtfs_sqlite import MbtaGtfsArchive
from mbta_gtfs_sqlite.models import Route

LINES_JSON_PATH = path.join(path.dirname(__file__), "lines.json")

LinesIndex = Dict[str, str]


def get_lines_index(archive: MbtaGtfsArchive) -> LinesIndex:
    if path.exists(LINES_JSON_PATH):
        return json.loads(open(LINES_JSON_PATH, "r").read())
    feed = archive.get_feed_by_key(key="20220103")
    feed.use_compact_only()
    feed.download_or_build()
    session = feed.create_sqlite_session()
    routes = session.query(Route).all()
    lines_index = {}
    for route in routes:
        lines_index[route.route_id] = route.line_id
    with open(LINES_JSON_PATH, "w") as f:
        f.write(json.dumps(lines_index, indent=4))
    return lines_index
