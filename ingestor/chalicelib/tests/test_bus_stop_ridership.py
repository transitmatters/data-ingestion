import io
import zipfile
from tempfile import NamedTemporaryFile

import pytest

from ..ridership.bus_stops import (
    build_route_files,
    format_stop_name,
    get_season_name,
    merge_stop_orders,
    read_season_csv,
)

# Up to Fall 2023 (and Spring 2024)
OLD_HEADER = (
    "season,route_id,route_variant,direction_id,trip_start_time,day_type_id,day_type_name,stop_name,stop_id,"
    "stop_sequence,boardings,alightings,load_,sample_size"
)
# Fall 2024, which starts with a byte order mark
FALL_2024_HEADER = (
    "﻿mode,season,year,route_id,route_variant,direction_id,day_type_id,day_type_name,trip_start_time,"
    "time_period_id,time_period_name,stop_name,stop_id,stop_sequence,boardings,alightings,load,sample_count"
)
# Fall 2025
FALL_2025_HEADER = (
    '"GTFS route_id","GTFS direction_id","trip start time","Stop Name","GTFS stop_id","stop sequence","Year",'
    '"Day Type","Boardings","Alightings","Load","Route/Variant","# of Trip Samples "'
)


def old_row(route, variant, direction, time, day_type, stop_name, stop_id, sequence, boardings, alightings):
    return (
        f"Fall 2019,{route},{variant},{direction},{time},day_type_01,{day_type},{stop_name},{stop_id},{sequence},"
        f"{boardings},{alightings},0,10"
    )


def fall_2025_row(route, direction, time, stop_name, stop_id, sequence, day_type, boardings, alightings, variant):
    return (
        f'{route},{direction},"{time}","{stop_name}",{stop_id},{sequence},"Fall 2025","{day_type}",'
        f"{boardings},{alightings},0,{variant},10"
    )


def read_csv_text(text):
    return read_season_csv(io.BytesIO(text.encode("utf-8")))


def write_zip(files):
    path = NamedTemporaryFile(suffix=".zip", delete=False).name
    with zipfile.ZipFile(path, "w") as archive:
        for name, text in files.items():
            archive.writestr(f"MBTA_Bus_Ridership_by_Trip_Season_Route_Line_and_Stop/{name}", text)
    return path


def test_get_season_name_keeps_only_fall_seasons():
    assert get_season_name("dir/MBTA_Bus_Ridership_by_Trip_Season_Route_Line_and_Stop_Fall_2025.csv") == "Fall 2025"
    assert get_season_name("dir/MBTA_Bus_Ridership_by_Trip_Season_Route_Line_and_Stop_Spring_2024.csv") is None
    assert get_season_name("dir/") is None


def test_format_stop_name():
    assert format_stop_name("WASHINGTON ST OPP RUGGLES ST ") == "Washington St opp Ruggles St"
    assert format_stop_name("MASSACHUSETTS AVE @ TROWBRIDG") == "Massachusetts Ave @ Trowbridg"


def test_read_season_csv_reads_each_schema_and_keeps_weekdays():
    old = read_csv_text(
        "\n".join(
            [
                OLD_HEADER,
                old_row("1", "1-0-0", 0, "05:00:00", "weekday", "A", 10, 1, 2.5, 0),
                old_row("1", "1-0-0", 0, "05:00:00", "saturday", "A", 10, 1, 9, 0),
            ]
        )
    )
    assert list(old["weight"]) == [1.0]
    assert old.iloc[0]["stop_id"] == "10"
    assert old.iloc[0]["boardings"] == 2.5

    fall_2024 = read_csv_text(
        "\n".join(
            [
                FALL_2024_HEADER,
                "3,Fall,2024,1,1-_-0,0,day_type_01,weekday,4:37:00,time_period_01,EARLY,NUBIAN STATION,64,0,13.8,0,13.8,69",
                "3,Fall,2024,1,1-_-0,0,day_type_03,sunday,4:37:00,time_period_01,EARLY,NUBIAN STATION,64,0,5,0,5,10",
            ]
        )
    )
    assert list(fall_2024["route_id"]) == ["1"]
    assert list(fall_2024["stop_name"]) == ["NUBIAN STATION"]

    fall_2025 = read_csv_text(
        "\n".join(
            [
                FALL_2025_HEADER,
                fall_2025_row(1, 1, "05:06:00", "A", 110, 0, "Wkdy", 1, 0, 1),
                fall_2025_row(1, 1, "05:06:00", "A", 110, 0, "Fri", 1, 0, 1),
                fall_2025_row(1, 1, "05:06:00", "A", 110, 0, "Sat", 1, 0, 1),
            ]
        )
    )
    assert list(fall_2025["weight"]) == pytest.approx([0.8, 0.2])


def test_read_season_csv_rejects_unknown_schema():
    with pytest.raises(ValueError, match="missing columns"):
        read_csv_text("route_id,stop_id\n1,2")


def test_merge_stop_orders_places_short_turns_and_deviations_among_their_neighbors():
    main = ["a", "b", "c", "d", "e"]
    assert merge_stop_orders([main, ["b", "c", "d"]]) == main
    # A deviation between b and d
    assert merge_stop_orders([main, ["a", "b", "x", "d", "e"]]) == ["a", "b", "x", "c", "d", "e"]
    # An extension past either end
    assert merge_stop_orders([main, ["w", "a", "b"]]) == ["w", "a", "b", "c", "d", "e"]
    assert merge_stop_orders([main, ["d", "e", "f"]]) == ["a", "b", "c", "d", "e", "f"]
    # A variant with no stops in common goes on the end
    assert merge_stop_orders([main, ["y", "z"]]) == main + ["y", "z"]


def test_build_route_files_totals_each_stop_in_route_order():
    fall_2019 = "\n".join(
        [
            OLD_HEADER,
            # The main variant runs two trips: stops 1, 2, 3
            old_row("28", "28-0-1", 1, "05:00:00", "weekday", "MATTAPAN STATION", 1, 1, 10, 0),
            old_row("28", "28-0-1", 1, "05:00:00", "weekday", "BLUE HILL AVE OPP WOODHAVEN", 2, 2, 4, 1),
            old_row("28", "28-0-1", 1, "05:00:00", "weekday", "RUGGLES", 3, 3, 0, 13),
            old_row("28", "28-0-1", 1, "06:00:00", "weekday", "MATTAPAN STATION", 1, 1, 20, 0),
            old_row("28", "28-0-1", 1, "06:00:00", "weekday", "BLUE HILL AVE OPP WOODHAVEN", 2, 2, 6, 2),
            old_row("28", "28-0-1", 1, "06:00:00", "weekday", "RUGGLES", 3, 3, 0, 24),
            # A short turn that skips stop 2 for stop 9
            old_row("28", "28-1-1", 1, "07:00:00", "weekday", "MATTAPAN STATION", 1, 1, 5, 0),
            old_row("28", "28-1-1", 1, "07:00:00", "weekday", "DETOUR", 9, 2, 1, 6),
            # Weekend trips aren't counted
            old_row("28", "28-0-1", 1, "07:00:00", "saturday", "MATTAPAN STATION", 1, 1, 100, 0),
        ]
    )
    fall_2025 = "\n".join(
        [
            FALL_2025_HEADER,
            fall_2025_row(28, 1, "05:00:00", "MATTAPAN STATION", 1, 0, "Wkdy", 10, 0, 1),
            fall_2025_row(28, 1, "05:00:00", "MATTAPAN STATION", 1, 0, "Fri", 20, 0, 1),
            fall_2025_row(28, 0, "05:00:00", "RUGGLES", 3, 0, "Wkdy", 7, 0, 2),
        ]
    )
    path = write_zip(
        {
            "MBTA_Bus_Ridership_by_Trip_Season_Route_Line_and_Stop_Fall_2025.csv": fall_2025,
            "MBTA_Bus_Ridership_by_Trip_Season_Route_Line_and_Stop_Fall_2019.csv": fall_2019,
            "MBTA_Bus_Ridership_by_Trip_Season_Route_Line_and_Stop_Spring_2024.csv": fall_2019,
        }
    )

    routes = build_route_files(path, {"1": "Mattapan Station"})

    assert list(routes) == ["28"]
    route = routes["28"]
    assert [season["season"] for season in route["seasons"]] == ["Fall 2019", "Fall 2025"]
    assert route["seasons"][0]["directions"] == [
        {
            "direction_id": 1,
            "stops": [
                {"stop_id": "1", "boardings": 35.0, "alightings": 0.0},
                # The short turn's stop goes right after the stop before it on that variant
                {"stop_id": "9", "boardings": 1.0, "alightings": 6.0},
                {"stop_id": "2", "boardings": 10.0, "alightings": 3.0},
                {"stop_id": "3", "boardings": 0.0, "alightings": 37.0},
            ],
        }
    ]
    # Four Monday-to-Thursday days and one Friday
    fall_2025_directions = route["seasons"][1]["directions"]
    assert [direction["direction_id"] for direction in fall_2025_directions] == [0, 1]
    assert fall_2025_directions[1]["stops"] == [{"stop_id": "1", "boardings": 12.0, "alightings": 0.0}]
    # GTFS names come first, then the CSV's
    assert route["stop_names"] == {
        "1": "Mattapan Station",
        "2": "Blue Hill Ave opp Woodhaven",
        "3": "Ruggles",
        "9": "Detour",
    }
