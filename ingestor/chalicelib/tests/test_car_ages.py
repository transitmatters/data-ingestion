from ..car_ages import get_car_build_year


# --- get_car_build_year ---


def test_blue_line_known_car():
    # Blue line: 0700-0793 → 2008
    assert get_car_build_year(700, "Blue") == 2008
    assert get_car_build_year(750, "Blue") == 2008
    assert get_car_build_year(793, "Blue") == 2008


def test_blue_line_boundary():
    assert get_car_build_year(699, "Blue") is None
    assert get_car_build_year(794, "Blue") is None


def test_orange_line_multiple_ranges():
    assert get_car_build_year(1400, "Orange") == 2019  # start of first range
    assert get_car_build_year(1415, "Orange") == 2019  # end of first range
    assert get_car_build_year(1416, "Orange") == 2020  # start of second range
    assert get_car_build_year(1551, "Orange") == 2025  # end of last range


def test_orange_line_out_of_range():
    assert get_car_build_year(1399, "Orange") is None
    assert get_car_build_year(1552, "Orange") is None


def test_red_line_old_fleet():
    assert get_car_build_year(1500, "Red") == 1970
    assert get_car_build_year(1651, "Red") == 1970


def test_red_line_newer_fleet():
    assert get_car_build_year(1900, "Red") == 2020
    assert get_car_build_year(1959, "Red") == 2025


def test_green_line():
    assert get_car_build_year(3600, "Green") == 1987
    assert get_car_build_year(3923, "Green") == 2019


def test_mattapan():
    assert get_car_build_year(3072, "Mattapan") == 1946
    assert get_car_build_year(3265, "Mattapan") == 1946


def test_unknown_line():
    assert get_car_build_year(700, "Silver") is None
    assert get_car_build_year(700, "") is None
    assert get_car_build_year(700, "CommuterRail") is None


def test_car_id_not_in_any_range():
    # Red line has no range covering, say, 9999
    assert get_car_build_year(9999, "Red") is None
