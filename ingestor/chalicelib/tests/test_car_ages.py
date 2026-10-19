from decimal import Decimal

from ..car_ages import compute_fleet_mix, get_car_build_year, get_car_type, is_car_new

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
    assert get_car_build_year(1400, "Orange") == 2018.5  # start of first range
    assert get_car_build_year(1403, "Orange") == 2018.5  # end of first range
    assert get_car_build_year(1404, "Orange") == 2018.75  # start of second range
    assert get_car_build_year(1551, "Orange") == 2025.75  # end of last range


def test_orange_line_out_of_range():
    assert get_car_build_year(1399, "Orange") is None
    assert get_car_build_year(1552, "Orange") is None


def test_red_line_old_fleet():
    assert get_car_build_year(1500, "Red") == 1970
    assert get_car_build_year(1651, "Red") == 1970


def test_red_line_newer_fleet():
    assert get_car_build_year(1900, "Red") == 2020.5
    assert get_car_build_year(1959, "Red") == 2026.0


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


def test_green_new_ranges():
    assert is_car_new(3900, "Green")
    assert is_car_new(3923, "Green")
    assert not is_car_new(3924, "Green")
    assert is_car_new(4001, "Green")
    assert is_car_new(4102, "Green")
    assert not is_car_new(4103, "Green")
    assert not is_car_new(3894, "Green")
    assert not is_car_new(3650, "Green")


def test_other_lines_new_ranges():
    assert is_car_new(1900, "Red")
    assert is_car_new(2151, "Red")
    assert not is_car_new(1885, "Red")
    assert is_car_new(1551, "Orange")
    assert not is_car_new(700, "Blue")
    assert not is_car_new(3234, "Mattapan")


def test_build_years():
    assert get_car_build_year(3850, "Green") == 2004.5
    assert get_car_build_year(4002, "Green") == 2026.75
    # Type 10 production cars aren't in the table until they're delivered
    assert get_car_build_year(4050, "Green") is None
    assert get_car_build_year(1966, "Red") == 2026.25
    assert get_car_build_year(1250, "Orange") == 1980


def test_car_types():
    assert get_car_type(3650, "Green") == "type7"
    assert get_car_type(3710, "Green") == "type7"
    assert get_car_type(3850, "Green") == "type8"
    assert get_car_type(3910, "Green") == "type9"
    assert get_car_type(4050, "Green") == "type10"
    assert get_car_type(1750, "Red") == "red2"
    assert get_car_type(1960, "Red") == "red4"
    assert get_car_type(9999, "Green") is None
    assert get_car_type(1250, "Orange") == "orange12"
    assert get_car_type(1450, "Orange") == "orange14"
    assert get_car_type(700, "Blue") is None  # single-type lines aren't tracked


def test_fleet_mix_mixed_consist():
    # A Type 7 + Type 8 train and a Type 9 pair: 1 + 1 + 2 cars
    mix = compute_fleet_mix([{3650, 3850}, {3901, 3904}], "Green")
    assert mix == {
        "fleet_mix_type7": Decimal("25.0"),
        "fleet_mix_type8": Decimal("25.0"),
        "fleet_mix_type9": Decimal("50.0"),
        "fleet_mix_type10": Decimal("0.0"),
    }


def test_fleet_mix_sums_to_100_and_skips_unknown_cars():
    mix = compute_fleet_mix([{1500, 1501, 1700}, {1900, 1901, 1902}, {1234}], "Red")
    assert set(mix) == {"fleet_mix_red1", "fleet_mix_red2", "fleet_mix_red3", "fleet_mix_red4"}
    assert mix["fleet_mix_red3"] == 0
    assert abs(sum(mix.values()) - 100) <= Decimal("0.2")


def test_fleet_mix_empty():
    assert compute_fleet_mix([], "Green") == {}
    assert compute_fleet_mix([{9999}], "Green") == {}
    assert compute_fleet_mix([{700}], "Blue") == {}
