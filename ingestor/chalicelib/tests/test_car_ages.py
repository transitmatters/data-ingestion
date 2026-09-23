from ..car_ages import get_car_build_year, is_car_new


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
