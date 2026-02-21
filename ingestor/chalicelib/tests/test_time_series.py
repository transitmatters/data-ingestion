from datetime import date


from ..service_ridership_dashboard.time_series import (
    _bucket_by_week,
    _choose_between_previous_and_current_value,
    _date_ranges_intersect,
    _fill_zero_ranges_in_time_series,
    _find_zero_ranges_in_time_series,
    _get_entry_value_for_date,
    _get_monday_of_week_containing_date,
    _iterate_mondays,
    get_earliest_weekly_median_time_series_entry,
    get_latest_weekly_median_time_series_entry,
    get_weekly_median_time_series,
    get_weekly_median_time_series_entry_for_date,
    merge_weekly_median_time_series,
)


# --- _iterate_mondays ---


def test_iterate_mondays_yields_entries_in_range():
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 8), date(2024, 1, 15)
    entries = {d1: "a", d2: "b", d3: "c"}
    result = list(_iterate_mondays(entries, d1, d3))
    assert result == [(d1, "a"), (d2, "b"), (d3, "c")]


def test_iterate_mondays_respects_max_end_date():
    d1, d2, d3 = date(2024, 1, 1), date(2024, 1, 8), date(2024, 1, 15)
    entries = {d1: "a", d2: "b", d3: "c"}
    result = list(_iterate_mondays(entries, d1, d2))
    assert result == [(d1, "a"), (d2, "b")]


def test_iterate_mondays_skips_dates_not_in_entries():
    d1, d3 = date(2024, 1, 1), date(2024, 1, 15)
    d2 = date(2024, 1, 8)
    entries = {d1: "a", d3: "c"}
    result = list(_iterate_mondays(entries, d1, d3))
    dates = [r[0] for r in result]
    assert d2 not in dates
    assert d1 in dates
    assert d3 in dates


# --- _get_entry_value_for_date ---


def test_get_entry_value_for_date_exact_match():
    d = date(2024, 3, 11)
    entries = {d: 42.0}
    assert _get_entry_value_for_date(entries, d, lambda v: v) == 42.0


def test_get_entry_value_for_date_falls_back_to_prior():
    d1 = date(2024, 3, 11)
    d2 = date(2024, 3, 18)
    entries = {d1: 10.0}
    assert _get_entry_value_for_date(entries, d2, lambda v: v) == 10.0


def test_get_entry_value_for_date_returns_none_when_no_prior():
    d1 = date(2024, 3, 18)
    d2 = date(2024, 3, 11)  # earlier than any entry
    entries = {d1: 99.0}
    assert _get_entry_value_for_date(entries, d2, lambda v: v) is None


def test_get_entry_value_for_date_uses_getter():
    d = date(2024, 1, 1)
    entries = {d: {"val": 7.0}}
    assert _get_entry_value_for_date(entries, d, lambda e: e["val"]) == 7.0


# --- _choose_between_previous_and_current_value ---


def test_choose_both_none_returns_zero():
    monday = date(2024, 1, 1)  # Monday
    assert _choose_between_previous_and_current_value(None, None, monday) == 0


def test_choose_current_none_returns_previous():
    monday = date(2024, 1, 1)
    assert _choose_between_previous_and_current_value(None, 5.0, monday) == 5.0


def test_choose_weekend_prefers_previous():
    saturday = date(2024, 1, 6)  # Saturday
    assert _choose_between_previous_and_current_value(3.0, 7.0, saturday) == 7.0


def test_choose_weekday_returns_current():
    wednesday = date(2024, 1, 3)
    assert _choose_between_previous_and_current_value(3.0, 7.0, wednesday) == 3.0


def test_choose_weekend_no_previous_returns_current():
    sunday = date(2024, 1, 7)
    assert _choose_between_previous_and_current_value(4.0, None, sunday) == 4.0


# --- _date_ranges_intersect ---


def test_date_ranges_intersect_overlapping():
    a = (date(2024, 1, 1), date(2024, 6, 30))
    b = (date(2024, 3, 1), date(2024, 9, 30))
    assert _date_ranges_intersect(a, b) is True


def test_date_ranges_intersect_touching():
    a = (date(2024, 1, 1), date(2024, 3, 31))
    b = (date(2024, 3, 31), date(2024, 6, 30))
    assert _date_ranges_intersect(a, b) is True


def test_date_ranges_intersect_disjoint():
    a = (date(2024, 1, 1), date(2024, 3, 31))
    b = (date(2024, 6, 1), date(2024, 9, 30))
    assert _date_ranges_intersect(a, b) is False


def test_date_ranges_intersect_contained():
    a = (date(2024, 1, 1), date(2024, 12, 31))
    b = (date(2024, 3, 1), date(2024, 9, 30))
    assert _date_ranges_intersect(a, b) is True


# --- _find_zero_ranges_in_time_series ---


def test_find_zero_ranges_no_zeros():
    assert _find_zero_ranges_in_time_series([1.0, 2.0, 3.0]) == []


def test_find_zero_ranges_single_block():
    result = _find_zero_ranges_in_time_series([1.0, 0.0, 0.0, 1.0])
    assert result == [(1, 2)]


def test_find_zero_ranges_multiple_blocks():
    result = _find_zero_ranges_in_time_series([0.0, 1.0, 0.0, 0.0, 2.0])
    assert result == [(0, 0), (2, 3)]


def test_find_zero_ranges_trailing_zeros():
    result = _find_zero_ranges_in_time_series([1.0, 2.0, 0.0, 0.0])
    assert result == [(2, 3)]


def test_find_zero_ranges_all_zeros():
    result = _find_zero_ranges_in_time_series([0.0, 0.0, 0.0])
    assert result == [(0, 2)]


# --- _fill_zero_ranges_in_time_series ---


def test_fill_zero_ranges_small_gap_is_filled():
    # 3-day gap (≤5) starting at index 1, with non-zero predecessor
    ts = [5.0, 0.0, 0.0, 0.0, 10.0]
    start = date(2024, 6, 1)
    result = _fill_zero_ranges_in_time_series(ts, start)
    assert result[1] == 5.0
    assert result[2] == 5.0
    assert result[3] == 5.0


def test_fill_zero_ranges_large_gap_not_filled():
    # 8-day gap (>5), no FILL_DATE_RANGES overlap
    ts = [3.0] + [0.0] * 8 + [7.0]
    start = date(2025, 6, 1)  # Not near any holiday
    result = _fill_zero_ranges_in_time_series(ts, start)
    assert result[1] == 0.0
    assert result[4] == 0.0


def test_fill_zero_ranges_christmas_2022_filled():
    # Christmas 2022: (date(2022, 12, 18), date(2023, 1, 3)) in FILL_DATE_RANGES
    # Build a large zero range that overlaps this holiday range
    start = date(2022, 12, 15)
    # indices 3..20 = Dec 18 to Jan 4 (>5 days, but in FILL_DATE_RANGES)
    n_zeros = 17
    ts = [2.0, 2.0, 2.0] + [0.0] * n_zeros + [4.0]
    result = _fill_zero_ranges_in_time_series(ts, start)
    assert result[3] == 2.0
    assert result[10] == 2.0


def test_fill_zero_ranges_no_predecessor_stays_zero():
    # Zero range at the very start (index 0) — no predecessor, stays 0
    ts = [0.0, 0.0, 1.0]
    start = date(2024, 6, 1)
    result = _fill_zero_ranges_in_time_series(ts, start)
    assert result[0] == 0.0
    assert result[1] == 0.0


# --- _get_monday_of_week_containing_date ---


def test_monday_of_week_monday_input():
    monday = date(2024, 1, 1)  # Already a Monday
    assert _get_monday_of_week_containing_date(monday) == monday


def test_monday_of_week_thursday_input():
    thursday = date(2024, 1, 4)
    expected = date(2024, 1, 1)
    assert _get_monday_of_week_containing_date(thursday) == expected


def test_monday_of_week_sunday_input():
    sunday = date(2024, 1, 7)
    expected = date(2024, 1, 1)
    assert _get_monday_of_week_containing_date(sunday) == expected


# --- _bucket_by_week ---


def test_bucket_by_week_same_week_grouped():
    monday = date(2024, 1, 1)
    wednesday = date(2024, 1, 3)
    friday = date(2024, 1, 5)
    entries = {monday: "a", wednesday: "b", friday: "c"}
    buckets = _bucket_by_week(entries)
    assert len(buckets) == 1
    assert set(buckets[monday]) == {"a", "b", "c"}


def test_bucket_by_week_different_weeks_separate():
    w1_monday = date(2024, 1, 1)
    w2_monday = date(2024, 1, 8)
    entries = {w1_monday: "x", w2_monday: "y"}
    buckets = _bucket_by_week(entries)
    assert len(buckets) == 2
    assert buckets[w1_monday] == ["x"]
    assert buckets[w2_monday] == ["y"]


# --- get_weekly_median_time_series ---


def test_get_weekly_median_single_week_single_entry():
    monday = date(2024, 1, 1)
    entries = {monday: 10.0}
    result = get_weekly_median_time_series(entries, lambda v: v, monday, monday)
    assert result == {"2024-01-01": 10.0}


def test_get_weekly_median_odd_count_picks_middle():
    monday = date(2024, 1, 1)
    tue = date(2024, 1, 2)
    wed = date(2024, 1, 3)
    # Values 1, 5, 9 → sorted [1, 5, 9] → median index 1 → 5
    entries = {monday: 9.0, tue: 1.0, wed: 5.0}
    result = get_weekly_median_time_series(entries, lambda v: v, monday, monday)
    assert result["2024-01-01"] == 5.0


def test_get_weekly_median_multiple_weeks():
    w1 = date(2024, 1, 1)
    w2 = date(2024, 1, 8)
    entries = {w1: 3.0, w2: 7.0}
    result = get_weekly_median_time_series(entries, lambda v: v, w1, w2)
    assert "2024-01-01" in result
    assert "2024-01-08" in result


# --- merge_weekly_median_time_series ---


def test_merge_empty_list():
    assert merge_weekly_median_time_series([]) == {}


def test_merge_single_series():
    s = {"2024-01-01": 5.0, "2024-01-08": 10.0}
    result = merge_weekly_median_time_series([s])
    assert result == s


def test_merge_shared_week_sums():
    s1 = {"2024-01-01": 4.0}
    s2 = {"2024-01-01": 6.0}
    result = merge_weekly_median_time_series([s1, s2])
    assert result["2024-01-01"] == 10.0


def test_merge_disjoint_weeks_all_present():
    s1 = {"2024-01-01": 3.0}
    s2 = {"2024-01-08": 7.0}
    result = merge_weekly_median_time_series([s1, s2])
    assert result["2024-01-01"] == 3.0
    assert result["2024-01-08"] == 7.0


# --- get_weekly_median_time_series_entry_for_date ---


def test_get_entry_for_date_monday():
    series = {"2024-01-01": 42.0}
    result = get_weekly_median_time_series_entry_for_date(series, date(2024, 1, 1))
    assert result == 42.0


def test_get_entry_for_date_mid_week():
    series = {"2024-01-01": 99.0}
    # Wednesday Jan 3 belongs to Mon Jan 1 week
    result = get_weekly_median_time_series_entry_for_date(series, date(2024, 1, 3))
    assert result == 99.0


def test_get_entry_for_date_not_in_series():
    series = {"2024-01-01": 5.0}
    result = get_weekly_median_time_series_entry_for_date(series, date(2024, 2, 5))
    assert result is None


# --- get_latest / get_earliest ---


def test_get_latest_entry():
    series = {"2024-01-01": 1.0, "2024-01-08": 2.0, "2024-01-15": 3.0}
    assert get_latest_weekly_median_time_series_entry(series) == 3.0


def test_get_earliest_entry():
    series = {"2024-01-01": 1.0, "2024-01-08": 2.0, "2024-01-15": 3.0}
    assert get_earliest_weekly_median_time_series_entry(series) == 1.0
