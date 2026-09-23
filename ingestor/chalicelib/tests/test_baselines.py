from datetime import date, timedelta

from ..baselines import compute
from ..baselines.build import build_baselines


def weekly_points(start: date, values: list[float | None]) -> list[tuple[date, float | None]]:
    return [(start + timedelta(weeks=i), value) for i, value in enumerate(values)]


def test_cutoff_is_three_years_back():
    assert compute.cutoff_date(date(2026, 9, 23)) == date(2023, 9, 23)
    assert compute.cutoff_date(date(2028, 2, 29)) == date(2025, 2, 28)


def test_to_weekly_buckets_by_monday_and_drops_missing_and_zero():
    points = [
        (date(2019, 1, 9), 10.0),  # Wednesday, no Monday record that week -> averaged
        (date(2019, 1, 11), 14.0),  # Friday
        (date(2019, 1, 14), 0.0),  # shutdown / no data
        (date(2019, 1, 21), None),
        (date(2019, 1, 28), float("nan")),
    ]
    assert compute.to_weekly(points) == {date(2019, 1, 7): 12.0}


def test_to_weekly_prefers_the_canonical_monday_record():
    # Real shape of bus route 34 in 2019: stray mid-week records run ~2x the weekly level.
    points = [(date(2019, 3, 4), 2741.0), (date(2019, 3, 6), 6567.0), (date(2019, 3, 11), 3014.0)]
    assert compute.to_weekly(points) == {date(2019, 3, 4): 2741.0, date(2019, 3, 11): 3014.0}


CUTOFF = date(2023, 1, 1)


def test_sustained_peak_averages_a_spike_week_in_rather_than_taking_it():
    values = [20.0] * 30
    values[10] = 40.0
    weekly = compute.to_weekly(weekly_points(date(2018, 1, 1), values))
    # A spike proves some capacity, but only counts for a quarter of the 4-week mean.
    assert compute.best_rolling(weekly, CUTOFF, compute.SUSTAINED_PEAK).value == 25.0
    assert compute.best_rolling(weekly, CUTOFF, compute.STABLE_LEVEL).value == 20.0


def test_stable_level_ignores_artifact_weeks():
    # Scheduled service: a one-week overlapping-feed artifact must not move the scheduled level.
    values = [161.0] * 40
    values[20] = 200.0
    weekly = compute.to_weekly(weekly_points(date(2018, 1, 1), values))
    assert compute.best_rolling(weekly, CUTOFF, compute.STABLE_LEVEL).value == 161.0


def test_best_window_is_the_highest_sustained_period():
    values = [20.0] * 20 + [25.0] * 6 + [22.0] * 20
    weekly = compute.to_weekly(weekly_points(date(2018, 1, 1), values))
    best = compute.best_rolling(weekly, CUTOFF, compute.SUSTAINED_PEAK)
    assert best.value == 25.0
    assert best.window_start == date(2018, 1, 1) + timedelta(weeks=20)
    assert best.window_end == date(2018, 1, 1) + timedelta(weeks=23, days=6)
    assert best.weeks_with_data == 4

    median_best = compute.best_rolling(
        compute.to_weekly(weekly_points(date(2018, 1, 1), [20.0] * 20 + [25.0] * 12 + [22.0] * 20)),
        CUTOFF,
        compute.STABLE_LEVEL,
    )
    # Earliest window where the strong stretch is the majority (7 of 12 weeks) wins ties.
    assert median_best.value == 25.0
    assert median_best.window_start == date(2018, 1, 1) + timedelta(weeks=15)


def test_windows_with_too_few_weeks_are_skipped():
    sparse = [30.0, None, None, 30.0]
    assert (
        compute.best_rolling(compute.to_weekly(weekly_points(date(2018, 1, 1), sparse)), CUTOFF, compute.SUSTAINED_PEAK)
        is None
    )

    sparse[1] = 30.0  # 3 of 4 weeks with data is enough
    best = compute.best_rolling(
        compute.to_weekly(weekly_points(date(2018, 1, 1), sparse)), CUTOFF, compute.SUSTAINED_PEAK
    )
    assert best.value == 30.0 and best.weeks_with_data == 3

    burst = [30.0, None, 30.0, None, 30.0, None, 30.0, None, 30.0, 30.0, 30.0, 30.0]
    assert (
        compute.best_rolling(compute.to_weekly(weekly_points(date(2018, 1, 1), burst)), CUTOFF, compute.STABLE_LEVEL)
        is None
    )


def test_windows_never_bridge_long_gaps():
    # Two 2-week runs separated by a year: no single 4-week calendar window has 3 weeks of data.
    points = weekly_points(date(2018, 1, 1), [40.0] * 2) + weekly_points(date(2019, 1, 7), [40.0] * 2)
    assert compute.best_rolling(compute.to_weekly(points), CUTOFF, compute.SUSTAINED_PEAK) is None


def test_recent_data_is_excluded_until_three_years_old():
    # Steady 20 mph through 2026, except a strong stretch in 2024 and another in 2027.
    start = date(2016, 1, 4)
    weekly = {}
    for week in (start + timedelta(weeks=i) for i in range(52 * 15)):
        value = 20.0
        if date(2024, 3, 1) <= week < date(2024, 9, 1):
            value = 26.0
        if date(2027, 3, 1) <= week < date(2027, 9, 1):
            value = 28.0
        weekly[week] = value

    in_2026 = compute.best_rolling(weekly, compute.cutoff_date(date(2026, 9, 23)), compute.SUSTAINED_PEAK)
    in_2030 = compute.best_rolling(weekly, compute.cutoff_date(date(2030, 9, 23)), compute.SUSTAINED_PEAK)
    assert in_2026.value == 20.0
    assert in_2030.value == 28.0
    assert in_2030.window_start.year == 2027


def test_week_straddling_the_cutoff_is_excluded():
    cutoff = date(2023, 9, 20)  # a Wednesday
    straddling_week = compute.week_start(cutoff)
    weekly = {straddling_week: 99.0, straddling_week - timedelta(weeks=1): 1.0}
    assert compute.complete_weeks_before(weekly, cutoff) == {straddling_week - timedelta(weeks=1): 1.0}


def test_thin_history_is_marked_insufficient():
    # Commuter rail ridership only starts mid-2020; 30 weeks before the cutoff isn't enough.
    weekly = compute.to_weekly(weekly_points(date(2023, 1, 2), [100.0] * 60))
    entry = compute.summarize_series(weekly, date(2023, 8, 1), 0, compute.SUSTAINED_PEAK)
    assert entry["insufficientHistory"] is True
    assert entry["value"] is None
    assert entry["historyWeeks"] < compute.MIN_HISTORY_WEEKS


def test_summary_flags_pre_pandemic_history_and_rounds():
    weekly = compute.to_weekly(weekly_points(date(2018, 1, 1), [23.456] * 200))
    entry = compute.summarize_series(weekly, date(2023, 9, 23), 2, compute.SUSTAINED_PEAK)
    assert entry["value"] == 23.46
    assert entry["prePandemicHistory"] is True
    assert entry["insufficientHistory"] is False
    assert entry["historyWindow"] == {"start": "2018-01-01", "end": "2023-09-23"}
    assert entry["bestWindow"]["weeksWithData"] == 4
    assert "heldBack" not in entry

    post_pandemic = compute.to_weekly(weekly_points(date(2020, 6, 22), [5.0] * 200))
    assert (
        compute.summarize_series(post_pandemic, date(2023, 9, 23), 0, compute.SUSTAINED_PEAK)["prePandemicHistory"]
        is False
    )


def test_held_back_series_publish_no_value_but_keep_the_candidate():
    weekly = compute.to_weekly(weekly_points(date(2020, 6, 22), [93814.0] * 200))
    entry = compute.summarize_series(weekly, date(2023, 9, 23), 0, compute.SUSTAINED_PEAK, "noPrePandemicHistory")
    assert entry["value"] is None
    assert entry["candidateValue"] == 93814
    assert entry["heldBack"] == "noPrePandemicHistory"


def test_build_baselines_output_shape():
    history = weekly_points(date(2016, 1, 4), [24.0] * 600)
    baselines = build_baselines(
        today=date(2026, 9, 23),
        series_by_metric={
            "speed": {"line-red": history},
            "service": {"line-red": history},
            "scheduledService": {"mode-bus": history, "line-Green": history},
            "ridership": {
                "line-Red": history,
                "line-new": weekly_points(date(2025, 1, 6), [1.0] * 40),
                "line-Worcester": history,
                "line-commuter-rail": history,
                "line-ferry": history,
                "line-Boat-F1": history,
                "line-1": history,
            },
        },
    )
    assert baselines["schemaVersion"] == 1
    assert baselines["cutoff"] == "2023-09-23"
    assert set(baselines["metrics"]) == {"speed", "service", "scheduledService", "ridership"}
    assert baselines["metrics"]["speed"]["window"] == {"weeks": 4, "minWeeks": 3, "aggregate": "mean"}
    assert baselines["metrics"]["scheduledService"]["window"] == {"weeks": 12, "minWeeks": 9, "aggregate": "median"}

    red_speed = baselines["metrics"]["speed"]["series"]["line-red"]
    assert red_speed["value"] == 24.0
    assert red_speed["bestWindow"]["end"] <= baselines["cutoff"]

    ridership = baselines["metrics"]["ridership"]["series"]
    assert ridership["line-new"]["insufficientHistory"] is True
    assert isinstance(ridership["line-Red"]["value"], int)
    assert ridership["line-1"]["value"] == 24
    for series_id, reason in [
        ("line-Worcester", "noPrePandemicHistory"),
        ("line-commuter-rail", "noPrePandemicHistory"),
        ("line-ferry", "seasonal"),
        ("line-Boat-F1", "seasonal"),
    ]:
        assert ridership[series_id]["value"] is None
        assert ridership[series_id]["heldBack"] == reason
        assert reason in baselines["heldBackReasons"]

    scheduled = baselines["metrics"]["scheduledService"]["series"]
    assert scheduled["line-Green"]["heldBack"] == "notComparable"
    assert scheduled["mode-bus"]["value"] == 24
