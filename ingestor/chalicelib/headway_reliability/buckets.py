# (field, lower, upper, includes lower, includes upper) on actual / scheduled headway. Exactly 0.5
# is bunched and 0.75-1.25 is exclusive, like the dashboard; 1.5 goes up, mirroring 0.5. Bus rows
# in DeliveredTripMetricsBus (mbta-performance lamp/bus/constants.py) use the same names and edges,
# so keep the two in step.
HEADWAY_RATIO_BUCKETS = (
    ("headways_under_50", float("-inf"), 0.5, True, True),
    ("headways_50_75", 0.5, 0.75, False, True),
    ("headways_75_125", 0.75, 1.25, False, False),
    ("headways_125_150", 1.25, 1.5, True, False),
    ("headways_over_150", 1.5, float("inf"), True, True),
)
HEADWAY_FIELDS = [bucket[0] for bucket in HEADWAY_RATIO_BUCKETS]
