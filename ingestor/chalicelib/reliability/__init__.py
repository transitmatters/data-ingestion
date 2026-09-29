__all__ = ["update_commuter_rail_reliability", "update_reliability", "update_the_ride_reliability"]

from .commuter_rail import update_commuter_rail_reliability
from .the_ride import update_the_ride_reliability


def update_reliability():
    """Ingest The RIDE and commuter rail reliability. A failure in one source doesn't block the other."""
    errors = []
    for update in (
        lambda: update_the_ride_reliability(lookback_days=120),
        lambda: update_commuter_rail_reliability(lookback_days=180),
    ):
        try:
            update()
        except Exception as e:
            print(f"Reliability ingest failed: {e!r}")
            errors.append(e)
    if errors:
        raise errors[0]
