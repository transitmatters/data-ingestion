"""Statuses and row headlines. Mirrors the dashboard's modules/home/compute/status.ts."""

from .config import METRICS
from .series import is_finite


def status_for(score: float | None, direction: str, thresholds) -> str:
    win, good, room = thresholds
    if not is_finite(score):
        return "insufficient"
    if direction == "higher":
        if score >= win:
            return "win"
        if score >= good:
            return "good"
        if score >= room:
            return "room"
        return "problem"
    if score <= win:
        return "win"
    if score <= good:
        return "good"
    if score <= room:
        return "room"
    return "problem"


def is_measured(status: str) -> bool:
    return status not in ("insufficient", "na")


SEVERITY = {"problem": 3, "room": 2, "good": 1}


def _gap_to_good(cell: dict) -> float:
    """How far a score sits from "good", as a fraction of the good threshold."""
    if cell["score"] is None:
        return 0
    method = METRICS[cell["metric"]]
    good = cell.get("thresholds", method["thresholds"])[1]
    direction = cell.get("direction", method["direction"])
    gap = good - cell["score"] if direction == "higher" else cell["score"] - good
    return max(0, gap / abs(good or 1))


def headline_for(cells: list[dict], min_measured: int) -> dict:
    """Worst-of, never an average: one problem cell makes the row "Needs work" however good the
    rest is. "Strong" needs no problems or room to grow and at least half the measured cells
    winning."""
    measured = [cell for cell in cells if is_measured(cell["status"])]
    held_back = sorted(
        (cell for cell in measured if cell["status"] in ("problem", "room")),
        key=lambda cell: (-SEVERITY[cell["status"]], -_gap_to_good(cell)),
    )
    held_back_by = [cell["metric"] for cell in held_back]
    statuses = [cell["status"] for cell in measured]
    if len(measured) < min_measured:
        kind = "limited"
    elif "problem" in statuses:
        kind = "needsWork"
    elif "room" in statuses:
        kind = "room"
    else:
        kind = "strong" if statuses.count("win") >= len(measured) / 2 else "good"
    return {"kind": kind, "heldBackBy": held_back_by}
