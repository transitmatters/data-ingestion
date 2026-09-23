"""Build and publish static/landing/baselines.json.

Run locally (writes a file, never uploads):
    cd ingestor && uv run python -m chalicelib.baselines --out baselines.json
"""

import argparse
import json
from datetime import date, datetime, timezone

from .. import landing, s3
from . import compute, sources

SCHEMA_VERSION = 1
BASELINES_KEY = "static/landing/baselines.json"

METRICS = {
    "speed": {
        "unit": "mph",
        "description": "Weekly average speed (miles covered / hours in motion), end to end.",
        "source": sources.TRIP_METRICS_TABLE,
        "decimals": 2,
        "window": compute.SUSTAINED_PEAK,
    },
    "service": {
        "unit": "round trips per day",
        "description": "Weekly delivered round trips per day.",
        "source": sources.TRIP_METRICS_TABLE,
        "decimals": 1,
        "window": compute.SUSTAINED_PEAK,
    },
    "scheduledService": {
        "unit": "scheduled trips per day",
        "description": "Weekly scheduled trips per day, from the service & ridership dashboard.",
        "source": f"s3://{sources.SERVICE_RIDERSHIP_BUCKET}/{sources.SERVICE_RIDERSHIP_KEY}",
        "decimals": 0,
        "window": compute.STABLE_LEVEL,
    },
    "ridership": {
        "unit": "riders per weekday",
        "description": "Weekly ridership (weekday fare validations / counts).",
        "source": sources.RIDERSHIP_TABLE,
        "decimals": 0,
        "window": compute.SUSTAINED_PEAK,
    },
}

# Baselines we compute but don't publish yet, pending a decision on how to handle them. Consumers
# keep their existing (hard-coded) value or show no baseline; the number stays visible as candidateValue.
HELD_BACK_REASONS = {
    "noPrePandemicHistory": "Ridership history starts June 2020, so the best period is a COVID-era recovery.",
    "seasonal": "Seasonal service; a rolling best compares summer peaks and needs a same-season method.",
    "notComparable": "Counts branch trips differently from the dashboard's service metric.",
}


def held_back_reason(metric: str, series_id: str) -> str | None:
    if metric == "ridership":
        if series_id in sources.CR_RIDERSHIP_IDS or series_id == "line-commuter-rail":
            return "noPrePandemicHistory"
        if series_id == "line-ferry" or series_id.startswith("line-Boat-"):
            return "seasonal"
    if metric == "scheduledService" and series_id == "line-Green":
        return "notComparable"
    return None


def build_baselines(today: date | None = None, series_by_metric: dict | None = None) -> dict:
    today = today or date.today()
    cutoff = compute.cutoff_date(today)
    if series_by_metric is None:
        speed, service = sources.get_trip_metrics_series()
        series_by_metric = {
            "speed": speed,
            "service": service,
            "scheduledService": sources.get_scheduled_service_series(),
            "ridership": sources.get_ridership_series(),
        }

    metrics = {}
    for metric, series in series_by_metric.items():
        meta = METRICS[metric]
        metrics[metric] = {
            "unit": meta["unit"],
            "description": meta["description"],
            "source": meta["source"],
            "window": meta["window"].to_json(),
            "series": {
                series_id: compute.summarize_series(
                    compute.to_weekly(points),
                    cutoff,
                    meta["decimals"],
                    meta["window"],
                    held_back_reason(metric, series_id),
                )
                for series_id, points in sorted(series.items())
            },
        }

    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "definition": (
            "Best rolling-window average over the full history (window per metric), using only weeks that "
            f"ended at least {compute.LOOKBACK_YEARS} years before generatedAt."
        ),
        "cutoff": cutoff.isoformat(),
        "lookbackYears": compute.LOOKBACK_YEARS,
        "minHistoryWeeks": compute.MIN_HISTORY_WEEKS,
        "heldBackReasons": HELD_BACK_REASONS,
        "metrics": metrics,
    }


def publish(baselines: dict):
    body = json.dumps(baselines, separators=(",", ":")).encode("utf-8")
    for bucket in landing.BUCKETS:
        print(f"Uploading {BASELINES_KEY} to {bucket}")
        s3.s3.put_object(
            Bucket=bucket,
            Key=BASELINES_KEY,
            Body=body,
            ContentType="application/json",
            CacheControl="public, max-age=3600",
        )
    for distribution in landing.DISTRIBUTIONS:
        s3.clear_cf_cache(distribution, [f"/{BASELINES_KEY}"])


def store_historical_baselines():
    publish(build_baselines())


def main():
    parser = argparse.ArgumentParser(description="Compute historical-best baselines.")
    parser.add_argument("--out", required=True, help="Path to write baselines JSON")
    parser.add_argument("--today", type=date.fromisoformat, help="Pretend today is YYYY-MM-DD")
    args = parser.parse_args()
    baselines = build_baselines(today=args.today)
    with open(args.out, "w") as f:
        json.dump(baselines, f, indent=2)
    print(f"Wrote {args.out} (cutoff {baselines['cutoff']})")
