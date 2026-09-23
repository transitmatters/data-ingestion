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
        "description": "Median weekly average speed (miles covered / hours in motion), end to end.",
        "source": sources.TRIP_METRICS_TABLE,
        "decimals": 2,
    },
    "service": {
        "unit": "round trips per day",
        "description": "Median weekly delivered round trips per day.",
        "source": sources.TRIP_METRICS_TABLE,
        "decimals": 1,
    },
    "scheduledService": {
        "unit": "scheduled trips per day",
        "description": "Median weekly scheduled trips per day, from the service & ridership dashboard.",
        "source": f"s3://{sources.SERVICE_RIDERSHIP_BUCKET}/{sources.SERVICE_RIDERSHIP_KEY}",
        "decimals": 0,
    },
    "ridership": {
        "unit": "riders per weekday",
        "description": "Median weekly ridership (weekday fare validations / counts).",
        "source": sources.RIDERSHIP_TABLE,
        "decimals": 0,
    },
}


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
            "series": {
                series_id: compute.summarize_series(compute.to_weekly(points), cutoff, meta["decimals"])
                for series_id, points in sorted(series.items())
            },
        }

    return {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "definition": (
            f"Best rolling {compute.WINDOW_WEEKS}-week median over the full history, using only weeks that "
            f"ended at least {compute.LOOKBACK_YEARS} years before generatedAt."
        ),
        "cutoff": cutoff.isoformat(),
        "lookbackYears": compute.LOOKBACK_YEARS,
        "windowWeeks": compute.WINDOW_WEEKS,
        "minWeeksInWindow": compute.MIN_WEEKS_IN_WINDOW,
        "minHistoryWeeks": compute.MIN_HISTORY_WEEKS,
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
