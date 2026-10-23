import json
from datetime import date, datetime, timedelta

from chalice import Chalice, ConvertToMiddleware, Cron
from chalicelib import (
    agg_speed_tables,
    alerts,
    bluebikes,
    bus_fleet,
    constants,
    cr_fleet,
    daily_speeds,
    delays,
    gtfs,
    landing,
    predictions,
    reliability,
    ridership,
    service_ridership_dashboard,
    shutdowns,
    speed_restrictions,
    trip_metrics,
    weather,
)
from chalicelib.baselines.build import store_historical_baselines as build_and_publish_baselines
from datadog_lambda.wrapper import datadog_lambda_wrapper

app = Chalice(app_name="ingestor")

app.register_middleware(ConvertToMiddleware(datadog_lambda_wrapper))


################
# STORE V3 ALERTS
# Runs every 15 minutes from either 4 AM -> 1:55AM or 5 AM -> 2:55 AM depending on DST
@app.schedule(Cron("0/15", "0-6,9-23", "*", "*", "?", "*"))
def store_current_alerts(event):
    """Fetches and stores current MBTA V3 alerts to S3."""
    alerts.save_v3_alerts()


#################
# STORE NEW TRAIN TRIPS
# Every day at 10:05am UTC: store new train runs from the previous day
# TODO: Convert to use data dashboard api, performance API died
# @app.schedule(Cron(5, 10, "*", "*", "?", "*"))
# def store_new_train_runs(event):
#     yesterday = date.today() - timedelta(days=1)
#     new_trains.update_all(yesterday)


#################
# STORE BLUEBIKES FEED
@app.schedule(Cron("0/5", "*", "*", "*", "?", "*"))
def bb_store_station_status(event):
    """Stores current Bluebikes station status snapshot to S3."""
    bluebikes.store_station_status()


# 10am UTC -> 6am EST
@app.schedule(Cron(0, 10, "*", "*", "?", "*"))
def bb_store_station_info(event):
    """Stores daily Bluebikes station info to S3."""
    bluebikes.store_station_info()


# 6am UTC -> 2am EDT
@app.schedule(Cron(0, 6, "*", "*", "?", "*"))
def bb_calc_daily_stats(event):
    """Calculates daily Bluebikes rideability statistics for yesterday."""
    yesterday = date.today() - timedelta(days=1)
    bluebikes.calc_daily_stats(yesterday)


# Runs every 30 minutes from either 4 AM -> 1:55AM or 5 AM -> 2:55 AM depending on DST
@app.schedule(Cron("0/30", "0-6,9-23", "*", "*", "?", "*"))
def update_delivered_trip_metrics(event):
    """Updates daily trip metrics for today (or yesterday before 9 AM UTC)."""
    today = datetime.now()
    if today.hour < 9:
        today = today - timedelta(days=1)
    daily_speeds.update_daily_table(today.date())


# 7am UTC -> 2/3am ET
# Update weekly and monthly tables. At 2/3 AM EST and also after we have updated yesterday's data.
@app.schedule(Cron(10, "7,12", "*", "*", "?", "*"))
def update_agg_trip_metrics(event):
    """Updates weekly and monthly aggregate trip metric tables."""
    agg_speed_tables.update_tables("weekly")
    agg_speed_tables.update_tables("monthly")


# 12 UTC -> 7/8am ET
# The MBTA cleans up their data the next day (we suspect sometime after 4 AM). Update yesterday's data after this (and 2 days ago to be safe).
@app.schedule(Cron(0, 12, "*", "*", "?", "*"))
def update_delivered_trip_metrics_yesterday(event):
    """Re-updates trip metrics for yesterday and two days ago after MBTA data cleanup."""
    today = datetime.now()
    yesterday = (today - timedelta(days=1)).date()
    two_days_ago = (today - timedelta(days=2)).date()
    daily_speeds.update_daily_table(yesterday)
    daily_speeds.update_daily_table(two_days_ago)


# 12:30 UTC -> 7:30/8:30am ET, after the MBTA's next-day data cleanup
@app.schedule(Cron(30, 12, "*", "*", "?", "*"))
def update_bus_fleet_metrics(event):
    yesterday = (datetime.now() - timedelta(days=1)).date()
    bus_fleet.update_bus_fleet_table(yesterday)


# 12:35 UTC -> 7:35/8:35am ET
@app.schedule(Cron(35, 12, "*", "*", "?", "*"))
def update_cr_fleet_metrics(event):
    yesterday = (datetime.now() - timedelta(days=1)).date()
    cr_fleet.update_cr_fleet_table(yesterday)


# 7:10am UTC -> 2:10/3:10am ET every day
@app.schedule(Cron(10, 7, "*", "*", "?", "*"))
def update_ridership(event):
    """Ingests the latest ridership data into DynamoDB."""
    ridership.ingest_ridership_data()


# Retired 2026-09-21: the upstream ArcGIS source (CSV_ZIP_URL in
# chalicelib/speed_restrictions.py) now 400s -- "Item does not exist or is
# inaccessible." mbta.com/performance-metrics/speed-restrictions no longer
# exists either. Data in DynamoDB is stale past 2026-05-31. Paused pending a
# reply from the T on whether this moved. It stays deployed as a manual lambda
# (resources.json still configures it); swap back to
# @app.schedule(Cron(20, 7, "?", "*", "MON-FRI", "*")) to resume.
@app.lambda_function()
def update_speed_restrictions(event, context):
    speed_restrictions.update_speed_restrictions(max_lookback_months=2)


# 7:50am UTC -> 2:50/3:50am ET every Monday (source data is published monthly)
@app.schedule(Cron(50, 7, "?", "*", "MON", "*"))
def update_reliability(event):
    reliability.update_reliability()


# 7:30am UTC -> 2:30/3:30am ET every day
@app.schedule(Cron(30, 7, "*", "*", "?", "*"))
def update_time_predictions(event):
    """Updates prediction accuracy data from MassDOT ArcGIS."""
    predictions.update_predictions()


# 8:00am UTC -> 3:00/4:00am ET and 11:00pm UTC -> 7:00/6:00pm ET every day
@app.schedule(Cron(0, "8,23", "*", "*", "?", "*"))
def update_gtfs(event):
    """Ingests GTFS feeds from the last 3 days into DynamoDB and S3."""
    # Look ahead as well as back. get_feeds_for_dates matches feeds *covering*
    # the range, so a backward-looking window never sees a feed until the day it
    # activates -- leaving mbta-performance without a gtfs.sqlite3 until the next
    # scheduled run, up to 12 hours later. MBTA publishes with a lead time, so
    # reaching forward builds the bundle before anything asks for it.
    today = datetime.now()
    three_days_ago = (today - timedelta(days=3)).date()
    two_weeks_ahead = (today + timedelta(days=14)).date()
    gtfs.ingest_gtfs_feeds_to_dynamo_and_s3(date_range=(three_days_ago, two_weeks_ahead))


# Builds a single GTFS feed on demand. mbta-performance enqueues a key here when
# it needs a bundle that is not in S3 yet, rather than attempting a ~67s, ~1GB
# build inside its own 60s Lambda. One feed per message keeps peak /tmp to a
# single build; failures land in gtfs-ingest-keys-dlq after 3 attempts.
@app.on_sqs_message(queue="gtfs-ingest-keys", batch_size=1)
def ingest_gtfs_feed_on_demand(event):
    for record in event:
        message = json.loads(record.body)
        feed_key = message["feed_key"]
        print(f"[{feed_key}] Building on demand from SQS")
        gtfs.ingest_gtfs_feeds_to_dynamo_and_s3(
            feed_key=feed_key,
            force_rebuild_feeds=message.get("force_rebuild_feeds", False),
        )


# 4:40am UTC -> 2:40/3:40am ET every day
@app.schedule(Cron(40, 7, "*", "*", "?", "*"))
def update_trip_metrics(event):
    """Ingests trip metrics for the last 7 days."""
    trip_metrics.ingest_recent_trip_metrics(lookback_days=7)


# 12:45pm UTC -> 7:45/8:45am ET every Monday and Tuesday
# There's shouldn't be any benefit to running it more frequently.
# Runs 15 minutes after the daily update
@app.schedule(Cron(45, 12, "?", "*", "MON,TUE", "*"))
def update_weekly_alert_delays(event):
    """Aggregates daily delay data into weekly totals for the last 15 days."""
    today = datetime.now()
    one_week_ago = (today - timedelta(days=15)).date()
    delays.update_weekly_from_daily(one_week_ago, today.date())


# for daily delay uploads
# 12:30pm UTC, after mbta-performance rebuilds yesterday's LAMP alerts file at 11:00 UTC
@app.schedule(Cron(30, 12, "*", "*", "?", "*"))
def update_daily_alert_delays(event):
    """Processes yesterday's alerts and updates the daily delay table."""
    today = datetime.now()
    yesterday = (today - timedelta(days=1)).date()
    delays.update_table(yesterday, today.date())


# 12:40pm UTC -> 7:40/8:40am ET, after mbta-performance rebuilds the last 30 days of LAMP alerts files at 11:00 UTC
@app.schedule(Cron(40, 12, "*", "*", "?", "*"))
def update_shutdowns(event):
    """Indexes new and revised shuttle, suspension, and station closure alerts into the Shutdowns table."""
    shutdowns.update_recent()


# Manually triggered lambda for backfilling the Shutdowns table. Run it in date order, a year or so at a time.
# Payload: {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}
@app.lambda_function()
def populate_shutdowns(params, context):
    shutdowns.update_table(date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"]))


# Manually triggered lambda for populating daily trip metric tables. Only needs to be ran once.
@app.lambda_function()
def populate_delivered_trip_metrics(params, context):
    """Backfills the DeliveredTripMetrics table for all routes from 2016 to now.

    Args:
        params: Lambda function parameters (unused).
        context: Lambda context object (unused).
    """
    start_date = datetime.strptime("2016-01-15", constants.DATE_FORMAT_BACKEND)
    end_date = datetime.now()
    for route in constants.ALL_ROUTES:
        daily_speeds.populate_daily_table(start_date, end_date, route[0], route[1])


# Manually triggered lambda for populating monthly or weekly tables. Only needs to be ran once.
@app.lambda_function()
def populate_agg_delivered_trip_metrics(params, context):
    """Backfills monthly and weekly aggregate trip metric tables for all lines.

    Args:
        params: Lambda function parameters (unused).
        context: Lambda context object (unused).
    """
    for line in constants.LINES:
        print(f"Populating monthly and weekly aggregate trip metrics for {line}")
        agg_speed_tables.populate_table(line, "monthly")
        agg_speed_tables.populate_table(line, "weekly")


# 9:00 UTC -> 4:00/5:00am ET every weekday.
# This is the last job that runs for the day.
# No need to run on weekends
@app.schedule(Cron(0, 9, "?", "*", "MON-FRI", "*"))
def store_landing_data(event):
    """Uploads trip metrics and ridership data for the landing page to S3."""
    print(
        f"Uploading ridership and trip metric data for landing page from {constants.NINETY_DAYS_AGO_STRING} to {constants.ONE_WEEK_AGO_STRING}"
    )
    trip_metrics_data = landing.get_trip_metrics_data()
    ridership_data = landing.get_ridership_data()
    landing.upload_to_s3(json.dumps(trip_metrics_data), json.dumps(ridership_data))
    landing.clear_cache()


# 8:15 UTC -> 3:15/4:15am ET every Monday, after the weekly trip metric tables and ridership update.
# Publishes static/landing/baselines.json: the "historical best" per line and metric, looking back
# 3+ years. It only moves when a week crosses the 3-year line, so weekly is plenty.
@app.schedule(Cron(15, 8, "?", "*", "MON", "*"))
def store_historical_baselines(event):
    build_and_publish_baselines()


# 9:00 UTC -> 4:30/5:30am ET every day (after GTFS and ridership have been ingested)
@app.schedule(Cron(30, 9, "*", "*", "?", "*"))
def update_service_ridership_dashboard(event):
    """Generates and uploads the service ridership dashboard JSON."""
    service_ridership_dashboard.create_service_ridership_dash_json()


#################
# STORE HOURLY WEATHER
# Every hour at :05 — fetch latest Boston hourly weather and merge into today's S3 file.
@app.schedule(Cron(5, "*", "*", "*", "?", "*"))
def store_hourly_weather(event):
    weather.ingest_hourly_weather()


# Manually triggered lambda for backfilling historical weather from Open-Meteo's archive API.
# Payload: {"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}
@app.lambda_function()
def backfill_weather(params, context):
    weather.backfill_weather(params["start_date"], params["end_date"])
