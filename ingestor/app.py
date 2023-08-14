from chalice import Chalice, Cron, ConvertToMiddleware
import json
from datetime import date, timedelta, datetime
from datadog_lambda.wrapper import datadog_lambda_wrapper
from chalicelib import (
    s3_alerts,
    new_trains,
    bluebikes,
    daily_speeds,
    constants,
    agg_speed_tables,
    gtfs,
    ridership,
    speed_restrictions,
    predictions,
    landing,
)

app = Chalice(app_name="ingestor")

app.register_middleware(ConvertToMiddleware(datadog_lambda_wrapper))


################
# STORE ALERTS
# Every day at 10am UTC: store alerts from the past
# It's called yesterday for now but it's really two days ago!!
@app.schedule(Cron(0, 10, "*", "*", "?", "*"))
def store_yesterday_alerts(event):
    two_days_ago = date.today() - timedelta(days=2)
    s3_alerts.store_alerts(two_days_ago)


#################
# STORE NEW TRAIN TRIPS
# Ever day at 10:05am UTC: store new train runs from the previous day
@app.schedule(Cron(5, 10, "*", "*", "?", "*"))
def store_new_train_runs(event):
    yesterday = date.today() - timedelta(days=1)
    new_trains.update_all(yesterday)


#################
# PROCESS & STORE SLOWZONES
# TBD


#################
# STORE BLUEBIKES FEED
@app.schedule(Cron("0/5", "*", "*", "*", "?", "*"))
def bb_store_station_status(event):
    bluebikes.store_station_status()


# 10am UTC -> 6am EST
@app.schedule(Cron(0, 10, "*", "*", "?", "*"))
def bb_store_station_info(event):
    bluebikes.store_station_info()


# 6am UTC -> 2am EDT
@app.schedule(Cron(0, 6, "*", "*", "?", "*"))
def bb_calc_daily_stats(event):
    yesterday = date.today() - timedelta(days=1)
    bluebikes.calc_daily_stats(yesterday)


# Runs every 5 minutes from either 4 AM -> 1:55AM or 5 AM -> 2:55 AM depending on DST
@app.schedule(Cron("0/5", "0-6,9-23", "*", "*", "?", "*"))
def update_delivered_trip_metrics(event):
    today = datetime.now()
    """ Update yesterdays entry until 4/5 am (9 AM UTC)"""
    if today.hour < 9:
        today = today - timedelta(days=1)
    daily_speeds.update_daily_table(today)


# 7am UTC -> 2/3am ET
# Update weekly and monthly tables. At 2/3 AM EST and also after we have updated yesterday's data.
@app.schedule(Cron(10, "7,12", "*", "*", "?", "*"))
def update_agg_trip_metrics(event):
    agg_speed_tables.update_tables("weekly")
    agg_speed_tables.update_tables("monthly")


# 12 UTC -> 7/8am ET
# The MBTA cleans up their data the next day (we suspect sometime after 4 AM). Update yesterday's data after this (and 2 days ago to be safe).
@app.schedule(Cron(0, 12, "*", "*", "?", "*"))
def update_delivered_trip_metrics_yesterday(event):
    today = datetime.now()
    daily_speeds.update_daily_table(today - timedelta(days=1))
    daily_speeds.update_daily_table(today - timedelta(days=2))


# 7am UTC -> 2/3am ET
@app.schedule(Cron(0, 7, "*", "*", "?", "*"))
def update_gtfs(event):
    today = date.today()
    gtfs.ingest_gtfs_feeds_to_dynamo_and_s3(date_range=(today, today))


# 7:10am UTC -> 2:10/3:10am ET
@app.schedule(Cron(10, 7, "*", "*", "?", "*"))
def update_ridership(event):
    ridership.ingest_ridership_data()


# 7:20am UTC -> 2:20/3:20am ET
@app.schedule(Cron(20, 7, "*", "*", "?", "*"))
def update_speed_restrictions(event):
    speed_restrictions.update_speed_restrictions()


# 7:30am UTC -> 2:30/3:30am ET every day
@app.schedule(Cron(30, 7, "?", "*", "?", "*"))
def update_time_predictions(event):
    predictions.update_predictions()


# Manually triggered lambda for populating daily trip metric tables. Only needs to be ran once.
@app.lambda_function()
def populate_delivered_trip_metrics(params, context):
    start_date = datetime.strptime("2016-01-15", constants.DATE_FORMAT_BACKEND)
    end_date = datetime.now()
    for route in constants.ALL_ROUTES:
        daily_speeds.populate_daily_table(start_date, end_date, route[0], route[1])


# Manually triggered lambda for populating monthly or weekly tables. Only needs to be ran once.
@app.lambda_function()
def populate_agg_delivered_trip_metrics(params, context):
    for line in constants.LINES:
        print(line)
        agg_speed_tables.populate_table(line, "monthly")
        agg_speed_tables.populate_table(line, "weekly")


# 9:00 UTC -> 4:00/5:00am ET every monday.
@app.schedule(Cron(0, 9, "?", "*", "MON", "*"))
def store_landing_data(event):
    print(
        f"Uploading ridership and trip metric data for landing page from {constants.NINETY_DAYS_AGO_STRING} to {constants.ONE_WEEK_AGO_STRING}"
    )
    trip_metrics_data = landing.get_trip_metrics_data()
    ridership_data = landing.get_ridership_data()
    landing.upload_to_s3(json.dumps(trip_metrics_data), json.dumps(ridership_data))
    landing.clear_cache()
