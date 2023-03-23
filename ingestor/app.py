from chalice import Chalice, Cron
from datetime import date, timedelta, datetime
from chalicelib import (
    s3_alerts,
    new_trains,
    bluebikes,
    daily_speeds,
    scheduled_speed,
    constants,
    agg_speed_tables
)

app = Chalice(app_name='ingestor')

################
# STORE ALERTS
# Every day at 10am UTC: store alerts from the past
# It's called yesterday for now but it's really two days ago!!
@app.schedule(Cron(0, 10, '*', '*', '?', '*'))
def store_yesterday_alerts(event):
    two_days_ago = date.today() - timedelta(days=2)
    s3_alerts.store_alerts(two_days_ago)

#################
# STORE NEW TRAIN TRIPS
# Ever day at 10:05am UTC: store new train runs from the previous day
@app.schedule(Cron(5, 10, '*', '*', '?', '*'))
def store_new_train_runs(event):
    yesterday = date.today() - timedelta(days=1)
    new_trains.update_all(yesterday)

#################
# PROCESS & STORE SLOWZONES
# TBD


#################
# STORE BLUEBIKES FEED
@app.schedule(Cron('0/5', '*', '*', '*', '?', '*'))
def bb_store_station_status(event):
    bluebikes.store_station_status()

# 10am UTC -> 6am EST
@app.schedule(Cron(0, 10, '*', '*', '?', '*'))
def bb_store_station_info(event):
    bluebikes.store_station_info()


# 6am UTC -> 2am EDT
@app.schedule(Cron(0, 6, '*', '*', '?', '*'))
def bb_calc_daily_stats(event):
    yesterday = date.today() - timedelta(days=1)
    bluebikes.calc_daily_stats(yesterday)


# Runs every 5 minutes from either 4 AM -> 1:55AM or 5 AM -> 2:55 AM depending on DST
@app.schedule(Cron('0/5', '0-6,9-23', '*', '*', '?', '*'))
def update_daily_speed_table(event):
    today = datetime.now()
    ''' Update yesterdays entry until 4/5 am (9 AM UTC)'''
    if today.hour < 9:
        today = today - timedelta(days=1)
    daily_speeds.update_daily_table(today)


# Runs every 5 minutes from either 4 AM -> 1:55AM or 5 AM -> 2:55 AM depending on DST
@app.schedule(Cron('0/5', '0-6,9-23', '*', '*', '?', '*'))
def update_scheduled_speed(event):
    today = datetime.now()
    ''' Update yesterdays entry until 4/5 am (9 AM UTC)'''
    if today.hour < 9:
        today = today - timedelta(days=1)
    scheduled_speed.update_scheduled_speed_entry(today)



# 7am UTC -> 2/3am EDT
@app.schedule(Cron(0, 7, '*', '*', '?', '*'))
def update_weekly_and_monthly_tables():
    agg_speed_tables.update_tables("weekly")
    agg_speed_tables.update_tables("monthly")


@app.lambda_function()
def populate_weekly_or_monthly_tables(params, context):
    agg_speed_tables.populate_table(params["line"], params["range"]) # monthly or weekly range

@app.lambda_function()
def populate_daily(params, context):
    start_date = datetime.strptime("2016-01-15", constants.DATE_FORMAT_BACKEND)
    end_date = datetime.now()
    daily_speeds.populate_daily_table(start_date, end_date, params["line"])
