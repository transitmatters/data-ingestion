from chalice import Chalice, Cron
from datetime import date, timedelta, datetime
from chalicelib import (
    s3_alerts,
    new_trains,
    bluebikes,
    daily_speeds,
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


# 7am UTC -> 2/3am ET
# Update weekly and monthly tables. At 2/3 AM EST and also after we have updated yesterday's data.
@app.schedule(Cron(10, "7, 12", '*', '*', '?', '*'))
def update_weekly_and_monthly_tables(event):
    agg_speed_tables.update_tables("weekly")
    agg_speed_tables.update_tables("monthly")

# 12 UTC -> 7/8am ET
# The MBTA cleans up their data the next day (we suspect sometime after 4 AM). Update yesterday's data after this (and 2 days ago to be safe).
@app.schedule(Cron(0, 12, '*', '*', '?', '*'))
def update_daily_next_day(event):
    today = datetime.now()
    daily_speeds.update_daily_table(today - timedelta(days=1))
    daily_speeds.update_daily_table(today - timedelta(days=2))




# Manually triggered lambda for populating monthly or weekly tables. Only needs to be ran once.
@app.lambda_function()
def populate_weekly_or_monthly_tables(params, context):
    '''
    line options: line-red | line-orange | line-blue
    range options: weekly | monthly
    '''
    agg_speed_tables.populate_table(params["line"], params["range"])


# Manually triggered lambda for populating daily tables. Only needs to be ran once.
# Takes line key as input (line-red | line-orange | line-blue)
@app.lambda_function()
def populate_daily(params, context):
    start_date = datetime.strptime("2016-01-15", constants.DATE_FORMAT_BACKEND)
    end_date = datetime.now()
    daily_speeds.populate_daily_table(start_date, end_date, params["line"])



# Manually triggered lambda for populating daily tables. Should only be ran once.
# Takes line key as input (line-red | line-orange | line-blue)
@app.route("/test")
def populate_daily():
    start_date = datetime.strptime("2016-01-01", constants.DATE_FORMAT_BACKEND)
    end_date = datetime.now()
    daily_speeds.populate_daily_table(start_date, end_date, "line-red")

@app.route("/popweek")
def pop_month():
    agg_speed_tables.populate_table("line-red", "weekly")

@app.route("/update/today")
def update_today():
    today = datetime.now()
    ''' Update yesterdays entry until 4/5 am (9 AM UTC)'''
    if today.hour < 9:
        today = today - timedelta(days=1)
    daily_speeds.update_daily_table(today)
