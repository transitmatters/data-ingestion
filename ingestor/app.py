from chalice import Chalice, Cron
from datetime import date, timedelta, datetime
from chalicelib import (
    s3_alerts,
    new_trains,
    bluebikes,
    line_traversal,
    schedule_adherence,
    dynamo,
    update_agg_tables
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
@app.schedule('*/5 0-6,9-23 * * *')
def update_daily_table(event):
    today = datetime.now()
    ''' Update yesterdays entry until 4 am'''
    if today.hour < 4:
        today = datetime - timedelta(days=1)
    line_traversal.update_daily_table(today)


# Runs every 5 minutes from either 4 AM -> 1:55AM or 5 AM -> 2:55 AM depending on DST
@app.schedule('*/10 0-6,9-23 * * *')
def update_sched_adherence(event):
    schedule_adherence.update_current_schedule_adherence()


@app.schedule(Cron(0, 7, '*', '*', '?', '*'))
def update_weekly_and_monthly_tables():
    dynamo.update_weekly_tables()
    dynamo.update_monthly_tables()
