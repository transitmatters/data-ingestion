from chalice import Chalice, Cron
from datetime import date, timedelta
from chalicelib import (
    s3_alerts,
    new_trains,
    bluebikes,
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

# 6am UTC -> 2am EST
@app.schedule(Cron(0, 6, '*', '*', '?', '*'))
def bb_calc_daily_stats(event):
    yesterday = date.today() - timedelta(days=1)
    bluebikes.calc_daily_stats(yesterday)
