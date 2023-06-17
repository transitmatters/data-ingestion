from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


'''array of stop pairs which encompass entire system. Not actually termini - one before the terminal stop.'''
TERMINI = {
    # RL: Davis SB = 70063, Shawmut SB = 70091, Quincy Adams SB = 70103, Quincy Adams NB = 70104, Davis NB = 70064, Shawmut NB = 70092
    "line-red": [[70063, 70091], [70092, 70064], [70063, 70103], [70104, 70064]],
    # OL: Malden Center SB = 70034, Green Street SB = 70002, Green Street NB = 70003, Malden Center NB 70035
    "line-orange": [[70034, 70002], [70003, 70035]],
    # BL: Revere Beach SB = 70057, Gov. Center SB = 70039, Revere Beach NB = 70058, Gov. Center NB 70040
    "line-blue": [[70057, 70039], [70040, 70058]],
    # Medford/Tufts NB/SB = 70511/70512, Back of the Hill NB/SB = 70258/70257, Lechmere NB/SB = 70501/70502, Woodland NB/SB = 70162
    "line-green-glx": [
        #E - Medford/Tufts NB/SB = 70511/70512, Back of the Hill NB/SB = 70258/70257,
        [70512,70257], [70258, 70511], 
        #D - Lechmere NB/SB = 70501/70502, Woodland NB/SB = 70162/70163
        [70162, 70501], [70502, 70163],
        #C - Gov. Center NB/SB = 70201/70202,  Englewood NB/SB = 70236/70235
        [70236, 70201],[70202,70235],
        #B - Gov. Center NB/SB = 70201/70202,  South St. NB/SB = 70110/70111
        [70110,70201],[70202,70111],
        ],
        "line-green-pre-glx": [
        #E - Gov. Center NB/SB = 70201/70202, Back of the Hill NB/SB = 70258/70257,
        [70258, 70201], [70202,70257],
        #D - Gov. Center NB/SB = 70201/70202, Woodland NB/SB = 70162/70163
        [70162, 70200], [70196, 70163],
        #C - Gov. Center NB/SB = 70201/70202, Englewood NB/SB = 70236/70235
        [70236, 70201],[70202,70235],
        #B - Boylston NB/SB = 70158/70159, South St. NB/SB = 70116/70111
        [70110, 70158],[70159, 70111],
        ]
    }


def get_stops(line, date):
    if(line == 'line-green'):
        pre_glx = date < GLX_EXTENSION
        return TERMINI['line-green-pre-glx'] if(pre_glx) else TERMINI['line-green-glx']
    return TERMINI[line]

SUB_LINES = {
    'line-green-glx': 'line-green',
}

LINES = ['line-red', 'line-orange', 'line-blue']

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"
GLX_EXTENSION = datetime.strptime('2023-03-19',DATE_FORMAT_BACKEND)

DD_URL_AGG_TT = "https://dashboard-api2.transitmatters.org/aggregate/traveltimes?{parameters}"
DD_URL_SINGLE_TT = "https://dashboard-api2.transitmatters.org/traveltimes/{date}?{parameters}"


def get_monthly_table_update_start():
    ''' Get 1st of current month '''
    yesterday = datetime.today() - timedelta(days=1)
    first_of_month = datetime(yesterday.year, yesterday.month, 1)
    return first_of_month


def get_weekly_table_update_start():
    ''' Get Sunday of current week. '''
    yesterday = datetime.now() - timedelta(days=1)
    days_since_sunday = (yesterday.weekday() + 1) % 7
    most_recent_sunday = yesterday - timedelta(days=days_since_sunday)
    return most_recent_sunday

# Configuration for aggregate speed table functions
TABLE_MAP = {
    "weekly": {
        "table_name": "WeeklySpeed",
        "delta": timedelta(days=7),
        "start_date": datetime.strptime("2016-01-10T08:00:00", DATE_FORMAT),  # Start on first Sunday with data.
        "update_start": get_weekly_table_update_start()
    },
    "monthly": {
        "table_name": "MonthlySpeed",
        "delta": relativedelta(months=1),
        "start_date": datetime.strptime("2016-01-01T08:00:00", DATE_FORMAT),  # Start on 1st of first month with data.
        "update_start": get_monthly_table_update_start()
    }
}
