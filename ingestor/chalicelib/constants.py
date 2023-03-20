'''array of stop pairs which encompass entire system. Not actually termini - one before the termini.'''
TERMINI = {
    "RL": [[70063, 70091], [70092, 70064], [70063, 70103], [70104, 70064]],
    "OL": [[70034, 70002], [70003, 70035]],
    "BL": [[70057, 70039], [70040, 70058]],
    }

LINES = ['RL', 'OL', 'BL']

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"

DD_URL_AGG_TT = "https://dashboard-api2.transitmatters.org/aggregate/traveltimes?{parameters}"
DD_URL_SINGLE_TT = "https://dashboard-api2.transitmatters.org/traveltimes/{date}?{parameters}"
