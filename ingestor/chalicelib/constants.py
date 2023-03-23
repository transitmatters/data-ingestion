'''array of stop pairs which encompass entire system. Not actually termini - one before the termini.'''


TERMINI = {
    # RL: Davis SB = 70063, Shawmut SB = 70091, Quincy Adams SB = 70103, Quincy Adams NB = 70104, Davis NB = 70064, Shawmut NB = 70092
    "RL": [[70063, 70091], [70092, 70064], [70063, 70103], [70104, 70064]],
    # OL: Malden Center SB = 70034, Green Street SB = 70002, Green Street NB = 70003, Malden Center NB 70035
    "OL": [[70034, 70002], [70003, 70035]],
    # BL: Revere Beach SB = 70057, Gov. Center SB = 70039, Revere Beach NB = 70058, Gov. Center NB 70040
    "BL": [[70057, 70039], [70040, 70058]],
    }

LINES = ['RL', 'OL', 'BL']

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DATE_FORMAT_BACKEND = "%Y-%m-%d"

DD_URL_AGG_TT = "https://dashboard-api2.transitmatters.org/aggregate/traveltimes?{parameters}"
DD_URL_SINGLE_TT = "https://dashboard-api2.transitmatters.org/traveltimes/{date}?{parameters}"
