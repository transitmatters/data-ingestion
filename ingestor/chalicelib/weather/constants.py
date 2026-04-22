BUCKET = "tm-mbta-performance"

# Downtown Boston (near City Hall)
LATITUDE = 42.3601
LONGITUDE = -71.0589

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

HOURLY_FIELDS = "temperature_2m,weather_code,precipitation,relative_humidity_2m,wind_speed_10m"

# WMO weather codes → coarse condition labels the frontend can rely on.
# https://open-meteo.com/en/docs — "Weather variable documentation"
WEATHER_CODE_TO_CONDITION = {
    0: "clear",   # Clear sky
    1: "clear",   # Mainly clear
    2: "cloudy",  # Partly cloudy
    3: "cloudy",  # Overcast
    45: "fog",    # Fog
    48: "fog",    # Depositing rime fog
    51: "rain",   # Light drizzle
    53: "rain",   # Moderate drizzle
    55: "rain",   # Dense drizzle
    56: "rain",   # Light freezing drizzle
    57: "rain",   # Dense freezing drizzle
    61: "rain",   # Slight rain
    63: "rain",   # Moderate rain
    65: "rain",   # Heavy rain
    66: "rain",   # Light freezing rain
    67: "rain",   # Heavy freezing rain
    71: "snow",   # Slight snow fall
    73: "snow",   # Moderate snow fall
    75: "snow",   # Heavy snow fall
    77: "snow",   # Snow grains
    80: "rain",   # Slight rain showers
    81: "rain",   # Moderate rain showers
    82: "rain",   # Violent rain showers
    85: "snow",   # Slight snow showers
    86: "snow",   # Heavy snow showers
    95: "storm",  # Thunderstorm (slight or moderate)
    96: "storm",  # Thunderstorm with slight hail
    99: "storm",  # Thunderstorm with heavy hail
}
# Thunderstorm forecast with hail is only available in Central Europe


def key(day):
    return f"Weather/hourly/{str(day)}.json.gz"
