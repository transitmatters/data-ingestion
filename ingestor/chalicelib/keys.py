#  This file can't be named secrets.py: https://github.com/numpy/numpy/issues/14860
import os

MBTA_V3_API_KEY = os.environ.get("MBTA_V3_API_KEY", "")

YANKEE_API_KEY = os.environ.get("YANKEE_API_KEY", "")
