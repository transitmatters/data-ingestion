"""Smoke-test a shrunk Lambda layer inside the Lambda Python runtime image.

`shrink` and `strip_binaries` in helpers.sh delete or rewrite parts of the layer that we believe are
never needed. This imports the deployed app and every chalicelib module, and exercises the library
paths we use, so an over-eager trim fails CI instead of failing in production. Run via `smoke_layer`.
"""

import io
import pkgutil
import sqlite3
from zoneinfo import ZoneInfo

import boto3
import numpy as np
import pandas as pd
import pytz
import sqlalchemy
from boto3.dynamodb.conditions import Key

# The deployed handlers, plus every chalicelib module except the backfill scripts, which read
# their arguments from the environment at import and only run outside Lambda
import app  # noqa: F401
import chalicelib

for module in pkgutil.walk_packages(chalicelib.__path__, "chalicelib."):
    if not any(part in module.name for part in (".tests", ".backfill", "__main__")):
        __import__(module.name)

# numpy's C core and its vendored OpenBLAS (numpy.libs, which strip_binaries leaves alone)
assert np.dot(np.eye(3), np.arange(3.0)).tolist() == [0.0, 1.0, 2.0]

# CSV and Excel round trips, the way s3.py and ridership/process.py read files
df = pd.DataFrame({"line": ["Red", "Orange", "Blue"], "count": [1, 2, 3]})
read = pd.read_csv(io.StringIO(df.to_csv(index=False)))
assert read["count"].sum() == 6, read
excel = io.BytesIO()
df.to_excel(excel, index=False)
excel.seek(0)
assert len(pd.read_excel(excel)) == 3

# Resample and timezone conversion, as in agg_speed_tables.py and bluebikes.py
times = pd.to_datetime(pd.Series([1735707600, 1738386000]), unit="s", utc=True).dt.tz_convert("US/Eastern")
assert len(pd.Series([1, 2], index=times).resample("ME").sum()) == 2

# mbta_gtfs_sqlite reads feeds through SQLAlchemy's sqlite dialect
engine = sqlalchemy.create_engine("sqlite://", module=sqlite3)
with engine.connect() as conn:
    assert conn.execute(sqlalchemy.text("select 1")).scalar() == 1

# Clients for the services we call; shrink deletes every other botocore service model
boto3.client("s3")
boto3.client("cloudfront")
boto3.resource("s3")
boto3.resource("sqs")
boto3.resource("dynamodb")
Key("date").eq("2026-01-01")

# Timezones resolve from the layer's tzdata/pytz (shrink drops most zones but keeps US/*)
ZoneInfo("US/Eastern")
pytz.timezone("US/Eastern")

print(f"Layer smoke test passed: pandas {pd.__version__}, numpy {np.__version__}, boto3 {boto3.__version__}")
