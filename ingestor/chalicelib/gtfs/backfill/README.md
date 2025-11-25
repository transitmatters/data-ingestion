# GTFS -> dynamo backfill

This is the backfill script that was used to import GTFS schedule data into dynamodb for the data dashboard. It was run once and probably doesn't need to be run again, but here's how you would do it.

## 1. Get AWS access

We have an S3 bucket called `tm-gtfs` that holds an archive of GTFS feeds as sqlite databases, in the format produced by [this tool](https://github.com/transitmatters/mbta-gtfs-sqlite). If for some reason this bucket no longer exists, it's possible to regenerate these files locally from scratch — this will happen automatically if you omit the `s3_bucket` argument to `MbtaGtfsArchive` — but it will be much, much faster to use our precomputed sqlite files.

You'll need an AWS user with a policy which allows it to read and write to this bucket. It will also need read/write access to dynamoDB tables.

Generate a key for this user and hold on to its access key ID and secret key.

## 2. Create a `.env` file

In this directory, add a `.env` file with this structure:

```
AWS_ACCESS_KEY_ID=xxx
AWS_SECRET_ACCESS_KEY=xxx
BACKFILL_START_DATE=2018-01-01 # Or whatever
BACKFILL_END_DATE=2020-01-01 # Or whatever
LOCAL_ARCHIVE_PATH=/path/to/gtfs/archive # Defaults to ./feeds
```

## 3. Run the migration

Make sure you're in the root of the repo, and fire away:

```
cd data-ingestion
uv run python -m ingestor.chalicelib.gtfs.backfill.main
```
