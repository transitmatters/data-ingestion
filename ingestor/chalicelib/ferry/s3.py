import glob
import boto3
from io import BytesIO
import gzip
import os

from disk import DATA_DIR


s3 = boto3.client("s3")

S3_BUCKET = "tm-mbta-performance"

S3_DATA_TEMPLATE = "Events/{relative_path}.gz"


def _compress_and_upload_file(fp: str):
    """Compress a file in-memory and upload to S3."""
    # generate output location
    rp = os.path.relpath(fp, DATA_DIR)
    s3_key = S3_DATA_TEMPLATE.format(relative_path=rp)

    with open(fp, "rb") as f:
        # gzip to buffer and upload
        gz_bytes = gzip.compress(f.read())
        buffer = BytesIO(gz_bytes)

        s3.upload_fileobj(
            buffer, S3_BUCKET, Key=s3_key, ExtraArgs={"ContentType": "text/csv", "ContentEncoding": "gzip"}
        )


def upload_events_to_s3():
    """Upload all events data to the TM s3 bucket."""

    # get all CSV files in the data directory
    all_events_files = glob.glob(str(DATA_DIR / "daily-*/*/Year=*/Month=*/Day=*/events.csv"))

    print(f"Found {len(all_events_files)} files to upload")

    # upload them to s3, gzipped
    for fp in all_events_files:
        _compress_and_upload_file(fp)
        print(f"Uploaded: {fp}")


if __name__ == "__main__":
    upload_events_to_s3()
