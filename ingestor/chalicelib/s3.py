import io
import time
import zlib

import boto3
import pandas as pd

s3 = boto3.client("s3")
cloudfront = boto3.client("cloudfront")


def download(bucket, key, encoding="utf8", compressed=True):
    """Downloads and decodes an object from S3.

    Args:
        bucket: The S3 bucket name.
        key: The S3 object key.
        encoding: Character encoding for decoding. Defaults to "utf8".
        compressed: Whether the object is zlib/gzip compressed. Defaults to True.

    Returns:
        The decoded string content of the S3 object.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    s3_data = obj["Body"].read()
    if not compressed:
        return s3_data.decode(encoding)
    # 32 should detect zlib vs gzip
    decompressed = zlib.decompress(s3_data, zlib.MAX_WBITS | 32).decode(encoding)
    return decompressed


# TODO: confirm if we want zlib or gzip compression
# note: alerts are zlib, but dashboard download code can handle either (in theory)
def upload(bucket, key, bytes, compress=True):
    """Uploads data to S3, optionally compressing it first.

    Args:
        bucket: The S3 bucket name.
        key: The S3 object key.
        bytes: The data to upload (bytes or string).
        compress: Whether to zlib-compress the data. Defaults to True.
    """
    if compress:
        bytes = zlib.compress(bytes)
    s3.put_object(Bucket=bucket, Key=key, Body=bytes)


def upload_df_as_csv(bucket, key, df):
    """Uploads a pandas DataFrame to S3 as a CSV file.

    Args:
        bucket: The S3 bucket name.
        key: The S3 object key.
        df: The DataFrame to upload.
    """
    key = str(key)

    buffer = io.BytesIO()
    df.to_csv(buffer, compression=None, encoding="utf-8", index=False)
    buffer.seek(0)

    s3.upload_fileobj(buffer, bucket, Key=key, ExtraArgs={"ContentType": "text/csv"})


def download_csv_as_df(bucket, key):
    """Downloads a CSV file from S3 and returns it as a pandas DataFrame.

    Args:
        bucket: The S3 bucket name.
        key: The S3 object key.

    Returns:
        A DataFrame parsed from the CSV content.
    """
    key = str(key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def ls(bucket, prefix):
    """Lists all object keys in an S3 bucket matching a prefix.

    Args:
        bucket: The S3 bucket name.
        prefix: The key prefix to filter by.

    Returns:
        A list of S3 object key strings.
    """
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_keys = []
    for page in pages:
        keys = [x["Key"] for x in page["Contents"]]
        all_keys.extend(keys)

    return all_keys


def clear_cf_cache(distribution: str, keys: list[str]):
    """Invalidates CloudFront cache for the specified paths.

    Args:
        distribution: The CloudFront distribution ID.
        keys: A list of path patterns to invalidate.
    """
    cloudfront.create_invalidation(
        DistributionId=distribution,
        InvalidationBatch={
            "Paths": {"Quantity": len(keys), "Items": keys},
            "CallerReference": str(time.time()).replace(".", ""),
        },
    )
