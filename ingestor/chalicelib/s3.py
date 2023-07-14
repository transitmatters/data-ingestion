import boto3
import io
import pandas as pd
import zlib
import time

s3 = boto3.client("s3")
cloudfront = boto3.client("cloudfront")


# General downloading/uploading
def download(bucket, key, encoding="utf8", compressed=True):
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
    if compress:
        bytes = zlib.compress(bytes)
    s3.put_object(Bucket=bucket, Key=key, Body=bytes)


def upload_df_as_csv(bucket, key, df):
    key = str(key)

    buffer = io.BytesIO()
    df.to_csv(buffer, compression=None, encoding="utf-8", index=False)
    buffer.seek(0)

    s3.upload_fileobj(buffer, bucket, Key=key, ExtraArgs={"ContentType": "text/csv"})


def download_csv_as_df(bucket, key):
    key = str(key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj["Body"])


def ls(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    all_keys = []
    for page in pages:
        keys = [x["Key"] for x in page["Contents"]]
        all_keys.extend(keys)

    return all_keys


def clear_cf_cache(distribution, keys):
    cloudfront.create_invalidation(
        DistributionId=distribution,
        InvalidationBatch={
            "Paths": {"Quantity": len(keys), "Items": keys},
            "CallerReference": str(time.time()).replace(".", ""),
        },
    )
