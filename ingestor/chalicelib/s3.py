import boto3
import io
import zlib

s3 = boto3.client('s3')

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
    df.to_csv(buffer, compression=None, encoding='utf-8', index=False)
    buffer.seek(0)

    s3.upload_fileobj(buffer, bucket, Key=key,
        ExtraArgs={'ContentType': 'text/csv'})
