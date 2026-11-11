import boto3
from moto import mock_aws

from .. import s3 as s3_module


def test_ls_returns_empty_list_when_prefix_has_no_objects(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        client.put_object(Bucket="test-bucket", Key="other/file.txt", Body=b"x")
        monkeypatch.setattr(s3_module, "s3", client)

        assert s3_module.ls("test-bucket", "missing/prefix/") == []
