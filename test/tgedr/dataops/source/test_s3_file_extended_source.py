import boto3
from moto import mock_aws

from tgedr.dataops.commons.utils_fs import temp_file
from tgedr.dataops.sink.s3_file_sink import S3FileSink
from tgedr.dataops.source.s3_file_extended_source import S3FileExtendedSource

BUCKET = "JustABucket"


def create_bucket(name: str):
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=name)


def create_file_in_bucket(bucket: str, key: str):
    file = temp_file()
    target = f"s3://{bucket}/{key}/{file}"
    o = S3FileSink()
    o.put(context={"source": file, "target": target})
    return file


@mock_aws
def test_get_metadata():
    create_bucket(BUCKET)
    key = f"folder1"
    file = create_file_in_bucket(bucket=BUCKET, key=key)

    src = f"s3://{BUCKET}/{key}/{file}"
    o = S3FileExtendedSource()
    actual = o.get_metadata(context={"source": src})
    assert 4 == len(actual.keys())
