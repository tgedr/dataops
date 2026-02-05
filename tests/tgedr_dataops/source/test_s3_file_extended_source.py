import boto3
import pytest
from moto import mock_aws

from tgedr_dataops.commons.utils_fs import temp_file
from tgedr_dataops.sink.s3_file_sink import S3FileSink
from tgedr_dataops.source.s3_file_extended_source import S3FileExtendedSource
from tgedr_dataops_abs.source import SourceException

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


@mock_aws
def test_get_metadata_missing_source_context():
    o = S3FileExtendedSource()
    
    with pytest.raises(SourceException, match="you must provide context for source"):
        o.get_metadata(context={})

