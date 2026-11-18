import os

import boto3
import pytest
from moto import mock_aws

from tgedr_dataops.commons.utils_fs import hash_file, temp_dir, temp_file
from tgedr_dataops.sink.s3_file_sink import S3FileSink
from tgedr_dataops.source.s3_file_source import S3FileSource
from tgedr_dataops_abs.sink import SinkException

BUCKET = "JustABucket"


def create_bucket(name: str):
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=name)


@mock_aws
def test_put_file_get_file(resources_folder):
    src_file = temp_file()
    hash = hash_file(src_file)

    create_bucket(name=BUCKET)
    target_file_key = f"s3://{BUCKET}/tmp/dummy.txt"

    o = S3FileSink()
    o.put(context={"source": src_file, "target": target_file_key})

    u = S3FileSource()
    files = u.list(context={"source": target_file_key})
    assert 1 == len(files)
    local_sink_file = os.path.join(temp_dir(), "dummy.txt")
    actual = u.get(context={"files": files, "target": local_sink_file})
    assert 1 == len(actual)
    assert hash == hash_file(local_sink_file)


@mock_aws
def test_put_in_folder_key():
    src_file = temp_file()
    hash = hash_file(src_file)

    create_bucket(name=BUCKET)
    target_folder_key = f"s3://{BUCKET}/tmp2/"

    o = S3FileSink()
    o.put(context={"source": src_file, "target": target_folder_key})

    u = S3FileSource()
    files = u.list(context={"source": target_folder_key})
    assert 1 == len(files)
    local_sink_file = os.path.join(temp_dir(), "dummy.txt")
    actual = u.get(context={"files": files, "target": local_sink_file})
    assert 1 == len(actual)
    assert hash == hash_file(local_sink_file)


@mock_aws
def test_put_missing_source_context():
    o = S3FileSink()
    
    with pytest.raises(SinkException, match="you must provide context for source"):
        o.put(context={"target": "s3://bucket/key"})


@mock_aws
def test_put_missing_target_context():
    o = S3FileSink()
    
    with pytest.raises(SinkException, match="you must provide context for target"):
        o.put(context={"source": "/tmp/file"})


@mock_aws
def test_put_source_is_directory():
    o = S3FileSink()
    dst_folder = temp_dir()
    
    with pytest.raises(SinkException, match="source can't be a folder"):
        o.put(context={"source": dst_folder, "target": "s3://bucket/key"})


@mock_aws
def test_delete_file():
    src_file = temp_file()
    src_file_2 = temp_file()
    
    create_bucket(name=BUCKET)
    target_file_key = f"s3://{BUCKET}/tmp/dummy.txt"
    target_file_key_2 = f"s3://{BUCKET}/tmp/dummy2.txt"
    
    o = S3FileSink()
    o.put(context={"source": src_file, "target": target_file_key})
    o.put(context={"source": src_file_2, "target": target_file_key_2})
    
    # Verify files exist
    u = S3FileSource()
    files = u.list(context={"source": f"s3://{BUCKET}/tmp/"})
    assert 2 == len(files)
    
    # Delete one file
    o.delete(context={"target": target_file_key})
    
    # Verify only one file remains
    files = u.list(context={"source": f"s3://{BUCKET}/tmp/"})
    assert 1 == len(files)
    assert target_file_key_2 in files[0]




@mock_aws
def test_delete_missing_target_context():
    o = S3FileSink()
    
    with pytest.raises(SinkException, match="you must provide context for target"):
        o.delete(context={})

