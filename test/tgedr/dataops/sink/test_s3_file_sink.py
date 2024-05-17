import os

import boto3
from moto import mock_aws

from tgedr.dataops.commons.utils_fs import hash_file, temp_dir, temp_file
from tgedr.dataops.sink.s3_file_sink import S3FileSink
from tgedr.dataops.source.s3_file_source import S3FileSource

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
