import os
from typing import Optional

import boto3
import pytest
from moto import mock_aws

from tgedr_dataops.commons.utils_fs import hash_file, temp_dir, temp_file
from tgedr_dataops.sink.s3_file_sink import S3FileSink
from tgedr_dataops.source.s3_file_source import S3FileSource
from tgedr_dataops_abs.source import SourceException

BUCKET = "JustABucket"


def create_bucket(name: str):
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=name)


def create_file_in_bucket(key: str, file: Optional[str] = None, suffix: Optional[str] = None):
    if file is None:
        file = temp_file(suffix=suffix)

    target = f"s3://{BUCKET}/{key}/{file}"
    o = S3FileSink()
    o.put(context={"source": file, "target": target})
    return target, hash_file(file)


@mock_aws
def test_list():
    create_bucket(BUCKET)
    key = f"folder1"
    create_file_in_bucket(key=key)
    create_file_in_bucket(key=key, suffix=".xml")

    src_folder = f"s3://{BUCKET}/{key}"
    o = S3FileSource()
    files = o.list(context={"source": src_folder})
    assert 2 == len(files)

    files = o.list(context={"source": src_folder, "suffix": ".xml"})
    assert 1 == len(files)


@mock_aws
def test_sourcing_one_file():
    create_bucket(BUCKET)
    key = "folder2"
    file, hash = create_file_in_bucket(key=key)

    o = S3FileSource()
    files = o.list(context={"source": file})
    assert 1 == len(files)

    target_folder = temp_dir()
    actual = o.get({"files": files, "target": target_folder})
    assert 1 == len(actual)
    assert hash == hash_file(actual[0])

    # force creation of target folder
    target_folder = temp_dir() + "/dummyes/dummydir/"
    actual = o.get({"files": files, "target": target_folder})
    assert 1 == len(actual)
    assert hash == hash_file(actual[0])


@mock_aws
def test_sourcing_multiple_files():
    create_bucket(BUCKET)
    key = f"folder3"
    create_file_in_bucket(key=key)
    file, hash = create_file_in_bucket(key=key, suffix=".xml")

    o = S3FileSource()
    files = o.list(context={"source": os.path.dirname(file)})
    assert 2 == len(files)

    target_folder = temp_dir()
    actual = o.get({"files": files, "target": target_folder})
    assert 2 == len(actual)
    assert hash == hash_file(actual[0])


@mock_aws
def test_list_missing_source_context():
    o = S3FileSource()
    
    with pytest.raises(SourceException, match="you must provide context for source"):
        o.list(context={})


@mock_aws
def test_get_missing_files_context():
    o = S3FileSource()
    
    with pytest.raises(SourceException, match="you must provide context for files"):
        o.get(context={"target": "/tmp"})


@mock_aws
def test_get_missing_target_context():
    o = S3FileSource()
    
    with pytest.raises(SourceException, match="you must provide context for target"):
        o.get(context={"files": []})


@mock_aws
def test_get_to_file_target():
    create_bucket(BUCKET)
    key = "folder4"
    file, hash = create_file_in_bucket(key=key)

    o = S3FileSource()
    files = o.list(context={"source": file})
    assert 1 == len(files)

    target_file = temp_file()
    actual = o.get({"files": files, "target": target_file})
    assert 1 == len(actual)
    assert hash == hash_file(actual[0])

