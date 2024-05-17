import os

import boto3
import pytest
from moto import mock_aws

from tgedr.dataops.commons.utils_fs import hash_file, temp_dir
from tgedr.dataops.sink.s3_file_sink import S3FileSink
from tgedr.dataops.source.s3_file_copy import S3FileCopy
from tgedr.dataops.source.s3_file_source import S3FileSource


@pytest.fixture
def local_folder(resources_folder) -> str:
    result = os.path.join(resources_folder, "s3_files")
    return result


@mock_aws
def test_copy_list_with_suffix(local_folder):
    local_file = os.path.join(local_folder, "dummy.txt")

    bucket = "S3FileCopy_A"
    key_folder = "a"
    folder = f"{bucket}/{key_folder}/"
    folder_url = f"s3://{folder}"

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    sink = S3FileSink()
    sink.put(context={"source": local_file, "target": folder_url + "dummy.txt"})
    sink.put(context={"source": local_file, "target": folder_url + "dummy.asc"})

    src = S3FileCopy()
    files = src.list(context={"source": folder_url})
    assert 2 == len(files)

    files = src.list(context={"source": folder_url, "suffix": ".asc"})
    assert 1 == len(files)


@mock_aws
def test_copy(local_folder):
    tmp_dir = temp_dir()
    local_sink_file = os.path.join(tmp_dir, "dummy.txt")
    local_sink_file_2 = os.path.join(tmp_dir, "dummy2.txt")
    local_sink_file_3 = os.path.join(tmp_dir, "dummy3.txt")

    local_file = os.path.join(local_folder, "dummy.txt")
    local_file_hash = hash_file(local_file)
    local_file2 = os.path.join(local_folder, "dummy2.txt")
    local_file2_hash = hash_file(local_file2)

    bucket_a = "S3FileCopy_A"
    bucket_a_key = "a"
    bucket_b = "S3FileCopy_B"
    bucket_b_key = "b"

    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket_a)
    conn.create_bucket(Bucket=bucket_b)

    sink = S3FileSink()
    for lf in [local_file, local_file2]:
        leaf = os.path.basename(lf)
        url = f"s3://{bucket_a}/{bucket_a_key}/{leaf}"
        sink.put(context={"source": local_file, "target": url})

    src = S3FileSource()
    files = src.list(context={"source": f"s3://{bucket_a}/{bucket_a_key}/"})
    actual = src.get(context={"files": files, "target": tmp_dir})
    assert 2 == len(actual)
    hashed_actuals = [hash_file(f) for f in actual]
    assert 0 == len([hf for hf in hashed_actuals if hf not in [local_file_hash, local_file2_hash]])

    cp = S3FileCopy()
    files = cp.list({"source": f"s3://{bucket_a}/{bucket_a_key}/"})
    actual = cp.get({"files": files, "target": bucket_b, "preserve_source_key": "1"})
    expected = [os.path.join(bucket_b, bucket_a_key, "dummy.txt"), os.path.join(bucket_b, bucket_a_key, "dummy2.txt")]
    assert 2 == len(actual)
    assert 2 == len([f for f in actual if f in expected])

    actual = cp.get({"files": files, "target": os.path.join(bucket_b, bucket_b_key)})
    expected = [os.path.join(bucket_b, bucket_b_key, "dummy.txt"), os.path.join(bucket_b, bucket_b_key, "dummy2.txt")]
    assert 2 == len(actual)
    assert 2 == len([f for f in actual if f in expected])

    actual = cp.get({"files": files, "target": os.path.join(bucket_b, bucket_b_key), "preserve_source_key": "1"})
    expected = [
        os.path.join(bucket_b, bucket_b_key, bucket_a_key, "dummy.txt"),
        os.path.join(bucket_b, bucket_b_key, bucket_a_key, "dummy2.txt"),
    ]
    assert 2 == len(actual)
    assert 2 == len([f for f in actual if f in expected])
