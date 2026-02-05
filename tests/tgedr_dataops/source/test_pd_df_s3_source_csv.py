from typing import List

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from tgedr_dataops.commons.utils_fs import temp_file
from tgedr_dataops.sink.s3_file_sink import S3FileSink
from tgedr_dataops.source.pd_df_s3_source import PdDfS3Source
from tgedr_dataops_abs.source import SourceException

BUCKET = "JustABucket"


def create_bucket(name: str):
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=name)


def create_data_in_bucket(url: str):
    data = {
        "name": ["John", "Anna", "Peter", "Linda"],
        "age": [28, 23, 34, 29],
        "city": ["New York", "Paris", "Berlin", "London"],
    }
    df = pd.DataFrame(data)
    local_file = temp_file(suffix=".csv")
    df.to_csv(local_file, index=False, header=False, sep="$")

    o = S3FileSink()
    o.put(context={"source": local_file, "target": url})


@mock_aws
def test_list():
    create_bucket(BUCKET)
    url = f"s3://{BUCKET}/dataset"
    url1 = f"{url}/file1.csv"
    url2 = f"{url}/file2.csv"
    create_data_in_bucket(url1)
    create_data_in_bucket(url2)

    source = PdDfS3Source()
    actual: List[str] = source.list({"url": url})
    actual.sort()
    assert ["s3://JustABucket/dataset/file1.csv", "s3://JustABucket/dataset/file2.csv"] == actual


@mock_aws
def test_get():
    create_bucket(BUCKET)
    url = f"s3://{BUCKET}/dataset/file.csv"
    create_data_in_bucket(url)

    source = PdDfS3Source()
    df = source.get({"url": url, "no_header": 1, "sep": "$", "column_names": ["name", "age", "city"]})
    assert "John" == df["name"][0]


@mock_aws
def test_list_with_suffix_filter():
    create_bucket(BUCKET)
    url = f"s3://{BUCKET}/dataset"
    url1 = f"{url}/file1.csv"
    url2 = f"{url}/file2.txt"
    url3 = f"{url}/file3.csv"
    create_data_in_bucket(url1)
    create_data_in_bucket(url2)
    create_data_in_bucket(url3)

    source = PdDfS3Source()
    actual: List[str] = source.list({"url": url, "suffix": ".csv"})
    actual.sort()
    assert ["s3://JustABucket/dataset/file1.csv", "s3://JustABucket/dataset/file3.csv"] == actual


@mock_aws
def test_list_missing_url_context():
    source = PdDfS3Source()
    
    with pytest.raises(SourceException, match="you must provide context for url"):
        source.list({})


@mock_aws
def test_get_missing_url_context():
    source = PdDfS3Source()
    
    with pytest.raises(SourceException, match="you must provide context for url"):
        source.get({})


@mock_aws
def test_get_invalid_format():
    create_bucket(BUCKET)
    url = f"s3://{BUCKET}/dataset/file.csv"
    create_data_in_bucket(url)
    
    source = PdDfS3Source()
    
    with pytest.raises(SourceException, match="invalid format"):
        source.get({"url": url, "file_format": "invalid_format"})

