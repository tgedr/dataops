from typing import List

import boto3
import pandas as pd
from moto import mock_aws

from tgedr.dataops.commons.utils_fs import temp_file
from tgedr.dataops.sink.s3_file_sink import S3FileSink
from tgedr.dataops.source.pd_df_s3_source import PdDfS3Source

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
    assert "London" == df["city"][3]
