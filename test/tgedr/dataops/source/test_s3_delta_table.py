from typing import List, Optional

import boto3
import pandas as pd
import pytest
from moto import mock_aws
from pyspark.sql import DataFrame, Row

import tgedr.dataops.source.s3_delta_table as s3dt
from tgedr.dataops.commons.utils_fs import temp_file
from tgedr.dataops.sink.s3_file_sink import S3FileSink
from tgedr.dataops.source.s3_delta_table import S3DeltaTable

BUCKET = "JustABucket"


def create_bucket(name: str):
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=name)


def create_dummy_file_in_bucket(bucket: str, key: str, dst_file: Optional[str] = None):
    dummy = temp_file()

    target = f"s3://{bucket}/{key}/{dst_file}"
    o = S3FileSink()
    o.put(context={"source": dummy, "target": target})
    return target


@pytest.fixture
def data(spark) -> DataFrame:
    d = [
        Row(id=3, country="us", region="america"),
        Row(id=2, country="dk", region="europe"),
    ]
    return spark.createDataFrame(d)


@mock_aws
def test_list():
    create_bucket(BUCKET)
    datasets: List[str] = ["A", "B"]
    for dataset in datasets:
        key = f"ss/datasets/{dataset}/_delta_log"
        create_dummy_file_in_bucket(bucket=BUCKET, key=key, dst_file="001.json")

    url = f"s3://{BUCKET}/ss/datasets"
    o = S3DeltaTable()
    actual: List[str] = o.list(context={"url": url})
    actual.sort()

    assert actual == ["ss/datasets/A", "ss/datasets/B"]


def test_get(monkeypatch, environment_mock, spark):
    key = "dummy"

    class MockS3DatasetDeltaTable:
        def __init__(self, table_uri, storage_options, without_files):
            pass

        def to_pandas(self, columns=None):
            df = pd.DataFrame({"id": [3, 2], "region": ["america", "europe"]})
            if columns is not None:
                df = df[columns]
            return df

    monkeypatch.setattr(s3dt, "DeltaTable", MockS3DatasetDeltaTable)
    o = S3DeltaTable()

    actual: pd.DataFrame = o.get(context={"url": f"s3://{key}"})
    assert 2 == actual.shape[0]
    assert 2 == actual.shape[1]

    actual: pd.DataFrame = o.get(context={"url": f"s3://{key}", "columns": ["region"]})
    assert 2 == actual.shape[0]
    assert 1 == actual.shape[1]

    assert ["america", "europe"] == list((actual.to_dict()["region"].values()))
