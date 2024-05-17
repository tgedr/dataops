# from moto import mock_aws
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from tgedr.dataops.store.s3_single_partition_parquet import (
    S3FsSinglePartitionParquetStore,
)
from tgedr.dataops.store.store import Store

"""
ATTENTION: run this test manually

s3fs depends on `aiobotocore` version that breaks up while running with moto's `mock_aws`, and I
couldn't find a solution to this (https://github.com/aio-libs/aiobotocore/issues/755)

steps required:
- change the name of the file removong the prefix 'MANUAL_'
- define env vars AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN in `.secrets` file
- run the test with the helper script: `./helper.sh test test/nn/gs/ss/dataops/store/test_s3_single_partition_parquet.py`
- after running don't forget to rename this test file back to MANUAL_*
"""

"""
def create_bucket(name: str):
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=name)
"""

BUCKET = "infrastructure-dev-datasets"

STORE: Store = S3FsSinglePartitionParquetStore()
DATASET_PATH = f"s3://{BUCKET}/tmp/test_s3_single_partition_parquet"
DATASET_PATH = f"{BUCKET}/tmp/test_s3_single_partition_parquet"


# @mock_aws
def test_00_schema():
    # create_bucket(name=BUCKET)

    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("value", pa.int64()),
            pa.field("country", pa.string()),
        ]
    )

    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [56, 12, 82],
            "country": ["us", "dk", "pt"],
        }
    )
    STORE.save(df, key=DATASET_PATH, partition_field="country", schema=schema)
    assert 3 == STORE.get(DATASET_PATH, schema=schema).shape[0]

    filter_condition = ~pc.is_in(pc.field("id"), value_set=pa.array([2, 3]))
    assert 1 == STORE.get(DATASET_PATH, filter=filter_condition, schema=schema).shape[0]

    filters = [("country", "in", ["dk"])]
    assert 1 == STORE.get(DATASET_PATH, filters=filters, schema=schema).shape[0]


def test_01_save():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7],
            "value": [56, 12, 82, 29, 90, 34, 230],
            "country": ["us", "dk", "pt", "dk", "us", "dk", "pt"],
        }
    )
    STORE.save(df, key=DATASET_PATH, partition_field="country")

    assert 7 == STORE.get(DATASET_PATH).shape[0]

    filter_condition = ~pc.is_in(pc.field("id"), value_set=pa.array([2, 3]))

    assert 5 == STORE.get(DATASET_PATH, filter=filter_condition).shape[0]

    filters = [("country", "in", ["us", "pt"])]
    assert 4 == STORE.get(DATASET_PATH, filters=filters).shape[0]


def test_02_overwrite():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7],
            "value": [56, 12, 82, 29, 90, 34, 240],
            "country": ["us", "dk", "pt", "dk", "us", "dk", "it"],
        }
    )
    STORE.save(df, key=DATASET_PATH, partition_field="country")
    actual = STORE.get(DATASET_PATH)
    assert 7 == actual.shape[0]
    assert 240 == actual.loc[actual["country"] == "it", "value"].tolist()[0]


def test_03_append():
    df = pd.DataFrame(
        {
            "id": [8, 9],
            "value": [59, 5],
            "country": ["ie", "fr"],
        }
    )
    STORE.save(df, key=DATASET_PATH, partition_field="country", append=True)
    actual = STORE.get(DATASET_PATH)
    assert 9 == actual.shape[0]
    assert 59 == actual.loc[actual["country"] == "ie", "value"].tolist()[0]


def test_04_replace_partitions():
    df = pd.DataFrame({"id": [10, 11, 12], "value": [97, 33, 62], "country": ["it", "it", "it"]})
    STORE.save(df, key=DATASET_PATH, partition_field="country", replace_partitions=True)

    actual = STORE.get(DATASET_PATH)
    assert 11 == actual.shape[0]
    it_values = actual.loc[actual["country"] == "it", "value"].tolist()
    it_values.sort()
    assert [33, 62, 97] == it_values


def test_05_add_in_new_partition():
    df = pd.DataFrame({"id": [13, 14], "value": [97, 33], "country": ["es", "es"]})
    STORE.save(df, key=DATASET_PATH, append=True, partition_field="country")
    assert 13 == STORE.get(DATASET_PATH).shape[0]


def test_06_append_to_existing_partition():
    df = pd.DataFrame({"id": [15, 16], "value": [36, 31], "country": ["us", "pt"]})
    STORE.save(df, key=DATASET_PATH, append=True, partition_field="country")

    actual = STORE.get(DATASET_PATH)
    assert 15 == actual.shape[0]
    assert 2 == len(actual.loc[actual["country"] == "pt", "value"].tolist())
    assert 3 == len(actual.loc[actual["country"] == "us", "value"].tolist())


def test_07_delete_partition():
    STORE.delete(DATASET_PATH, partition_field="country", partition_values=["us"])
    actual = STORE.get(DATASET_PATH)
    assert 12 == actual.shape[0]
    assert 0 == len(actual.loc[actual["country"] == "us", "value"].tolist())


def test_08_delete_rows():
    STORE.delete(DATASET_PATH, partition_field="country", kv_dict={"id": [2]})
    actual = STORE.get(DATASET_PATH)
    assert 11 == actual.shape[0]
    assert 2 == len(actual.loc[actual["country"] == "dk", "value"].tolist())


def test_09_update_rows():
    df = pd.DataFrame({"id": [16], "value": [1], "country": ["pt"]})
    STORE.update(df, key=DATASET_PATH, partition_field="country", key_fields=["id"])

    actual = STORE.get(DATASET_PATH)
    pt_values = actual.loc[actual["country"] == "pt", "value"].tolist()
    assert 2 == len(pt_values)
    pt_values.sort()
    assert [1, 82] == pt_values
