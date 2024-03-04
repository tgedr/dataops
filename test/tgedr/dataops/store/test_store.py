import os
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from tgedr.dataops.store.local_fs_single_partition_parquet import (
    LocalFsSinglePartitionParquetStore,
)
from tgedr.dataops.store.store import Store

store: Store = LocalFsSinglePartitionParquetStore()
tmp_folder = tempfile.TemporaryDirectory().name
if not os.path.isdir(tmp_folder):
    os.makedirs(tmp_folder)
dataset_path = os.path.join(tmp_folder, "parquet_experiment")


def test_save():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7],
            "value": [56, 12, 82, 29, 90, 34, 230],
            "country": ["us", "dk", "pt", "dk", "us", "dk", "pt"],
        }
    )
    store.save(df, key=dataset_path, partition_field="country")

    assert 7 == store.get(dataset_path).shape[0]

    filter_condition = ~pc.is_in(pc.field("id"), value_set=pa.array([2, 3]))

    assert 5 == store.get(dataset_path, filter=filter_condition).shape[0]


def test_save_again_and_check():
    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7],
            "value": [56, 12, 82, 29, 90, 34, 230],
            "country": ["us", "dk", "pt", "dk", "us", "dk", "pt"],
        }
    )
    store.save(df, key=dataset_path, partition_field="country")

    assert 7 == store.get(dataset_path).shape[0]


def test_add_records_in_new_partition():
    df = pd.DataFrame({"id": [8, 9], "value": [97, 33], "country": ["it", "it"]})
    store.save(df, key=dataset_path, append=True, partition_field="country")

    assert 9 == store.get(dataset_path).shape[0]


def test_append_to_existing_partition():
    df = pd.DataFrame({"id": [10, 11], "value": [36, 31], "country": ["us", "pt"]})
    store.save(df, key=dataset_path, append=True, partition_field="country")

    assert 11 == store.get(dataset_path).shape[0]


def test_replace_partition_records_alltogether():
    df = pd.DataFrame({"id": [12, 13, 14], "value": [97, 33, 62], "country": ["it", "it", "it"]})
    store.save(df, key=dataset_path, partition_field="country", replace_partitions=True)

    assert 12 == store.get(dataset_path).shape[0]


def test_delete_partitions():
    store.delete(dataset_path, partition_field="country", partitions=["it"])
    assert 9 == store.get(dataset_path).shape[0]


def test_overwrite_dataset():
    df = pd.DataFrame(
        {
            "id": [21, 22, 23, 24],
            "value": [256, 212, 282, 229],
            "metric": ["a", "b", "c", "d"],
            "country": ["us", "dk", "pt", "it"],
        }
    )
    store.save(df, key=dataset_path, partition_field="country")
    assert 4 == store.get(dataset_path).shape[0]


def test_update_rows():
    df = pd.DataFrame({"id": [21], "value": [123], "metric": ["e"], "country": ["us"]})
    store.update(df, key=dataset_path, partition_field="country", key_fields=["id", "country"])

    df = store.get(dataset_path)
    assert 123 == df.loc[df["country"] == "us", "value"].tolist()[0]
    assert 4 == df.shape[0]


def test_delete_rows():
    store.delete(dataset_path, partition_field="country", kv_dict={"id": [21], "value": [123]})
    assert 3 == store.get(dataset_path).shape[0]
