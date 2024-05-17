import os
from datetime import datetime
from test.conftest import assert_frames_are_equal

import pytest
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F

from tgedr.dataops.commons.metadata import FieldFrame, Metadata
from tgedr.dataops.store.spark_delta import SparkDeltaStore


@pytest.fixture
def tmp_dir(temporary_folder) -> str:
    return os.path.join(temporary_folder, "test_spark_parquet")


@pytest.fixture
def data(spark) -> DataFrame:
    now: float = datetime.now().timestamp()
    d = [
        Row(id=3, country="us", time=now, region="america"),
        Row(id=2, country="dk", time=now, region="europe"),
    ]
    return spark.createDataFrame(d)


@pytest.fixture
def data2(spark) -> DataFrame:
    now: float = datetime.now().timestamp()
    d = [
        Row(id=4, country="jp", time=now, region="asia"),
        Row(id=2, country="pt", time=now, region="europe"),
    ]
    return spark.createDataFrame(d)


@pytest.fixture
def data3(spark) -> DataFrame:
    now: float = datetime.now().timestamp()
    d = [
        Row(id=3, country="us", time=now, region="america"),
        Row(id=4, country="jp", time=now, region="asia"),
        Row(id=2, country="pt", time=now, region="europe"),
    ]
    return spark.createDataFrame(d)


def test_01_save(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    df = o.get(key=tmp_dir)
    assert_frames_are_equal(df.toPandas(), data.toPandas(), sort_columns=["id"])


def test_02_save_append(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    table_version = o.get_latest_table_versions(path=tmp_dir)[0]
    o.save(df=data, key=tmp_dir, append=True)
    df = o.get(key=tmp_dir)
    assert df.count() == (2 * data.count())
    df = o.get(key=tmp_dir, version=table_version)
    assert df.count() == (data.count())


def test_03_save_overwrite(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    df = o.get(key=tmp_dir)
    assert df.count() == (data.count())


def test_04_save_with_partitions(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    assert 0 == len(o._get_table_partitions(path=tmp_dir))
    o.save(df=data, key=tmp_dir, partition_fields=["country"])
    assert 1 == len(o._get_table_partitions(path=tmp_dir))
    o.save(df=data, key=tmp_dir, partition_fields=["region", "country"])
    assert 2 == len(o._get_table_partitions(path=tmp_dir))
    o.save(df=data, key=tmp_dir, partition_fields=["country"])
    assert 1 == len(o._get_table_partitions(path=tmp_dir))


def test_05_metadata(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    expected = Metadata(name="xpto", version="2", framing=None, sources=None)
    o.save(df=data, key=tmp_dir, metadata=expected)
    table_version = o.get_latest_table_versions(path=tmp_dir)[0]
    actual = o.get_metadata(path=tmp_dir)
    assert expected == actual
    expected2 = Metadata(name="xpto", version="3", framing=[FieldFrame(field="id", lower=2, upper=3)], sources=None)
    o.save(df=data, key=tmp_dir, metadata=expected2)
    actual = o.get_metadata(path=tmp_dir)
    assert expected2 == actual
    actual = o.get_metadata(path=tmp_dir, version=table_version)
    assert expected <= actual


def test_06_delete_all(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    assert o.get(key=tmp_dir).count() == data.count()

    # test numeric criteria
    o.delete(key=tmp_dir)
    assert 0 == o.get(key=tmp_dir).count()

    o.save(df=data, key=tmp_dir)
    # force date criteria
    condition = o._SparkDeltaStore__get_deletion_criteria(data.drop("id"))
    o.delete(key=tmp_dir, condition=condition)
    assert 0 == o.get(key=tmp_dir).count()

    o.save(df=data, key=tmp_dir)
    # force string criteria
    condition = o._SparkDeltaStore__get_deletion_criteria(data.drop("id").drop("time"))
    o.delete(key=tmp_dir, condition=condition)
    assert 0 == o.get(key=tmp_dir).count()


def test_06_delete_one_row(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    o.delete(key=tmp_dir, condition=(F.col("id") == 3))
    df = o.get(key=tmp_dir)
    assert 1 == df.count()


def test_07_update_one_row(environment_mock, data, data2, data3, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    o.update(df=data2, key=tmp_dir, match_fields=["id"])
    actual = o.get(key=tmp_dir)
    assert_frames_are_equal(actual.toPandas(), data3.toPandas(), sort_columns=["id"])


def test_08_update_one_row_with_partition_change(environment_mock, data, data2, data3, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir, partition_fields=["region", "country"])
    o.update(df=data2, key=tmp_dir, match_fields=["id"], partition_fields=["country"])
    actual = o.get(key=tmp_dir)
    assert_frames_are_equal(actual.toPandas(), data3.toPandas(), sort_columns=["id"])
    assert ["country"] == o._get_table_partitions(path=tmp_dir)


def test_09_set_column_comments(environment_mock, spark, tmp_dir):
    df = spark.createDataFrame(
        [
            Row(id=3, country="us", region="america"),
            Row(id=2, country="dk", region="europe"),
        ]
    )

    o = SparkDeltaStore()
    o.save(df=df, key=tmp_dir, partition_fields=["region", "country"], table_name="dummy.test_09_set_column_comments")
    id_description: DataFrame = spark.sql(f"describe dummy.test_09_set_column_comments").filter(
        F.col("col_name") == "id"
    )
    row: dict = (id_description.collect()[0]).asDict()
    assert "" == row["comment"]

    column_descriptions = {
        "id": "unique id\\'s",
        "country": "country of the data event took place",
        "region": "country_region",
    }
    o.set_column_comments(db="dummy", table="test_09_set_column_comments", col_comments=column_descriptions)

    id_description: DataFrame = spark.sql(f"describe dummy.test_09_set_column_comments").filter(
        F.col("col_name") == "id"
    )
    row: dict = (id_description.collect()[0]).asDict()
    assert "unique id's" == row["comment"]


def test_10_schema_change(environment_mock, data, tmp_dir):
    o = SparkDeltaStore()
    o.save(df=data, key=tmp_dir)
    assert False == o._has_schema_changed(tmp_dir, data)

    df = data.withColumn("dummy", F.lit(0))
    assert o._has_schema_changed(tmp_dir, df)
