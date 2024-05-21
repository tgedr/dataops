from typing import List

import pandas as pd
from pyspark.sql import Row

from tgedr.dataops.commons.utils_fs import temp_dir
from tgedr.dataops.source.local_delta_table import LocalDeltaTable


def test_list(spark):
    key = temp_dir()

    df = spark.createDataFrame(
        [
            Row(id=3, region="america"),
            Row(id=2, region="europe"),
        ]
    )

    df.write.format("delta").mode("overwrite").save(key + "/A")
    df.write.format("delta").mode("overwrite").save(key + "/B")

    o = LocalDeltaTable()
    actual: List[str] = o.list(context={"url": key})
    actual.sort()
    assert ["A", "B"] == actual


def test_get(spark):
    key = temp_dir()

    df = spark.createDataFrame(
        [
            Row(id=3, region="america"),
            Row(id=2, region="europe"),
        ]
    )

    df.write.format("delta").mode("overwrite").save(key)

    o = LocalDeltaTable()

    actual: pd.DataFrame = o.get(context={"url": key})
    assert 2 == actual.shape[0]
    assert 2 == actual.shape[1]

    actual: pd.DataFrame = o.get(context={"url": key, "columns": ["region"]})
    assert 2 == actual.shape[0]
    assert 1 == actual.shape[1]

    assert ["america", "europe"] == list((actual.to_dict()["region"].values()))
