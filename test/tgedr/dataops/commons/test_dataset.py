from pyspark.sql import Row

from tgedr.dataops.commons.dataset import Dataset
from tgedr.dataops.commons.metadata import FieldFrame, Metadata


def test_dataset(spark):
    df = spark.createDataFrame([Row(id=3, country="us")])
    md = Metadata(name="tableX", version="version", framing=[FieldFrame(field="id", lower=3, upper=3)], sources=None)

    o = Dataset(metadata=md, data=df)
    assert "metadata" in o.as_dict() and "data" in o.as_dict()
