from datetime import datetime

import pytest
from pyspark.sql import DataFrame, Row

from tgedr.dataops.validation.abs import DataValidation, DataValidationException
from tgedr.dataops.validation.pyspark import Impl


@pytest.fixture
def df(spark) -> DataFrame:
    now: float = datetime.now()
    d = [
        Row(id=3, country="us", time=now, value=2.3),
        Row(id=2, country="dk", time=now, value=100.12),
    ]
    return spark.createDataFrame(d)


def test_get_impl():
    assert type(Impl()) == type(DataValidation.get_impl("pyspark"))


def test_get_impl_x():
    with pytest.raises(DataValidationException):
        assert type(Impl()) == type(DataValidation.get_impl("pysparkx"))


def test_validation_column_set(df):
    expectations = {
        "expectation_suite_name": "column_set",
        "expectations": [
            {
                "expectation_type": "expect_table_columns_to_match_set",
                "kwargs": {"column_set": ["id", "value", "country", "time"]},
                "meta": {},
            },
        ],
    }
    o = DataValidation.get_impl("pyspark")

    outcome = o.validate(df=df, expectations=expectations)
    assert outcome["success"] is True


def test_validation_column_types(df):
    expectations = {
        "expectation_suite_name": "column_types",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "id", "type_": "LongType"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "value", "type_": "DoubleType"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "time", "type_": "TimestampType"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "country", "type_": "StringType"},
                "meta": {},
            },
        ],
    }
    o = DataValidation.get_impl("pyspark")

    outcome = o.validate(df=df, expectations=expectations)
    assert outcome["success"] is True, [x["exception_info"] for x in outcome["results"]]
