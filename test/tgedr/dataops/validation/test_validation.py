import pandas as pd
import pytest

from tgedr.dataops.validation.abs import DataValidation, DataValidationException
from tgedr.dataops.validation.pandas import Impl


@pytest.fixture
def pandas_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7],
            "value": [56.23, 12.3, 82.0, 29.2, 90.4, 34.6, 230.9],
            "country": ["us", "dk", "pt", "dk", "us", "dk", "pt"],
        }
    )


def test_get_impl():
    assert type(Impl()) == type(DataValidation.get_impl("pandas"))


def test_get_impl_x():
    with pytest.raises(DataValidationException):
        assert type(Impl()) == type(DataValidation.get_impl("pandasx"))


def test_pandas_validation_column_set(pandas_df):
    expectations = {
        "expectation_suite_name": "column_set",
        "expectations": [
            {
                "expectation_type": "expect_table_columns_to_match_set",
                "kwargs": {"column_set": ["id", "value", "country"]},
                "meta": {},
            },
        ],
    }
    o = DataValidation.get_impl("pandas")

    outcome = o.validate(df=pandas_df, expectations=expectations)
    assert outcome["success"] is True


def test_pandas_validation_column_types(pandas_df):
    expectations = {
        "expectation_suite_name": "column_types",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "id", "type_": "int"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "value", "type_": "float"},
                "meta": {},
            },
            {
                "expectation_type": "expect_column_values_to_be_of_type",
                "kwargs": {"column": "country", "type_": "str"},
                "meta": {},
            },
        ],
    }
    o = DataValidation.get_impl("pandas")

    outcome = o.validate(df=pandas_df, expectations=expectations)
    assert outcome["success"] is True, [x["exception_info"] for x in outcome["results"]]
