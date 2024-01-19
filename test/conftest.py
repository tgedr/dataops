import os
import sys
from typing import List

import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")))  # isort:skip


@pytest.fixture(scope="session")
def spark():
    """
    Returns a Spark session that should be used for unit tests.
    """
    return SparkSession.builder.getOrCreate()


def assert_frames_are_equal(actual: DataFrame, expected: DataFrame, sort_columns: List[str], abs_tol: float = None):
    results_sorted = actual.sort_values(by=sort_columns).reset_index(drop=True)
    expected_sorted = expected.sort_values(by=sort_columns).reset_index(drop=True)
    if abs_tol is not None:
        assert_frame_equal(
            results_sorted,
            expected_sorted,
            check_dtype=False,
            check_exact=False,
            check_like=True,
            atol=abs_tol,
        )
    else:
        assert_frame_equal(
            results_sorted,
            expected_sorted,
            check_dtype=False,
            check_exact=False,
            check_like=True,
        )
