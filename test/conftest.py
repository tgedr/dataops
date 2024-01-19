
import hashlib
import os
import sys
from typing import AnyStr, List


import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal

# from pyspark.sql import SparkSession

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), "src")))  # isort:skip

"""
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()
"""


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


def hash_file(filepath, hash_func=hashlib.sha256) -> AnyStr:
    """Generate a hash for a file.

    Args:
        filepath (str): The path to the file.
        hash_func: A hashlib hash function, e.g., hashlib.md5().

    Returns:
        str: The hexadecimal hash string of the file.
    """
    # Initialize the hash object
    hasher = hash_func()

    # Open the file in binary read mode
    with open(filepath, "rb") as file:
        # Read the file in chunks to avoid using too much memory
        chunk_size = 8192
        while chunk := file.read(chunk_size):
            hasher.update(chunk)

    # Return the hexadecimal digest of the hash
    return hasher.hexdigest()


@pytest.fixture(scope="session")
def resources_folder() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "resources"))


@pytest.fixture
def aws_real_test_env(monkeypatch):
    monkeypatch.setenv("S3_CONNECTOR_USE_CREDENTIALS", "0")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "XPTO")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "XPTO")
    monkeypatch.setenv("AWS_REGION", "eu-central-1")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "XPTO")
