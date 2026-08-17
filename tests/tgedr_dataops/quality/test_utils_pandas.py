import pandas as pd
import pytest

from tgedr_dataops.quality.utils_pandas import validate_schema


def test_validate_schema_passes_for_matching_schema():
    df = pd.DataFrame({"a": pd.array([1], dtype="Int64"), "b": ["x"]})
    validate_schema(df, {"a": "Int64", "b": "object"})


def test_validate_schema_raises_for_schema_mismatch():
    df = pd.DataFrame({"a": [1.0]})
    with pytest.raises(ValueError, match="Schema mismatch"):
        validate_schema(df, {"a": "Int64"})
