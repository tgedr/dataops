"""Integration tests for PandasValidationImpl validating actual pandas DataFrames.

These tests use Great Expectations 1.9.1 API without mocking.
"""

import pandas as pd
import pytest
from src.tgedr_dataops.quality.pandas_validation import PandasValidation


def test_pandas_dataframe_validation_success():
    """Test successful validation of a pandas DataFrame."""
    # Create test DataFrame
    df = pd.DataFrame({
        "id": [1, 2, 3, 4],
        "name": ["Alice", "Bob", "Charlie", "Diana"],
        "age": [25, 30, 35, 28],
        "score": [85.5, 92.0, 78.5, 88.0]
    })
    
    # Define expectations
    expectations = {
        "expectation_suite_name": "user_data_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "name"}
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "name"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 18, "max_value": 100}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "score", "min_value": 0.0, "max_value": 100.0}
            }
        ]
    }
    
    # Validate
    impl = PandasValidation()
    result = impl.validate(df, expectations)
    
    # Verify results
    assert result is not None
    assert isinstance(result, dict)
    assert "success" in result
    assert result["success"] is True, f"Expected validation to succeed, got: {result}"
    assert "results" in result
    assert "statistics" in result
    assert result["statistics"]["successful_expectations"] == 4
    assert result["statistics"]["unsuccessful_expectations"] == 0


def test_pandas_dataframe_validation_with_failures():
    """Test validation failure when data doesn't meet expectations."""
    # Create DataFrame with issues
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", None, "Charlie"],  # Has null value
        "age": [25, 30, 150],  # Age 150 exceeds max
    })
    
    expectations = {
        "expectation_suite_name": "strict_user_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "name"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 18, "max_value": 100}
            }
        ]
    }
    
    # Validate
    impl = PandasValidation()
    result = impl.validate(df, expectations)
    
    # Verify validation failed
    assert result is not None
    assert "success" in result
    assert result["success"] is False, "Expected validation to fail"
    assert "results" in result
    assert len(result["results"]) > 0, "Should have failure details"
    assert result["statistics"]["unsuccessful_expectations"] > 0


def test_pandas_dataframe_validation_empty_dataframe():
    """Test validation with empty DataFrame."""
    df = pd.DataFrame({"id": [], "value": []})
    
    expectations = {
        "expectation_suite_name": "empty_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "id"}
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "value"}
            }
        ]
    }
    
    # Validate
    impl = PandasValidation()
    result = impl.validate(df, expectations)
    
    # Verify results - column existence checks should pass even for empty DataFrame
    assert result is not None
    assert result["success"] is True


def test_pandas_dataframe_validation_complex_expectations():
    """Test validation with multiple complex expectations."""
    df = pd.DataFrame({
        "product_id": [1, 2, 3, 4, 5],
        "category": ["electronics", "electronics", "books", "books", "electronics"],
        "price": [299.99, 499.99, 19.99, 29.99, 399.99],
        "stock": [10, 5, 100, 50, 0]
    })
    
    expectations = {
        "expectation_suite_name": "product_suite",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_equal",
                "kwargs": {"value": 5}
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "category",
                    "value_set": ["electronics", "books", "clothing"]
                }
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "price",
                    "min_value": 0.0,
                    "max_value": 10000.0
                }
            },
            {
                "expectation_type": "expect_column_min_to_be_between",
                "kwargs": {
                    "column": "stock",
                    "min_value": 0,
                    "max_value": 0
                }
            }
        ]
    }
    
    # Validate
    impl = PandasValidation()
    result = impl.validate(df, expectations)
    
    # Verify results
    assert result is not None
    assert result["success"] is True
    assert result["statistics"]["evaluated_expectations"] == 4
    assert result["statistics"]["successful_expectations"] == 4


def test_pandas_dataframe_validation_single_column():
    """Test validation with a single column."""
    df = pd.DataFrame({"temperature": [20.5, 21.0, 19.8, 22.3, 20.1]})
    
    expectations = {
        "expectation_suite_name": "temperature_suite",
        "expectations": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "temperature"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "temperature",
                    "min_value": 15.0,
                    "max_value": 25.0
                }
            }
        ]
    }
    
    # Validate
    impl = PandasValidation()
    result = impl.validate(df, expectations)
    
    # Verify results
    assert result["success"] is True
    assert result["statistics"]["successful_expectations"] == 2
