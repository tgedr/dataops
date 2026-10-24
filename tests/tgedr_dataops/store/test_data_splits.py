"""Tests for pandas-backed and Arrow-backed dataset split containers."""

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from datasets import Dataset, DatasetDict, IterableDataset, IterableDatasetDict

from tgedr_dataops.store.data_splits import DataFrameSplits, DatasetSplits, IterableDatasetSplits, _freeze


# DataFrameSplits fixtures / helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def train_df() -> pd.DataFrame:  # noqa: D103
    return pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})

@pytest.fixture
def train_df2() -> pd.DataFrame:  # noqa: D103
    return pd.DataFrame({"name": ["Paul", "Marylin"], "age": [20, 15]})

@pytest.fixture
def test_df() -> pd.DataFrame:  # noqa: D103
    return pd.DataFrame({"name": ["Charlie"], "age": [35]})

@pytest.fixture
def test_df2() -> pd.DataFrame:  # noqa: D103
    return pd.DataFrame({"name": ["Mauro"], "age": [39]})


@pytest.fixture
def validation_df() -> pd.DataFrame:  # noqa: D103
    return pd.DataFrame({"name": ["Dalila"], "age": [38]})

@pytest.fixture
def validation_df2() -> pd.DataFrame:  # noqa: D103
    return pd.DataFrame({"name": ["Elena"], "age": [28]})


@pytest.fixture
def splits(train_df, test_df, validation_df) -> DataFrameSplits:  # noqa: D103
    return DataFrameSplits(train=train_df, test=test_df, validation=validation_df)


# ---------------------------------------------------------------------------
# DataFrameSplits.__eq__
# ---------------------------------------------------------------------------

def test_eq_equal(splits, train_df, test_df, validation_df):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(train=train_df, test=test_df, validation=validation_df)
    assert splits == other


def test_eq_wrong_type(splits):  # noqa: ANN001, ANN201, D103
    assert not (splits == "not a DataFrameSplits")


def test_eq_returns_not_implemented_for_unknown_type(splits):  # noqa: ANN001, ANN201, D103
    assert splits.__eq__(object()) is False
    assert DataFrameSplits.__eq__(splits, object()) is False


def test_eq_different_train(splits):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(
        train=pd.DataFrame({"name": ["X"], "age": [1]}),
        test=splits.test,
        validation=splits.validation,
    )
    assert splits != other


def test_eq_different_test(splits):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(
        train=splits.train,
        test=pd.DataFrame({"name": ["X"], "age": [1]}),
        validation=splits.validation,
    )
    assert splits != other


def test_eq_different_validation(splits):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(
        train=splits.train,
        test=splits.test,
        validation=pd.DataFrame({"name": ["X"], "age": [1]}),
    )
    assert splits != other


def test_eq_none_fields():  # noqa: ANN201, D103
    assert DataFrameSplits() == DataFrameSplits()


def test_eq_one_side_none_train(splits):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(train=None, test=splits.test, validation=splits.validation)
    assert splits != other


def test_eq_one_side_none_test(splits):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(train=splits.train, test=None, validation=splits.validation)
    assert splits != other


def test_eq_one_side_none_validation(splits):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(train=splits.train, test=splits.test, validation=None)
    assert splits != other


# ---------------------------------------------------------------------------
# DataFrameSplits.__hash__
# ---------------------------------------------------------------------------

def test_hash(splits):  # noqa: ANN001, ANN201, D103
    assert isinstance(hash(splits), int)


# ---------------------------------------------------------------------------
# DataFrameSplits.equals
# ---------------------------------------------------------------------------

def test_equals_delegates_to_eq(splits, train_df, test_df, validation_df):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(train=train_df, test=test_df, validation=validation_df)
    assert splits.equals(other)
    assert not splits.equals(DataFrameSplits())


# ---------------------------------------------------------------------------
# DataFrameSplits.union
# ---------------------------------------------------------------------------

def test_union_both_present(splits):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits(
        train=pd.DataFrame({"name": ["Eve"], "age": [40]}),
        test=pd.DataFrame({"name": ["Frank"], "age": [41]}),
        validation=pd.DataFrame({"name": ["Grace"], "age": [42]}),
    )
    # Compute expected values BEFORE calling union(), since it mutates splits in place
    expected_train = pd.concat([splits.train, other.train], ignore_index=True)
    expected_test = pd.concat([splits.test, other.test], ignore_index=True)
    expected_validation = pd.concat([splits.validation, other.validation], ignore_index=True)

    result = splits.union(other)
    assert result is splits
    assert_frame_equal(result.train, expected_train)
    assert_frame_equal(result.test, expected_test)
    assert_frame_equal(result.validation, expected_validation)


def test_union_other_none(splits):  # noqa: ANN001, ANN201, D103
    other = DataFrameSplits()
    result = splits.union(other)
    assert_frame_equal(result.train, splits.train)
    assert_frame_equal(result.test, splits.test)
    assert_frame_equal(result.validation, splits.validation)


def test_union_self_none(train_df, test_df, validation_df):  # noqa: ANN001, ANN201, D103
    self_splits = DataFrameSplits()
    other = DataFrameSplits(train=train_df, test=test_df, validation=validation_df)
    result = self_splits.union(other)
    assert result is self_splits
    assert_frame_equal(result.train, train_df)
    assert_frame_equal(result.test, test_df)
    assert_frame_equal(result.validation, validation_df)


def test_union_partial_missing():  # noqa: ANN201, D103
    self_splits = DataFrameSplits(
        train=pd.DataFrame({"a": [1]}),
        test=None,
        validation=pd.DataFrame({"a": [2]}),
    )
    other = DataFrameSplits(
        train=None,
        test=pd.DataFrame({"a": [3]}),
        validation=None,
    )
    result = self_splits.union(other)
    assert_frame_equal(result.train, pd.DataFrame({"a": [1]}))
    assert_frame_equal(result.test, pd.DataFrame({"a": [3]}))
    assert_frame_equal(result.validation, pd.DataFrame({"a": [2]}))


# ---------------------------------------------------------------------------
# DataFrameSplits.from_dict
# ---------------------------------------------------------------------------

def test_from_dict_all_keys(train_df, test_df, validation_df):  # noqa: ANN001, ANN201, D103
    result = DataFrameSplits.from_dict(
        {"train": train_df, "test": test_df, "validation": validation_df}
    )
    assert_frame_equal(result.train, train_df)
    assert_frame_equal(result.test, test_df)
    assert_frame_equal(result.validation, validation_df)


def test_from_dict_missing_keys(train_df):  # noqa: ANN001, ANN201, D103
    result = DataFrameSplits.from_dict({"train": train_df})
    assert_frame_equal(result.train, train_df)
    assert result.test is None
    assert result.validation is None


def test_from_dict_empty():  # noqa: ANN201, D103
    result = DataFrameSplits.from_dict({})
    assert result.train is None
    assert result.test is None
    assert result.validation is None


# ---------------------------------------------------------------------------
# DataFrameSplits.to_dataset_dict
# ---------------------------------------------------------------------------

def test_to_dataset_dict_all_keys(splits):  # noqa: ANN001, ANN201, D103
    result = splits.to_dataset_dict()
    assert isinstance(result, DatasetDict)
    assert set(result.keys()) == {"train", "test", "validation"}


def test_to_dataset_dict_missing_keys():  # noqa: ANN201, D103
    result = DataFrameSplits(train=pd.DataFrame({"a": [1]})).to_dataset_dict()
    assert set(result.keys()) == {"train"}


def test_to_dataset_dict_empty():  # noqa: ANN201, D103
    result = DataFrameSplits().to_dataset_dict()
    assert isinstance(result, DatasetDict)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# DataFrameSplits.shape
# ---------------------------------------------------------------------------

def test_shape_all_keys(splits):  # noqa: ANN001, ANN201, D103
    assert splits.shape == "train: (2, 2), test: (1, 2), validation: (1, 2)"


def test_shape_missing_keys():  # noqa: ANN201, D103
    assert DataFrameSplits().shape == "train: None, test: None, validation: None"


# DatasetSplits
# ---------------------------------------------------------------------------

def dataset(values: list[int]) -> Dataset:
    return Dataset.from_dict({"value": values})


def test_eq_identical_contents() -> None:
    assert DatasetSplits(train=dataset([1, 2])) == DatasetSplits(train=dataset([1, 2]))


def test_eq_different_contents() -> None:
    assert DatasetSplits(train=dataset([1, 2])) != DatasetSplits(train=dataset([1, 3]))


def test_eq_different_splits() -> None:
    assert DatasetSplits(train=dataset([1])) != DatasetSplits(test=dataset([1]))


def test_eq_missing_split() -> None:
    assert DatasetSplits() == DatasetSplits(train=None, test=None, validation=None)
    assert DatasetSplits(train=dataset([1])) != DatasetSplits()


def test_eq_different_schema() -> None:
    left = Dataset.from_dict({"value": [1]})
    right = Dataset.from_dict({"value": [1.0]})
    assert DatasetSplits(train=left) != DatasetSplits(train=right)


def test_eq_wrong_type() -> None:
    assert DatasetSplits() != object()


def test_equals_uses_content_equality() -> None:
    left = DatasetSplits(train=dataset([1, 2]))
    right = DatasetSplits(train=dataset([1, 2]))
    assert left.equals(right)
    assert not left.equals(DatasetSplits(train=dataset([1])))
    assert not left.equals(object())


def test_hash_is_equal_for_equal_splits() -> None:
    left = DatasetSplits(train=dataset([1, 2]))
    right = DatasetSplits(train=dataset([1, 2]))
    assert left == right
    assert hash(left) == hash(right)


def test_hash_distinguishes_split_data() -> None:
    assert hash(DatasetSplits(train=dataset([1]))) != hash(DatasetSplits(train=dataset([2])))


def test_splits_can_be_used_as_dictionary_keys() -> None:
    splits = DatasetSplits(train=dataset([1]))
    mapping = {splits: "present"}
    assert mapping[DatasetSplits(train=dataset([1]))] == "present"


@pytest.mark.parametrize("payload", [[None], [[1, 2]], [{"x": 1}]])
def test_hash_supports_nested_and_nullable_values(payload: list[object]) -> None:
    splits = DatasetSplits(train=Dataset.from_dict({"value": payload}))
    assert isinstance(hash(splits), int)


@pytest.mark.parametrize("split", ["train", "test", "validation"])
def test_dataset_split_helpers(split: str) -> None:
    data = dataset([1])
    splits = DatasetSplits(**{split: data})
    assert splits.has_split(split)
    assert splits.get_split(split) is data
    missing = DatasetSplits()
    assert not missing.has_split(split)
    assert missing.get_split(split) is None


def test_dataset_split_helpers_reject_unknown_name() -> None:
    splits = DatasetSplits()
    with pytest.raises(ValueError, match="Invalid split"):
        splits.has_split("unknown")
    with pytest.raises(ValueError, match="Invalid split"):
        splits.get_split("unknown")


@pytest.mark.parametrize("split", ["test", "validation"])
def test_eq_compares_each_optional_split(split: str) -> None:
    left = DatasetSplits(**{split: dataset([1])})
    right = DatasetSplits(**{split: dataset([2])})
    assert left != right
    assert left != DatasetSplits()
    assert DatasetSplits() != left


def test_dataset_splits_eq_wrong_type() -> None:
    assert DatasetSplits() != object()


def test_dataset_dict_conversions() -> None:
    train, test, validation = dataset([1]), dataset([2]), dataset([3])
    splits = DatasetSplits(train=train, test=test, validation=validation)
    dataset_dict = splits.to_dataset_dict()
    assert dataset_dict["train"] is train
    assert dataset_dict["test"] is test
    assert dataset_dict["validation"] is validation
    assert DatasetSplits.from_dataset_dict(dataset_dict) == splits
    assert DatasetSplits.from_dataset_dict(DatasetDict()) == DatasetSplits()
    assert DatasetSplits().to_dataset_dict() == DatasetDict()


@pytest.mark.parametrize("side", ["left", "right"])
@pytest.mark.parametrize("split", ["train", "test", "validation"])
def test_union_copies_split_when_other_side_missing(side: str, split: str) -> None:
    data = dataset([1])
    if side == "left":
        left, right = DatasetSplits(**{split: data}), DatasetSplits()
    else:
        left, right = DatasetSplits(), DatasetSplits(**{split: data})
    result = left.union(right)
    assert result.get_split(split).to_list() == data.to_list()


def test_union_concatenates_present_splits() -> None:
    left = DatasetSplits(train=dataset([1]), test=dataset([2]), validation=dataset([3]))
    right = DatasetSplits(train=dataset([4]), test=dataset([5]), validation=dataset([6]))
    result = left.union(right)
    assert result.train.to_list() == [{"value": 1}, {"value": 4}]
    assert result.test.to_list() == [{"value": 2}, {"value": 5}]
    assert result.validation.to_list() == [{"value": 3}, {"value": 6}]


def test_shape_with_empty_splits() -> None:
    assert DatasetSplits(train=dataset([])).shape == "train: (0, 1), test: None, validation: None"


def test_freeze_handles_sets_nan_and_unhashable_values() -> None:
    import numpy as np

    assert _freeze({1, 2}) == frozenset({1, 2})
    assert _freeze(float("nan")) == (float, "nan")
    assert _freeze(np.array([1, 2])) == repr(np.array([1, 2]))
    assert _freeze(1) == 1


def test_dataframe_splits_unknown_name_raises_attribute_error() -> None:
    splits = DataFrameSplits()
    with pytest.raises(AttributeError):
        splits.has_split("unknown")
    with pytest.raises(AttributeError):
        splits.get_split("unknown")


# IterableDatasetSplits
# ---------------------------------------------------------------------------


def iterable_dataset(values: list[int]) -> IterableDataset:
    return IterableDataset.from_dict({"value": values})


def test_iterable_dataset_splits_fields_and_accessors() -> None:
    train, test, validation = iterable_dataset([1]), iterable_dataset([2]), iterable_dataset([3])
    splits = IterableDatasetSplits(train=train, test=test, validation=validation)
    assert splits.has_split("train")
    assert splits.get_split("train") is train
    assert splits.has_split("test")
    assert splits.get_split("test") is test
    assert splits.has_split("validation")
    assert splits.get_split("validation") is validation
    assert not IterableDatasetSplits().has_split("train")
    assert IterableDatasetSplits().get_split("train") is None


def test_iterable_dataset_splits_reject_unknown_names() -> None:
    splits = IterableDatasetSplits()
    with pytest.raises(ValueError, match="Invalid split"):
        splits.has_split("other")
    with pytest.raises(ValueError, match="Invalid split"):
        splits.get_split("other")


def test_iterable_dataset_splits_dict_round_trip() -> None:
    streams = IterableDatasetDict({"train": iterable_dataset([1]), "validation": iterable_dataset([2])})
    splits = IterableDatasetSplits.from_iterable_dataset_dict(streams)
    assert splits.to_iterable_dataset_dict() == streams
    assert IterableDatasetSplits.from_iterable_dataset_dict(IterableDatasetDict()) == IterableDatasetSplits()
    assert IterableDatasetSplits().to_iterable_dataset_dict() == IterableDatasetDict()


def test_iterable_dataset_splits_identity_equality_and_hash() -> None:
    stream = iterable_dataset([1, 2])
    splits = IterableDatasetSplits(train=stream)
    assert splits == IterableDatasetSplits(train=stream)
    assert splits != IterableDatasetSplits(train=iterable_dataset([1, 2]))
    assert splits != object()
    assert splits.equals(IterableDatasetSplits(train=stream))
    assert not splits.equals(object())
    assert hash(splits) == hash(IterableDatasetSplits(train=stream))
    assert isinstance(hash(IterableDatasetSplits()), int)


def test_iterable_dataset_splits_shape_reports_unknown_rows() -> None:
    stream = iterable_dataset([1, 2])
    assert IterableDatasetSplits(train=stream).shape == "train: (None, 1), test: None, validation: None"


def test_iterable_dataset_splits_do_not_consume_stream_on_comparison() -> None:
    stream = iterable_dataset([1, 2])
    splits = IterableDatasetSplits(train=stream)
    assert splits == IterableDatasetSplits(train=stream)
    assert hash(splits) == hash(IterableDatasetSplits(train=stream))
    assert list(stream) == [{"value": 1}, {"value": 2}]
