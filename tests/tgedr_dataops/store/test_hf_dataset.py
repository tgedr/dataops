import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from datasets import Dataset, DatasetDict

from tgedr_dataops_abs.store import NoStoreException
from tgedr_dataops.store.data_splits import DataFrameSplits
from src.tgedr_dataops.store.hf_dataset import HuggingFaceDatasetStore


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
def validation_df() -> pd.DataFrame:  # noqa: D103
    return pd.DataFrame({"name": ["Dalila"], "age": [38]})


@pytest.fixture
def splits(train_df, test_df, validation_df) -> DataFrameSplits:  # noqa: D103
    return DataFrameSplits(train=train_df, test=test_df, validation=validation_df)


# ---------------------------------------------------------------------------
# HuggingFaceDatasetStore
# ---------------------------------------------------------------------------

def test_init():  # noqa: ANN201, D103
    config = {"foo": "bar"}
    store = HuggingFaceDatasetStore(config=config)
    assert store._config == config  # noqa: SLF001


def test_init_no_config():  # noqa: ANN201, D103
    store = HuggingFaceDatasetStore()
    assert store._config is None  # noqa: SLF001


def test_get_success(splits, train_df, test_df, validation_df):  # noqa: ANN001, ANN201, D103
    mock_ds = DatasetDict(
        {
            "train": Dataset.from_pandas(train_df),
            "test": Dataset.from_pandas(test_df),
            "validation": Dataset.from_pandas(validation_df),
        }
    )
    store = HuggingFaceDatasetStore()

    with patch("src.tgedr_dataops.store.hf_dataset.load_dataset", return_value=mock_ds) as mock_load:
        result = store.get(key="some/key")

    mock_load.assert_called_once_with("some/key")
    assert isinstance(result, DataFrameSplits)
    assert_frame_equal(result.train, train_df)
    assert_frame_equal(result.test, test_df)
    assert_frame_equal(result.validation, validation_df)


def test_get_drops_index_level_0_column(train_df, test_df, validation_df):  # noqa: ANN001, ANN201, D103
    # A non-default index on the DataFrame produces an "__index_level_0__" column
    # after a Dataset round-trip; get() should strip it.
    indexed_train = train_df.copy()
    indexed_train.index = [5, 7]

    mock_ds = DatasetDict(
        {
            "train": Dataset.from_pandas(indexed_train),
            "test": Dataset.from_pandas(test_df),
            "validation": Dataset.from_pandas(validation_df),
        }
    )
    store = HuggingFaceDatasetStore()

    with patch("src.tgedr_dataops.store.hf_dataset.load_dataset", return_value=mock_ds) as mock_load:
        result = store.get(key="some/key")

    mock_load.assert_called_once_with("some/key")
    assert "__index_level_0__" not in result.train.columns
    assert "__index_level_0__" not in result.test.columns
    assert "__index_level_0__" not in result.validation.columns
    assert_frame_equal(result.train, train_df)
    assert_frame_equal(result.test, test_df)
    assert_frame_equal(result.validation, validation_df)


def test_get_dataset_not_found():  # noqa: ANN201, D103
    store = HuggingFaceDatasetStore()

    with patch(
        "src.tgedr_dataops.store.hf_dataset.load_dataset", side_effect=Exception("not found")
    ):
        with pytest.raises(NoStoreException, match="not found on Hugging Face Hub"):
            store.get(key="missing/key")


def test_delete_success_cache_exists():  # noqa: ANN201, D103
    store = HuggingFaceDatasetStore()
    key = "owner/dataset"
    cache_dir = Path("fake_cache") / key.replace("/", "___")
    cache_dir.mkdir(parents=True)
    try:
        with (
            patch("src.tgedr_dataops.store.hf_dataset.datasets.config.HF_DATASETS_CACHE", "fake_cache"),
            patch("src.tgedr_dataops.store.hf_dataset.HfApi") as mock_api_class,
        ):
            mock_api = MagicMock()
            mock_api_class.return_value = mock_api
            store.delete(key=key)

        mock_api.delete_repo.assert_called_once_with(repo_id=key, repo_type="dataset")
        assert not cache_dir.exists()
    finally:
        shutil.rmtree(Path("fake_cache"), ignore_errors=True)


def test_delete_success_cache_not_exists():  # noqa: ANN201, D103
    store = HuggingFaceDatasetStore()
    key = "owner/dataset"

    with (
        patch("src.tgedr_dataops.store.hf_dataset.datasets.config.HF_DATASETS_CACHE", "nonexistent_cache"),
        patch("src.tgedr_dataops.store.hf_dataset.HfApi") as mock_api_class,
    ):
        mock_api = MagicMock()
        mock_api_class.return_value = mock_api
        store.delete(key=key)

    mock_api.delete_repo.assert_called_once_with(repo_id=key, repo_type="dataset")


def test_delete_repo_fails():  # noqa: ANN201, D103
    store = HuggingFaceDatasetStore()
    key = "owner/dataset"

    with (
        patch("src.tgedr_dataops.store.hf_dataset.datasets.config.HF_DATASETS_CACHE", "nonexistent_cache"),
        patch("src.tgedr_dataops.store.hf_dataset.HfApi") as mock_api_class,
    ):
        mock_api = MagicMock()
        mock_api.delete_repo.side_effect = Exception("no permission")
        mock_api_class.return_value = mock_api
        with pytest.raises(NoStoreException, match="not deletable on Hugging Face Hub"):
            store.delete(key=key)


def test_save(splits):  # noqa: ANN001, ANN201, D103
    store = HuggingFaceDatasetStore()

    with patch.object(DataFrameSplits, "to_dataset_dict", return_value=MagicMock()) as mock_to_ds:
        store.save(df=splits, key="owner/dataset")

    mock_to_ds.assert_called_once()
    mock_to_ds.return_value.push_to_hub.assert_called_once_with("owner/dataset")


def test_update_append(splits, train_df, test_df, validation_df, train_df2):  # noqa: ANN001, ANN201, D103
    store = HuggingFaceDatasetStore()

    dfs_2: DataFrameSplits = DataFrameSplits(train=train_df2, test=test_df, validation=validation_df)  # noqa: SLF001
    with (
        patch.object(store, "get", return_value=splits) as mock_get,
        patch.object(store, "save") as mock_save,
    ):
        store.update(df=dfs_2, key="owner/dataset", append=True)

    mock_get.assert_called_once_with(key="owner/dataset")
    mock_save.assert_called_once()
    saved_dfs = mock_save.call_args[0][0]
    assert_frame_equal(saved_dfs.train, pd.concat([train_df, train_df2], ignore_index=True))
    assert_frame_equal(saved_dfs.test, test_df)
    assert_frame_equal(saved_dfs.validation, validation_df)


def test_update_no_append(splits):  # noqa: ANN001, ANN201, D103
    store = HuggingFaceDatasetStore()

    with (
        patch.object(store, "get") as mock_get,
        patch.object(store, "save") as mock_save,
    ):
        store.update(df=splits, key="owner/dataset")

    mock_get.assert_not_called()
    mock_save.assert_called_once_with(splits, key="owner/dataset")
