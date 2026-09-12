"""Unit tests for HuggingFaceDatasetFileBasedStore."""

from pathlib import Path
import shutil
import tempfile
from unittest.mock import MagicMock, patch

import httpx
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from huggingface_hub.utils import RepositoryNotFoundError

from tgedr_dataops_abs.store import NoStoreException
from tgedr_dataops.store.hf_dataset import DataFrameSplits
from src.tgedr_dataops.store.hf_dataset_file_based import HuggingFaceDatasetFileBasedStore


def repo_not_found_error() -> RepositoryNotFoundError:  # noqa: D103
    response = httpx.Response(404, request=httpx.Request("GET", "https://huggingface.co/api/datasets/x"))
    return RepositoryNotFoundError("nope", response=response)


@pytest.fixture
def store() -> HuggingFaceDatasetFileBasedStore:  # noqa: D103
    return HuggingFaceDatasetFileBasedStore(config={"visibility": "public"})


@pytest.fixture
def df() -> pd.DataFrame:  # noqa: D103
    return pd.DataFrame({"name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]})


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


def test_init_default_visibility(store) -> None:  # noqa: ANN001, D103
    assert store._config == {"visibility": "public"}  # noqa: SLF001
    assert store._HuggingFaceDatasetFileBasedStore__dataset_visibility == "public"  # noqa: SLF001


def test_init_no_config() -> None:  # noqa: D103
    store = HuggingFaceDatasetFileBasedStore()
    assert store._config is None  # noqa: SLF001
    assert store._HuggingFaceDatasetFileBasedStore__dataset_visibility == "private"  # noqa: SLF001


def test_init_visibility_from_config() -> None:  # noqa: D103
    store = HuggingFaceDatasetFileBasedStore(config={"visibility": "private"})
    assert store._HuggingFaceDatasetFileBasedStore__dataset_visibility == "private"  # noqa: SLF001


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


def test_get_success(store) -> None:  # noqa: ANN001, D103
    train = pd.DataFrame({"name": ["Alice"], "age": [30]})
    test = pd.DataFrame({"name": ["Bob"], "age": [25]})
    validation = pd.DataFrame({"name": ["Charlie"], "age": [35]})

    mock_ds = MagicMock()
    mock_ds.__iter__.return_value = iter(["train", "test", "validation"])
    mock_ds.__getitem__.side_effect = lambda split: {
        "train": MagicMock(to_pandas=lambda: train.copy()),
        "test": MagicMock(to_pandas=lambda: test.copy()),
        "validation": MagicMock(to_pandas=lambda: validation.copy()),
    }[split]

    with patch(
        "src.tgedr_dataops.store.hf_dataset_file_based.datasets.load_dataset",
        return_value=mock_ds,
    ) as mock_load:
        result = store.get(key="owner/dataset")

    mock_load.assert_called_once_with("owner/dataset")
    assert isinstance(result, DataFrameSplits)
    assert_frame_equal(result.train, train)
    assert_frame_equal(result.test, test)
    assert_frame_equal(result.validation, validation)


def test_get_drops_index_level_0_column(store) -> None:  # noqa: ANN001, D103
    indexed_train = pd.DataFrame({"name": ["Alice"], "age": [30]})
    indexed_train.index = [5]

    mock_ds = MagicMock()
    mock_ds.__iter__.return_value = iter(["train"])
    mock_ds.__getitem__.side_effect = lambda split: MagicMock(  # noqa: ARG005
        to_pandas=lambda: pd.DataFrame({"_": [1], "__index_level_0__": [0], "name": ["Alice"]})
    )

    with patch(
        "src.tgedr_dataops.store.hf_dataset_file_based.datasets.load_dataset",
        return_value=mock_ds,
    ):
        result = store.get(key="owner/dataset")

    assert "__index_level_0__" not in result.train.columns
    assert "name" in result.train.columns


def test_get_dataset_not_found(store) -> None:  # noqa: ANN001, D103
    with (
        patch(
            "src.tgedr_dataops.store.hf_dataset_file_based.datasets.load_dataset",
            side_effect=Exception("not found"),
        ),
        pytest.raises(NoStoreException, match="not found on Hugging Face Hub"),
    ):
        store.get(key="missing/key")


# ---------------------------------------------------------------------------
# _assert_dataset_existence
# ---------------------------------------------------------------------------


def test_assert_dataset_existence_creates_when_missing(store) -> None:  # noqa: ANN001, D103
    with (
        patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as mock_api,
    ):
        mock_api.list_datasets.return_value = iter([])
        store._assert_dataset_existence(key="owner/dataset")  # noqa: SLF001

    mock_api.list_datasets.assert_called_once_with(dataset_name="owner/dataset")
    mock_api.create_repo.assert_called_once_with(
        repo_id="owner/dataset", repo_type="dataset", exist_ok=True, visibility="public"
    )


def test_assert_dataset_existence_exists(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as mock_api:
        mock_api.list_datasets.return_value = iter(["owner/dataset"])
        store._assert_dataset_existence(key="owner/dataset")  # noqa: SLF001

    mock_api.create_repo.assert_not_called()


# ---------------------------------------------------------------------------
# _get_last_file_index
# ---------------------------------------------------------------------------


def test_get_last_file_index_with_files(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "list", return_value=["train_00002.parquet", "train_00000.parquet"]):
        assert store._get_last_file_index(key="owner/dataset", split="train") == 2  # noqa: SLF001


def test_get_last_file_index_empty(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "list", return_value=[]):
        assert store._get_last_file_index(key="owner/dataset", split="train") == -1  # noqa: SLF001


def test_get_last_file_index_wrong_extension(store) -> None:  # noqa: ANN001, D103
    with (
        patch.object(store, "list", return_value=["owner/dataset/.gitattributes"]),
        pytest.raises(ValueError, match="invalid literal"),
    ):
        store._get_last_file_index(key="owner/dataset", split="train")  # noqa: SLF001


# ---------------------------------------------------------------------------
# _get_next_file_index
# ---------------------------------------------------------------------------


def test_get_next_file_index(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "_get_last_file_index", return_value=2):
        assert store._get_next_file_index(key="owner/dataset", split="train") == "00003"  # noqa: SLF001


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def test_list_split(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as mock_api:
        mock_api.list_repo_files.return_value = [
            "train_00000.parquet",
            "train_00001.parquet",
            "test_00000.parquet",
            ".gitattributes",
            "README.md",
        ]
        result = store.list(key="owner/dataset", split="train")

    assert result == ["train_00000.parquet", "train_00001.parquet"]


def test_list_all(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as mock_api:
        mock_api.list_repo_files.return_value = [
            "train_00000.parquet",
            "test_00000.parquet",
            "validation_00000.parquet",
        ]
        result = store.list(key="owner/dataset", split="all")

    assert result == ["train_00000.parquet", "test_00000.parquet", "validation_00000.parquet"]


def test_list_repo_not_found() -> None:  # noqa: D103
    store = HuggingFaceDatasetFileBasedStore()
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as mock_api:
        mock_api.list_repo_files.side_effect = repo_not_found_error()
        with pytest.raises(NoStoreException, match="not found on Hugging Face Hub"):
            store.list(key="owner/dataset", split="train")


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete_split(store) -> None:  # noqa: ANN001, D103
    with (
        patch.object(store, "list", return_value=["train_00000.parquet", "train_00001.parquet"]),
        patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as mock_api,
    ):
        store.delete(key="owner/dataset", split="train")

    assert mock_api.delete_file.call_count == 2
    mock_api.delete_file.assert_any_call(
        path_in_repo="train_00000.parquet", repo_id="owner/dataset", repo_type="dataset"
    )
    mock_api.delete_file.assert_any_call(
        path_in_repo="train_00001.parquet", repo_id="owner/dataset", repo_type="dataset"
    )


def test_delete_all(store) -> None:  # noqa: ANN001, D103
    with (
        patch.object(store, "_delete_dataset") as mock_delete_dataset,
    ):
        store.delete(key="owner/dataset", split="all")

    mock_delete_dataset.assert_called_once_with("owner/dataset")


# ---------------------------------------------------------------------------
# _delete_dataset
# ---------------------------------------------------------------------------


def test_delete_dataset_removes_cache_and_repo(store) -> None:  # noqa: ANN001, D103
    # Build a fake cache directory that the method will remove
    fake_cache = Path("fake_hf_cache") / "owner___dataset"
    fake_cache.mkdir(parents=True)
    try:
        with (
            patch("src.tgedr_dataops.store.hf_dataset_file_based.datasets.config.HF_DATASETS_CACHE", "fake_hf_cache"),
            patch("src.tgedr_dataops.store.hf_dataset_file_based.HfApi") as mock_api_class,
        ):
            mock_api = MagicMock()
            mock_api_class.return_value = mock_api
            store._delete_dataset(key="owner/dataset")  # noqa: SLF001

        assert not fake_cache.exists()
        mock_api.delete_repo.assert_called_once_with(repo_id="owner/dataset", repo_type="dataset")
    finally:
        shutil.rmtree(Path("fake_hf_cache"), ignore_errors=True)


def test_delete_dataset_no_cache(store) -> None:  # noqa: ANN001, D103
    with (
        patch("src.tgedr_dataops.store.hf_dataset_file_based.datasets.config.HF_DATASETS_CACHE", "nonexistent_cache"),
        patch("src.tgedr_dataops.store.hf_dataset_file_based.HfApi") as mock_api_class,
    ):
        mock_api = MagicMock()
        mock_api_class.return_value = mock_api
        store._delete_dataset(key="owner/dataset")  # noqa: SLF001

    mock_api.delete_repo.assert_called_once_with(repo_id="owner/dataset", repo_type="dataset")


def test_delete_dataset_repo_not_found(store) -> None:  # noqa: ANN001, D103
    with (
        patch("src.tgedr_dataops.store.hf_dataset_file_based.datasets.config.HF_DATASETS_CACHE", "nonexistent_cache"),
        patch("src.tgedr_dataops.store.hf_dataset_file_based.HfApi") as mock_api_class,
    ):
        mock_api = MagicMock()
        mock_api.delete_repo.side_effect = repo_not_found_error()
        mock_api_class.return_value = mock_api
        store._delete_dataset(key="owner/dataset")  # noqa: SLF001


def test_delete_dataset_repo_delete_fails(store) -> None:  # noqa: ANN001, D103
    with (
        patch("src.tgedr_dataops.store.hf_dataset_file_based.datasets.config.HF_DATASETS_CACHE", "nonexistent_cache"),
        patch("src.tgedr_dataops.store.hf_dataset_file_based.HfApi") as mock_api_class,
    ):
        mock_api = MagicMock()
        mock_api.delete_repo.side_effect = Exception("no permission")
        mock_api_class.return_value = mock_api
        with pytest.raises(NoStoreException, match="deletion failed"):
            store._delete_dataset(key="owner/dataset")  # noqa: SLF001


# ---------------------------------------------------------------------------
# _is_empty
# ---------------------------------------------------------------------------


def test_is_empty_true(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "list", return_value=[]):
        assert store._is_empty(key="owner/dataset") is True  # noqa: SLF001


def test_is_empty_false(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "list", return_value=["train_00000.parquet"]):
        assert store._is_empty(key="owner/dataset") is False  # noqa: SLF001


def test_is_empty_repo_missing(store) -> None:  # noqa: ANN001, D103
    with patch.object(store, "list", side_effect=NoStoreException("not found")):
        assert store._is_empty(key="owner/dataset") is True  # noqa: SLF001


# ---------------------------------------------------------------------------
# _store_data
# ---------------------------------------------------------------------------


def test_store_data_chunks_and_uploads(store, df) -> None:  # noqa: ANN001, D103
    with (
        patch.object(store, "_get_last_file_index", return_value=-1),
        patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as mock_api,
        tempfile.TemporaryDirectory() as tmp_dir,
        patch(
            "src.tgedr_dataops.store.hf_dataset_file_based.tempfile.TemporaryDirectory",
            return_value=MagicMock(__enter__=lambda _: tmp_dir),
        ),
    ):
        store._store_data(df=df, key="owner/dataset", split="train")  # noqa: SLF001
        # 3 rows chunked by 2 -> 2 chunks -> 2 files
        assert mock_api.upload_file.call_count == 2
        uploaded = [call.kwargs["path_in_repo"] for call in mock_api.upload_file.call_args_list]
        assert uploaded == ["train_00000.parquet", "train_00001.parquet"]
        # ensure files were physically written to temp and uploaded
        for call in mock_api.upload_file.call_args_list:
            written = Path(call.kwargs["path_or_fileobj"])
            assert written.exists()
            assert written.parent == Path(tmp_dir)


# ---------------------------------------------------------------------------
# save
# ---------------------------------------------------------------------------


def test_save_invalid_split_raises(store, df) -> None:  # noqa: ANN001, D103
    with pytest.raises(ValueError, match="Invalid split"):
        store.save(df=df, key="owner/dataset", split="bogus")


def test_save_append(store, df) -> None:  # noqa: ANN001, D103
    with (
        patch.object(store, "_assert_dataset_existence") as mock_assert,
        patch.object(store, "_store_data") as mock_store,
        patch.object(store, "_is_empty") as mock_empty,
        patch.object(store, "delete") as mock_delete,
    ):
        store.save(df=df, key="owner/dataset", split="train", append=True)

    mock_assert.assert_called_once_with("owner/dataset")
    mock_store.assert_called_once_with(df, "owner/dataset", "train")
    mock_empty.assert_not_called()
    mock_delete.assert_not_called()


def test_save_append_truthy_values(store, df) -> None:  # noqa: ANN001, D103
    for truthy in ["true", True, 1]:
        with (
            patch.object(store, "_assert_dataset_existence"),
            patch.object(store, "_store_data") as mock_store,
        ):
            store.save(df=df, key="owner/dataset", split="train", append=truthy)
        mock_store.assert_called_once_with(df, "owner/dataset", "train")


def test_save_append_false_string(store, df) -> None:  # noqa: ANN001, D103
    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "_store_data"),
        patch.object(store, "_is_empty", return_value=False) as mock_empty,
        patch.object(store, "delete") as mock_delete,
    ):
        store.save(df=df, key="owner/dataset", split="train", append="false")

    mock_empty.assert_called_once_with("owner/dataset")
    mock_delete.assert_called_once_with("owner/dataset", "train")


def test_save_replace_non_empty_keeps_indexes(store, df) -> None:  # noqa: ANN001, D103
    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "_store_data"),
        patch.object(store, "_is_empty", return_value=False),
        patch.object(store, "delete") as mock_delete,
    ):
        store.save(df=df, key="owner/dataset", split="train")

    mock_delete.assert_called_once_with("owner/dataset", "train")


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


def test_update_invalid_split_raises(store, df) -> None:  # noqa: ANN001, D103
    with pytest.raises(ValueError, match="Invalid split"):
        store.update(df=df, key="owner/dataset", split="bogus", key_fields=["id"])


def test_update_split_missing(store, df) -> None:  # noqa: ANN001, D103
    # When the split is missing, update() should save an empty DataFrame (replacing target split)
    existing = DataFrameSplits(test=pd.DataFrame({"id": [9], "name": ["Zoe"]}))

    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "get", return_value=existing),
        patch.object(store, "save") as mock_save,
    ):
        store.update(df=df, key="owner/dataset", split="train", key_fields=["id"])

    mock_save.assert_called_once()
    saved_df = mock_save.call_args[1]["df"]
    assert saved_df.empty
    assert mock_save.call_args[1]["split"] == "train"


def test_update_match_empty_appends(store) -> None:  # noqa: ANN001, D103
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    # Split exists but no matching key rows -> data is appended
    existing = DataFrameSplits(train=pd.DataFrame({"id": [10], "name": ["Old"]}))
    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "get", return_value=existing),
        patch.object(store, "save") as mock_save,
    ):
        store.update(df=df, key="owner/dataset", split="train", key_fields=["id"])

    # When no rows match, update() first appends the new df, then re-saves the existing split
    assert mock_save.call_count == 2
    assert mock_save.call_args_list[0].kwargs["append"] is True
    assert mock_save.call_args_list[0].kwargs["df"].equals(df)
    assert mock_save.call_args_list[1].kwargs["df"].equals(existing.train)


def test_update_all_rows_match(store) -> None:  # noqa: ANN001, D103
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [31, 26]})
    existing = DataFrameSplits(train=pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]}))

    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "get", return_value=existing),
        patch.object(store, "save") as mock_save,
    ):
        store.update(df=df, key="owner/dataset", split="train", key_fields=["id"])

    saved_df = mock_save.call_args[1]["df"]
    assert_frame_equal(saved_df, df)


def test_update_partial_match(store) -> None:  # noqa: ANN001, D103
    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "New"], "age": [31, 99]})
    train = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]})
    existing = DataFrameSplits(train=train)

    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "get", return_value=existing),
        patch.object(store, "save") as mock_save,
    ):
        store.update(df=df, key="owner/dataset", split="train", key_fields=["id"])

    saved_df = mock_save.call_args[1]["df"]
    # Both rows match by id and len(match) == len(df), so the split rows are
    # overwritten with the incoming values; the result equals df.
    assert_frame_equal(saved_df, df)


def test_update_partial_match_with_unmatched_rows(store) -> None:  # noqa: ANN001, D103
    # df has 3 rows: ids 1 and 2 match existing, id 3 is new
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "New", "Extra"], "age": [31, 99, 50]})
    train = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"], "age": [30, 25]})
    existing = DataFrameSplits(train=train)

    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "get", return_value=existing),
        patch.object(store, "save") as mock_save,
    ):
        store.update(df=df, key="owner/dataset", split="train", key_fields=["id"])

    saved_df = mock_save.call_args[1]["df"]
    # len(match)=2 < len(df)=3 -> update matched rows, then append unmatched rows.
    # drop_duplicates removes the positional duplicate (the matched-but-not-unique
    # row appended by df.iloc[~index_right]).
    assert len(saved_df) == 3
    assert set(saved_df["id"]) == {1, 2, 3}
    # id=1 matched and was updated
    assert saved_df[saved_df["id"] == 1]["age"].iloc[0] == 31
    # dedupe keeps the updated row for id=2 (from the in-place assignment), not the stale one
    assert len(saved_df[saved_df["id"] == 2]) == 1
    assert saved_df[saved_df["id"] == 2]["name"].iloc[0] == "New"
