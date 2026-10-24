"""Unit tests for the Arrow-backed Hugging Face file store."""

from pathlib import Path
import shutil
from unittest.mock import MagicMock, patch

import httpx
import pandas as pd
import pytest
from datasets import Dataset, DatasetDict, IterableDataset, IterableDatasetDict
from huggingface_hub.utils import RepositoryNotFoundError

from tgedr_dataops_abs.store import NoStoreException
from tgedr_dataops.store.data_splits import DatasetSplits, IterableDatasetSplits
from tgedr_dataops.store.hf_dataset_file_arrow_based import HuggingFaceDatasetFileArrowBasedStore


def repo_not_found_error() -> RepositoryNotFoundError:
    response = httpx.Response(404, request=httpx.Request("GET", "https://huggingface.co/api/datasets/x"))
    return RepositoryNotFoundError("nope", response=response)


@pytest.fixture
def store() -> HuggingFaceDatasetFileArrowBasedStore:
    return HuggingFaceDatasetFileArrowBasedStore(config={"visibility": "public"})


@pytest.fixture
def dataset() -> Dataset:
    return Dataset.from_dict({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "age": [30, 25, 35]})


def test_init_default_visibility(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    assert store._config == {"visibility": "public"}
    assert store._HuggingFaceDatasetFileBasedStore__dataset_visibility == "public"  # noqa: SLF001
    assert store._dataset_chunk_size == 100000


def test_init_no_config() -> None:
    store = HuggingFaceDatasetFileArrowBasedStore()
    assert store._config is None
    assert store._dataset_chunk_size == 100000


def test_init_visibility_from_config() -> None:
    store = HuggingFaceDatasetFileArrowBasedStore(config={"visibility": "private"})
    assert store._HuggingFaceDatasetFileBasedStore__dataset_visibility == "private"  # noqa: SLF001


def test_init_dataset_chunks_size_from_config() -> None:
    store = HuggingFaceDatasetFileArrowBasedStore(config={"dataset_chunks_size": 2})
    assert store._dataset_chunk_size == 2


def test_init_dataset_chunks_size_default_when_not_in_config() -> None:
    store = HuggingFaceDatasetFileArrowBasedStore(config={"visibility": "public"})
    assert store._dataset_chunk_size == 100000


@pytest.mark.parametrize("bad_chunk_size", [0, -1, 0.5, "2", None])
def test_init_dataset_chunks_size_invalid_raises(bad_chunk_size: object) -> None:
    with pytest.raises(ValueError, match="dataset_chunks_size"):
        HuggingFaceDatasetFileArrowBasedStore(config={"dataset_chunks_size": bad_chunk_size})


def test_get_success(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    ds_dict = DatasetDict({"train": dataset, "test": dataset.select([0]), "validation": dataset.select([1])})
    with patch("tgedr_dataops.store.hf_dataset_file_arrow_based.datasets.load_dataset", return_value=ds_dict) as load:
        result = store.get("owner/dataset")
    load.assert_called_once_with("owner/dataset", streaming=False)
    assert isinstance(result, DatasetSplits)
    assert result.train is dataset
    assert result.test.to_list() == [{"id": 1, "name": "Alice", "age": 30}]
    assert result.validation.to_list() == [{"id": 2, "name": "Bob", "age": 25}]


def test_get_selects_columns(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    ds_dict = DatasetDict({"train": dataset})
    with patch("tgedr_dataops.store.hf_dataset_file_arrow_based.datasets.load_dataset", return_value=ds_dict):
        result = store.get("owner/dataset", columns=["name"])
    assert result.train.column_names == ["name"]


@pytest.mark.parametrize("columns", [None, []])
def test_get_no_columns_filter(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset, columns: list[str] | None) -> None:
    ds_dict = DatasetDict({"train": dataset})
    with patch("tgedr_dataops.store.hf_dataset_file_arrow_based.datasets.load_dataset", return_value=ds_dict):
        result = store.get("owner/dataset", columns=columns)
    assert result.train.column_names == dataset.column_names


def test_get_streaming_option(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    iterable_dict = IterableDatasetDict({"train": IterableDataset.from_dict({"id": [1]})})
    with patch(
        "tgedr_dataops.store.hf_dataset_file_arrow_based.datasets.load_dataset", return_value=iterable_dict
    ) as load:
        result = store.get("owner/dataset", streaming=True)
    load.assert_called_once_with("owner/dataset", streaming=True)
    assert isinstance(result, IterableDatasetSplits)
    assert result.train is iterable_dict["train"]


def test_get_dataset_not_found(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with (
        patch("tgedr_dataops.store.hf_dataset_file_arrow_based.datasets.load_dataset", side_effect=Exception("missing")),
        pytest.raises(NoStoreException, match="not found on Hugging Face Hub"),
    ):
        store.get("missing/key")


def test_assert_dataset_existence_creates_when_missing(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as api:
        api.list_datasets.return_value = iter([])
        store._assert_dataset_existence("owner/dataset")
    api.list_datasets.assert_called_once_with(dataset_name="owner/dataset")
    api.create_repo.assert_called_once_with(
        repo_id="owner/dataset", repo_type="dataset", exist_ok=True, visibility="public"
    )


def test_assert_dataset_existence_exists(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as api:
        api.list_datasets.return_value = iter(["owner/dataset"])
        store._assert_dataset_existence("owner/dataset")
    api.create_repo.assert_not_called()


def test_get_last_file_index_with_files(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "list", return_value=["data/train-00002-00005.parquet", "data/train-00000-00005.parquet"]):
        assert store._get_last_file_index("owner/dataset", "train") == 2


def test_get_last_file_index_empty(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "list", return_value=[]):
        assert store._get_last_file_index("owner/dataset", "train") == -1


def test_get_last_file_index_malformed_filename_raises(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "list", return_value=["owner/dataset/.gitattributes"]), pytest.raises(IndexError):
        store._get_last_file_index("owner/dataset", "train")


def test_get_next_file_index(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "_get_last_file_index", return_value=2):
        assert store._get_next_file_index("owner/dataset", "train") == "00003"


def test_list_split(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    files = ["data/train-00000-00002.parquet", "data/test-00000-00001.parquet", "README.md"]
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as api:
        api.list_repo_files.return_value = files
        assert store.list("owner/dataset", "train") == [files[0]]


def test_list_all(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    files = ["data/train-00000-00001.parquet", "data/test-00000-00001.parquet", "README.md"]
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as api:
        api.list_repo_files.return_value = files
        assert store.list("owner/dataset", "all") == files[:2]


def test_list_repo_not_found(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as api:
        api.list_repo_files.side_effect = repo_not_found_error()
        with pytest.raises(NoStoreException, match="not found on Hugging Face Hub"):
            store.list("owner/dataset", "train")


def test_delete_split(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    files = ["data/train-00000-00002.parquet", "data/train-00001-00002.parquet"]
    with patch.object(store, "list", return_value=files), patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as api:
        store.delete("owner/dataset", "train")
    assert api.delete_file.call_count == 2
    for file in files:
        api.delete_file.assert_any_call(path_in_repo=file, repo_id="owner/dataset", repo_type="dataset")


def test_delete_all(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "_delete_dataset") as delete_dataset:
        store.delete("owner/dataset", "all")
    delete_dataset.assert_called_once_with("owner/dataset")


def test_delete_dataset_removes_cache_and_repo(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    cache = Path("fake_hf_cache") / "owner___dataset"
    cache.mkdir(parents=True, exist_ok=True)
    try:
        with (
            patch("tgedr_dataops.store.hf_dataset_file_based.datasets.config.HF_DATASETS_CACHE", "fake_hf_cache"),
            patch("tgedr_dataops.store.hf_dataset_file_based.HfApi") as api_class,
        ):
            api_class.return_value = MagicMock()
            store._delete_dataset("owner/dataset")
        assert not cache.exists()
        api_class.return_value.delete_repo.assert_called_once_with(repo_id="owner/dataset", repo_type="dataset")
    finally:
        shutil.rmtree(Path("fake_hf_cache"), ignore_errors=True)


def test_delete_dataset_no_cache(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with (
        patch("tgedr_dataops.store.hf_dataset_file_based.datasets.config.HF_DATASETS_CACHE", "nonexistent_cache"),
        patch("tgedr_dataops.store.hf_dataset_file_based.HfApi") as api_class,
    ):
        api_class.return_value = MagicMock()
        store._delete_dataset("owner/dataset")
    api_class.return_value.delete_repo.assert_called_once_with(repo_id="owner/dataset", repo_type="dataset")


def test_delete_dataset_repo_not_found(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with (
        patch("tgedr_dataops.store.hf_dataset_file_based.datasets.config.HF_DATASETS_CACHE", "nonexistent_cache"),
        patch("tgedr_dataops.store.hf_dataset_file_based.HfApi") as api_class,
    ):
        api_class.return_value.delete_repo.side_effect = repo_not_found_error()
        store._delete_dataset("owner/dataset")


def test_delete_dataset_repo_delete_fails(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with (
        patch("tgedr_dataops.store.hf_dataset_file_based.datasets.config.HF_DATASETS_CACHE", "nonexistent_cache"),
        patch("tgedr_dataops.store.hf_dataset_file_based.HfApi") as api_class,
        pytest.raises(NoStoreException, match="deletion failed"),
    ):
        api_class.return_value.delete_repo.side_effect = Exception("no permission")
        store._delete_dataset("owner/dataset")


def test_is_empty_results(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    with patch.object(store, "list", return_value=[]):
        assert store._is_empty("owner/dataset") is True
    with patch.object(store, "list", return_value=["data/train-00000-00001.parquet"]):
        assert store._is_empty("owner/dataset") is False
    with patch.object(store, "list", side_effect=NoStoreException("missing")):
        assert store._is_empty("owner/dataset") is True


def test_store_data_writes_and_uploads_chunks(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset, tmp_path: Path) -> None:
    store = HuggingFaceDatasetFileArrowBasedStore(config={"dataset_chunks_size": 2})
    dataset = dataset
    with (
        patch.object(store, "_get_last_file_index", return_value=2),
        patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as api,
        patch("tgedr_dataops.store.hf_dataset_file_arrow_based.tempfile.TemporaryDirectory") as temporary_dir,
        patch.object(Dataset, "to_parquet") as to_parquet,
    ):
        temporary_dir.return_value.__enter__.return_value = str(tmp_path)
        store._store_data(dataset, "owner/dataset", "train")
    assert to_parquet.call_count == 2
    assert all(call.args[0].parent == tmp_path for call in to_parquet.call_args_list)
    assert api.upload_file.call_count == 2
    assert [call.kwargs["path_in_repo"] for call in api.upload_file.call_args_list] == [
        "data/train-00003-00005.parquet", "data/train-00004-00005.parquet"
    ]


def test_store_data_empty_dataset_uploads_nothing(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    empty = Dataset.from_dict({"name": [], "age": []})
    with patch.object(store, "_get_last_file_index") as index, patch.object(store, "_HuggingFaceDatasetFileBasedStore__api") as api:
        store._store_data(empty, "owner/dataset", "train")
    api.upload_file.assert_not_called()
    index.assert_not_called()


def test_save_rejects_iterable_dataset(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    stream = IterableDataset.from_dict({"id": [1]})
    with pytest.raises(TypeError, match="does not support IterableDataset"):
        store.save(stream, "owner/dataset")


def test_save_invalid_split_raises(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    with pytest.raises(ValueError, match="Invalid split"):
        store.save(dataset, "owner/dataset", "bogus")


def test_save_append(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    with (
        patch.object(store, "_assert_dataset_existence") as assert_exists,
        patch.object(store, "_store_data") as store_data,
        patch.object(store, "_is_empty") as is_empty,
        patch.object(store, "delete") as delete,
    ):
        store.save(dataset, "owner/dataset", "train", append=True)
    assert_exists.assert_called_once_with("owner/dataset")
    store_data.assert_called_once_with(dataset, "owner/dataset", "train")
    is_empty.assert_not_called()
    delete.assert_not_called()


@pytest.mark.parametrize("append", ["true", True, 1])
def test_save_append_truthy_values(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset, append: object) -> None:
    with patch.object(store, "_assert_dataset_existence"), patch.object(store, "_store_data") as store_data:
        store.save(dataset, "owner/dataset", append=append)
    store_data.assert_called_once_with(dataset, "owner/dataset", "train")


def test_save_append_false_string(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "_store_data"),
        patch.object(store, "_is_empty", return_value=False) as is_empty,
        patch.object(store, "delete") as delete,
    ):
        store.save(dataset, "owner/dataset", "train", append="false")
    is_empty.assert_called_once_with("owner/dataset")
    delete.assert_called_once_with("owner/dataset", "train")


def test_save_replace_empty_repo(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "_store_data") as store_data,
        patch.object(store, "_is_empty", return_value=True) as is_empty,
        patch.object(store, "delete") as delete,
    ):
        store.save(dataset, "owner/dataset", "validation")
    is_empty.assert_called_once_with("owner/dataset")
    delete.assert_not_called()
    store_data.assert_called_once_with(dataset, "owner/dataset", "validation")


def test_save_replace_non_empty(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "_store_data"),
        patch.object(store, "_is_empty", return_value=False),
        patch.object(store, "delete") as delete,
    ):
        store.save(dataset, "owner/dataset")
    delete.assert_called_once_with("owner/dataset", "train")


def test_update_invalid_split_raises(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    with pytest.raises(ValueError, match="Invalid split"):
        store.update(dataset, "owner/dataset", "bogus", ["id"])


def test_update_split_missing(store: HuggingFaceDatasetFileArrowBasedStore, dataset: Dataset) -> None:
    existing = DatasetSplits(test=Dataset.from_dict({"id": [9], "name": ["Zoe"], "age": [40]}))
    with patch.object(store, "_assert_dataset_existence"), patch.object(store, "get", return_value=existing), patch.object(store, "save") as save:
        store.update(dataset, "owner/dataset", "train", ["id"])
    save.assert_called_once_with(df=dataset, key="owner/dataset", split="train")


def test_update_replaces_matching_rows(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    incoming = Dataset.from_dict({"id": [1, 2], "name": ["Alice", "New"], "age": [31, 26]})
    existing = DatasetSplits(train=Dataset.from_dict({"id": [1, 2], "name": ["Old", "Bob"], "age": [30, 25]}))
    with patch.object(store, "_assert_dataset_existence"), patch.object(store, "get", return_value=existing), patch.object(store, "save") as save:
        store.update(incoming, "owner/dataset", "train", ["id"])
    saved = save.call_args.kwargs["df"]
    assert saved.to_list() == [{"id": 1, "name": "Alice", "age": 31}, {"id": 2, "name": "New", "age": 26}]


def test_update_appends_unmatched_rows(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    incoming = Dataset.from_dict({"id": [1, 2], "name": ["Updated", "New"]})
    existing = DatasetSplits(train=Dataset.from_dict({"id": [1], "name": ["Old"]}))
    with patch.object(store, "_assert_dataset_existence"), patch.object(store, "get", return_value=existing), patch.object(store, "save") as save:
        store.update(incoming, "owner/dataset", "train", ["id"])
    assert save.call_args.kwargs["df"].to_list() == [{"id": 1, "name": "Updated"}, {"id": 2, "name": "New"}]


def test_update_no_matching_rows_appends_all(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    incoming = Dataset.from_dict({"id": [1, 2], "name": ["Alice", "Bob"]})
    existing = DatasetSplits(train=Dataset.from_dict({"id": [10], "name": ["Old"]}))
    with patch.object(store, "_assert_dataset_existence"), patch.object(store, "get", return_value=existing), patch.object(store, "save") as save:
        store.update(incoming, "owner/dataset", "train", ["id"])
    assert save.call_count == 1
    assert save.call_args.kwargs["df"].to_list() == [{"id": 10, "name": "Old"}, {"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]


def test_update_same_schema_and_no_unmatched_rows_avoids_concatenation(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    incoming = Dataset.from_dict({"id": [1], "name": ["Updated"]})
    existing = DatasetSplits(train=Dataset.from_dict({"id": [1], "name": ["Old"]}))
    with (
        patch.object(store, "_assert_dataset_existence"),
        patch.object(store, "get", return_value=existing),
        patch.object(store, "save") as save,
        patch("tgedr_dataops.store.hf_dataset_file_arrow_based.concatenate_datasets") as concatenate,
    ):
        store.update(incoming, "owner/dataset", "train", ["id"])
    concatenate.assert_not_called()
    assert save.call_args.kwargs["df"].to_list() == [{"id": 1, "name": "Updated"}]


def test_update_rejects_iterable_input(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    stream = IterableDataset.from_dict({"id": [1]})
    with patch.object(store, "_assert_dataset_existence"), patch.object(
        store, "get", return_value=DatasetSplits()
    ):
        with pytest.raises(TypeError, match="does not support IterableDataset"):
            store.update(stream, "owner/dataset", "train", ["id"])


def test_update_rejects_iterable_existing_split(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    incoming = Dataset.from_dict({"id": [1]})
    stream = IterableDataset.from_dict({"id": [1]})
    existing = IterableDatasetSplits(train=stream)
    with patch.object(store, "_assert_dataset_existence"), patch.object(store, "get", return_value=existing):
        with pytest.raises(TypeError, match="does not support IterableDataset"):
            store.update(incoming, "owner/dataset", "train", ["id"])


def test_update_invalid_key_field_raises(store: HuggingFaceDatasetFileArrowBasedStore) -> None:
    incoming = Dataset.from_dict({"id": [1], "name": ["Alice"]})
    existing = DatasetSplits(train=Dataset.from_dict({"id": [1], "name": ["Old"]}))
    with patch.object(store, "_assert_dataset_existence"), patch.object(store, "get", return_value=existing):
        with pytest.raises(KeyError):
            store.update(incoming, "owner/dataset", "train", ["missing"])
