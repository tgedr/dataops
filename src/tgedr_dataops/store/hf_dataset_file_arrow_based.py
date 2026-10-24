# Copyright (c) 2023 tgedr contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""Module for interacting with Hugging Face datasets."""

from typing import Any, ClassVar, List  # noqa: UP035
import logging
import math
from datasets import Dataset, DatasetDict, IterableDataset, IterableDatasetDict, concatenate_datasets
from pathlib import Path
import datasets
import tempfile

from tgedr_dataops_abs.store import NoStoreException
from tgedr_dataops.store.data_splits import DatasetSplits, IterableDatasetSplits
from tgedr_dataops.store.hf_dataset_file_based import HuggingFaceDatasetFileBasedStore

logger = logging.getLogger(__name__)


class HuggingFaceDatasetFileArrowBasedStore(HuggingFaceDatasetFileBasedStore):
    """Store Hugging Face Datasets as partitioned Parquet files on the Hub."""

    __VALID_SPLITS: ClassVar[list[str]] = ["train", "test", "validation"]

    def get(
        self, key: str, columns: list[str] | None = None, streaming: bool = False, **kwargs: Any
    ) -> DatasetSplits | IterableDatasetSplits:
        """Load a Hub dataset as a container of Arrow-backed splits.

        Parameters
        ----------
        key : str
            Dataset identifier on the Hugging Face Hub.
        columns : list[str] | None
            Optional columns to select from each split. None or an empty list
            leaves all columns selected.
        streaming : bool
            Whether to request the dataset in streaming mode.
        **kwargs : Any
            Additional keyword arguments. Currently accepted but not forwarded
            to ``datasets.load_dataset``.

        Returns
        -------
        DatasetSplits | IterableDatasetSplits
            The loaded regular or streaming splits, when present.

        Raises
        ------
        NoStoreException
            If the dataset cannot be loaded.
        """
        logger.info(f"[get|in] ({key}, {columns}, {streaming}, {kwargs})")

        try:
            ds: DatasetDict | IterableDatasetDict = datasets.load_dataset(key, streaming=streaming)
        except Exception as nfe:
            raise NoStoreException(f"[get] dataset '{key}' not found on Hugging Face Hub.") from nfe

        if (columns is not None) and (0 < len(columns)):
            ds = ds.select_columns(columns)

        if isinstance(ds, IterableDatasetDict):
            result = IterableDatasetSplits.from_iterable_dataset_dict(ds)
        else:
            result = DatasetSplits.from_dataset_dict(ds)
        logger.info(f"[get|out] => shape: {result}")
        return result

    def _store_data(self, df: Dataset, key: str, split: str) -> None:
        """Write non-empty dataset shards to Parquet and upload them.

        Parameters
        ----------
        df : Dataset
            The dataset to store.
        key : str
            Dataset identifier on the Hugging Face Hub.
        split : str
            Name of the split represented by the dataset.
        """
        logger.info(f"[_store_data|in] ({df.shape}, {key}, {split})")
        if 0 == df.num_rows:
            logger.info("[_store_data] nothing to store, df is empty")
            logger.info("[_store_data|out]")
            return

        num_chunks = math.ceil(df.num_rows / self._dataset_chunk_size)
        dfs: list[Dataset] = [df.shard(num_shards=num_chunks, index=i) for i in range(num_chunks)]

        last_index: int = self._get_last_file_index(key, split)
        n: int = last_index + 1 + len(dfs)
        with tempfile.TemporaryDirectory() as tmp_dir:
            for df in dfs:  # noqa: PLR1704
                last_index += 1
                file_name = f"{split}-{last_index:05d}-{n:05d}.parquet"
                file_path = Path(tmp_dir) / file_name
                df.to_parquet(file_path)
                self._api.upload_file(
                    path_or_fileobj=file_path, path_in_repo=f"data/{file_name}", repo_id=key, repo_type="dataset"
                )
        logger.info("[_store_data|out]")

    def save(self, df: Dataset, key: str, split: str = "train", **kwargs) -> Any:  # noqa: ANN003
        """Save a dataset split to the Hub, replacing it unless appending.

        Parameters
        ----------
        df : Dataset
            The Arrow-backed dataset to save.
        key : str
            Dataset identifier on the Hugging Face Hub.
        split : str
            Split name; must be train, test, or validation.
        **kwargs : Any
            Store options. ``append`` may be true, ``1``, or ``"true"`` to
            append files instead of replacing the split.

        Raises
        ------
        ValueError
            If ``split`` is not a supported split name.
        TypeError
            If ``df`` is a streaming dataset.
        """
        if isinstance(df, IterableDataset):
            raise TypeError("save does not support IterableDataset; materialize the data first.")
        logger.info(f"[save|in] ({df.shape}, {key}, {split}, {kwargs})")
        if split not in self.__VALID_SPLITS:
            raise ValueError(f"Invalid split: '{split}'. Valid splits are: {self.__VALID_SPLITS}")

        append: bool = False
        if "append" in kwargs:
            append = kwargs["append"] == "true" or kwargs["append"] is True or kwargs["append"] == 1

        self._assert_dataset_existence(key)
        if append:
            self._store_data(df, key, split)
        else:
            if not self._is_empty(key):
                self.delete(key, split)
            self._store_data(df, key, split)
        logger.info("[save|out]")

    def update(self, df: Dataset, key: str, split: str, key_fields: List[str], **kwargs) -> Any:  # noqa: ANN003, UP006
        """Update records in a Hub dataset split by matching key columns.

        Existing rows whose key values occur in ``df`` are replaced with the
        incoming values. Incoming rows whose keys do not exist are appended.
        If the split does not yet exist, ``df`` becomes the split contents.

        Parameters
        ----------
        df : Dataset
            Incoming Arrow-backed rows to apply.
        key : str
            Dataset identifier on the Hugging Face Hub.
        split : str
            Split name; must be train, test, or validation.
        key_fields : List[str]
            Column names whose values identify matching rows.
        **kwargs : Any
            Reserved for additional store-specific options.

        Raises
        ------
        ValueError
            If ``split`` is not a supported split name.
        KeyError
            If a key field is missing from a row in either dataset.
        """
        logger.info(f"[update|in] ({getattr(df, 'shape', 'streaming dataset')}, {key}, {split}, {key_fields}, {kwargs})")
        if split not in self.__VALID_SPLITS:
            raise ValueError(f"Invalid split: '{split}'. Valid splits are: {self.__VALID_SPLITS}")
        if isinstance(df, IterableDataset):
            raise TypeError("update does not support IterableDataset; materialize the data first.")

        self._assert_dataset_existence(key)
        dss_baseline: DatasetSplits = self.get(key)
        ds_existing = dss_baseline.get_split(split)

        if ds_existing is None:
            ds2save = df
        elif isinstance(ds_existing, IterableDataset):
            raise TypeError("update does not support IterableDataset; materialize the data first.")
        else:
            def row_key(row: dict) -> tuple:
                return tuple(row[field] for field in key_fields)

            incoming_keys = {row_key(row): row for row in df.to_list()}
            existing_keys = {row_key(row) for row in ds_existing.to_list()}

            ds_updated = ds_existing.map(
                lambda row: {**row, **incoming_keys.get(row_key(row), {})},
                load_from_cache_file=False,
            )
            ds_unmatched = df.filter(
                lambda row: row_key(row) not in existing_keys,
                load_from_cache_file=False,
            )

            ds2save = concatenate_datasets([ds_updated, ds_unmatched]) if ds_unmatched.num_rows else ds_updated

        self.save(df=ds2save, key=key, split=split)
        logger.info("[update|out]")
