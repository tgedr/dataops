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
import re
import shutil
from pathlib import Path
import datasets
import pandas as pd
import tempfile
from huggingface_hub import HfApi
from huggingface_hub.utils import RepositoryNotFoundError
from tgedr_dataops_abs.store import Store, NoStoreException

from tgedr_dataops.store.hf_dataset import DataFrameSplits

logger = logging.getLogger(__name__)


class HuggingFaceDatasetFileBasedStore(Store):
    """Store pandas DataFrames as partitioned Parquet files on Hugging Face Hub."""

    __VALID_SPLITS: ClassVar[list[str]] = ["train", "test", "validation"]
    __DATASET_CHUNKS_SIZE: ClassVar[int] = 2

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize store with optional configuration.

        Parameters
        ----------
        config : dict[str, Any] | None
            Configuration dictionary for the store.
        """
        super().__init__(config=config)
        self.__dataset_visibility: str = "private"
        if config is not None:
            self.__dataset_visibility = config.get("visibility", "private")
        self.__api = HfApi()

    def get(self, key: str, **kwargs) -> DataFrameSplits:  # noqa: ANN003
        """Load a dataset from Hugging Face Hub as DataFrameSplits.

        Parameters
        ----------
        key : str
            Dataset identifier (e.g. "user/dataset") on Hugging Face Hub.
        **kwargs : Any
            Additional keyword arguments passed to the dataset loading function.

        Returns
        -------
        DataFrameSplits
            The loaded dataset splits as DataFrames.
        """
        logger.info(f"[get|in] ({key}, {kwargs})")
        try:
            ds = datasets.load_dataset(key)
        except Exception as nfe:
            raise NoStoreException(f"[get] dataset '{key}' not found on Hugging Face Hub.") from nfe

        ds_dict = {}

        for split in ds:
            df = ds[split].to_pandas()
            if "__index_level_0__" in df.columns:
                df = df.drop(columns=["__index_level_0__"])
            ds_dict[split] = df

        result = DataFrameSplits.from_dict(ds_dict)
        logger.info(f"[get|out] => shape: {result.shape}")
        return result

    def _assert_dataset_existence(self, key: str) -> None:
        logger.info(f"[_assert_dataset_existence|in] ({key})")
        if 0 == len(list(self.__api.list_datasets(dataset_name=key))):
            self.__api.create_repo(
                repo_id=key, repo_type="dataset", exist_ok=True, visibility=self.__dataset_visibility
            )
        logger.info("[_assert_dataset_existence|out]")

    def _get_last_file_index(self, key: str, split: str = "train") -> int:
        logger.info(f"[_get_last_file_index|in] ({key}, {split})")
        default_index: int = -1
        files = self.list(key=key, split=split)
        indices = [int(f.split("_")[-1].split(".")[0]) for f in files]
        result = max(indices) if indices else default_index
        logger.info(f"[_get_last_file_index|out] => {result}")
        return result

    def _get_next_file_index(self, key: str, split: str = "train") -> str:
        logger.info(f"[_get_next_file_index|in] ({key}, {split})")
        last_index = self._get_last_file_index(key, split)
        result = f"{last_index + 1:05d}"
        logger.info(f"[_get_next_file_index|out] => {result}")
        return result

    def list(self, key: str, split: str = "train") -> list[str]:  # noqa: D102
        logger.info(f"[list|in] ({key}, {split})")
        try:
            pattern = re.compile(r"^(train|test|validation)_")
            files = self.__api.list_repo_files(key, repo_type="dataset")
            files = [f for f in files if pattern.match(f)]
            result = [f for f in files if f.startswith(f"{split}_")] if split != "all" else files
            logger.info(f"[list|out] => {result}")
            return result  # noqa: TRY300
        except RepositoryNotFoundError as e:
            raise NoStoreException(f"[list] dataset '{key}' not found on Hugging Face Hub.") from e

    def _delete_dataset(self, key: str) -> None:
        logger.info(f"[_delete_dataset|in] (key={key})")
        # Clean local cache without re-downloading
        cache_dir = Path(datasets.config.HF_DATASETS_CACHE) / key.replace("/", "___")
        if cache_dir.exists():
            shutil.rmtree(cache_dir)
        # Delete remote
        api = HfApi()
        try:
            api.delete_repo(repo_id=key, repo_type="dataset")
        except RepositoryNotFoundError as e:
            logger.warning(f"[_delete_dataset] no repository: {e}")
        except Exception as nfe:
            raise NoStoreException(f"[delete] dataset '{key}' deletion failed") from nfe
        logger.info("[_delete_dataset|out]")

    def delete(self, key: str, split: str = "train") -> None:
        """Delete a dataset split from Hugging Face Hub.

        Parameters
        ----------
        key : str
            Dataset identifier (e.g. "user/dataset") on Hugging Face Hub.
        split : str
            Split to delete ("train", "test", "validation", or "all" to delete the entire dataset).
        """
        logger.info(f"[delete|in] ({key}, {split})")

        if split == "all":
            self._delete_dataset(key)
        else:
            files = self.list(key, split)
            for f in files:
                self.__api.delete_file(path_in_repo=f, repo_id=key, repo_type="dataset")
        logger.info("[delete|out]")

    def _is_empty(self, key: str) -> bool:
        logger.info(f"[_is_empty|in] ({key})")
        try:
            files = self.list(key=key, split="all")
            result = len(files) == 0
        except NoStoreException:
            result = True
        logger.info(f"[_is_empty|out] => {result}")
        return result

    def _store_data(self, df: pd.DataFrame, key: str, split: str) -> None:
        logger.info(f"[_store_data|in] ({df.shape}, {key}, {split})")
        dfs: list[pd.DataFrame] = [
            df.iloc[i : i + self.__DATASET_CHUNKS_SIZE] for i in range(0, df.shape[0], self.__DATASET_CHUNKS_SIZE)
        ]
        last_index: int = self._get_last_file_index(key, split)
        with tempfile.TemporaryDirectory() as tmp_dir:
            for df in dfs:  # noqa: PLR1704
                last_index += 1
                file_name = f"{split}_{last_index:05d}.parquet"
                file_path = Path(tmp_dir) / file_name
                df.to_parquet(file_path, index=False)
                self.__api.upload_file(
                    path_or_fileobj=file_path, path_in_repo=file_name, repo_id=key, repo_type="dataset"
                )
        logger.info("[_store_data|out]")

    def save(self, df: pd.DataFrame, key: str, split: str = "train", **kwargs) -> Any:  # noqa: ANN003
        """Save a dataset split on Hugging Face Hub.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing the data to save.
        key : str
            Dataset identifier (repo_id) on the Hugging Face Hub.
        split : str, optional
            Dataset split to save to (default is "train").
        **kwargs
            Additional keyword arguments. If "append" is truthy, data is
            appended to the existing dataset; otherwise the split is
            replaced with the new data.

        Raises
        ------
        ValueError
            If the split is not a valid split.
        """
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

    def update(self, df: pd.DataFrame, key: str, split: str, key_fields: List[str], **kwargs) -> Any:  # noqa: ANN003, UP006
        """Update a dataset split on Hugging Face Hub based on key fields.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing the new data to update.
        key : str
            Dataset identifier (e.g. "user/dataset") on Hugging Face Hub.
        split : str
            Split to update ("train", "test", or "validation").
        key_fields : List[str]
            List of column names to use as key fields for matching rows.
        **kwargs : Any
            Additional keyword arguments passed to the save function.
        """
        logger.info(f"[update|in] ({df.shape}, {key}, {split}, {key_fields}, {kwargs})")
        if split not in self.__VALID_SPLITS:
            raise ValueError(f"Invalid split: '{split}'. Valid splits are: {self.__VALID_SPLITS}")

        self._assert_dataset_existence(key)

        existing_data: DataFrameSplits = self.get(key)
        if not existing_data.has_split(split):
            data_to_update: pd.DataFrame = pd.DataFrame()
        else:
            data_to_update: pd.DataFrame = existing_data.get_split(split)
            match = pd.merge(data_to_update.reset_index(), df.reset_index(), on=key_fields)
            if match.empty:
                logger.info(f"[update] no matching rows found for key fields {key_fields}, appending new data")
                self.save(df=df, key=key, split=split, append=True)
            else:
                index_left = match["index_x"]
                index_right = match["index_y"]
                # update matching rows
                data_to_update.iloc[index_left] = df.iloc[index_right]
                if len(match) < len(df):
                    logger.info(f"[update] some rows not matched for key fields {key_fields}, appending unmatched rows")
                    df_unmatched = df.iloc[~index_right]
                    data_to_update = pd.concat([data_to_update, df_unmatched], ignore_index=True)
                data_to_update = data_to_update.drop_duplicates(inplace=False)

        self.save(df=data_to_update, key=key, split=split)
        logger.info("[update|out]")
