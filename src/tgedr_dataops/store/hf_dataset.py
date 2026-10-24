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

import shutil
from pathlib import Path
from typing import Any
import logging
import datasets
from datasets import load_dataset

from huggingface_hub import HfApi
from tgedr_dataops_abs.store import Store, NoStoreException

from tgedr_dataops.store.data_splits import DataFrameSplits

logger = logging.getLogger(__name__)


class HuggingFaceDatasetStore(Store):
    """HuggingFaceDatasetStore provides methods to interact with Hugging Face datasets."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize store with optional configuration.

        Parameters
        ----------
        config : dict[str, Any] | None
            Configuration dictionary for the store.
        """
        super().__init__(config=config)

    def get(self, key: str, **kwargs) -> DataFrameSplits:  # noqa: ANN003, ARG002
        """Get data from the store by key.

        Parameters
        ----------
        key : str
            The key identifying the data to retrieve.
        **kwargs
            Additional store-specific parameters.

        Returns
        -------
        DataFrameSplits
            Retrieved data.

        Raises
        ------
        NoStoreException
            If the dataset is not found on Hugging Face Hub.

        """
        logger.info(f"[get|in] (key={key})")
        try:
            ds = load_dataset(key)
        except Exception as nfe:
            raise NoStoreException(f"Dataset '{key}' not found on Hugging Face Hub.") from nfe

        ds_dict = {}

        for split in ds:
            df = ds[split].to_pandas()
            if "__index_level_0__" in df.columns:
                df = df.drop(columns=["__index_level_0__"])
            ds_dict[split] = df

        result = DataFrameSplits.from_dict(ds_dict)
        logger.info(f"[get|out] => shape: {result.shape}")
        return result

    def delete(self, key: str, **kwargs) -> None:  # noqa: ANN003, ARG002
        """Delete data from the store by key.

        Parameters
        ----------
        key : str
            The key identifying the data to delete.
        **kwargs
            Additional store-specific parameters.
        """
        logger.info(f"[delete|in] (key={key})")
        # Clean local cache without re-downloading

        cache_dir = Path(datasets.config.HF_DATASETS_CACHE) / key.replace("/", "___")
        if cache_dir.exists():
            shutil.rmtree(cache_dir)

        # Delete remote
        api = HfApi()
        try:
            api.delete_repo(repo_id=key, repo_type="dataset")
        except Exception as nfe:
            raise NoStoreException(f"Dataset '{key}' not deletable on Hugging Face Hub.") from nfe

        logger.info("[delete|out]")

    def save(self, df: DataFrameSplits, key: str, **kwargs) -> Any:  # noqa: ANN003, ARG002
        """Save data to the store.

        Parameters
        ----------
        df : DataFrameSplits
            The data to save.
        key : str
            The key to associate with the data.
        **kwargs
            Additional store-specific parameters.
        """
        logger.info(f"[save|in] (df shape={df.shape}, key={key})")

        ds = df.to_dataset_dict()
        ds.push_to_hub(key)
        logger.info("[save|out]")

    def update(self, df: DataFrameSplits, key: str, **kwargs) -> Any:  # noqa: ANN003
        """Update existing data in the store.

        Parameters
        ----------
        df : DataFrameSplits
            The updated data.
        key : str
            The key identifying the data to update.
        **kwargs
            Additional store-specific parameters.
        """
        logger.info(f"[update|in] (df shape={df.shape}, key={key})")

        if kwargs.get("append"):
            dfs: DataFrameSplits = self.get(key=key)
            dfs = dfs.union(df)
            self.save(dfs, key=key)
        else:
            self.save(df, key=key)

        logger.info("[update|out]")
