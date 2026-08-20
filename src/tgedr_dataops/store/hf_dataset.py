


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

from dataclasses import dataclass
import shutil
from pathlib import Path
from typing import Any
import logging
import pandas as pd
import datasets
from datasets import Dataset, DatasetDict, load_dataset

from huggingface_hub import HfApi
from tgedr_dataops_abs.store import Store, NoStoreException

logger = logging.getLogger(__name__)

@dataclass
class DataFrameSplits:
    """Container for train, test, and optional validation DataFrames.

    Attributes
    ----------
    train : pd.DataFrame | None
        Training dataset.
    test : pd.DataFrame | None
        Test dataset.
    validation : pd.DataFrame | None
        Validation dataset, optional.
    """

    train: pd.DataFrame | None = None
    test: pd.DataFrame | None = None
    validation: pd.DataFrame | None = None

    def __eq__(self, other: object) -> bool:
        """Check equality of two DataFrameSplits instances.

        Parameters
        ----------
        other : object
            Another DataFrameSplits instance to compare with.

        Returns
        -------
        bool
            True if both instances are equal, False otherwise.
        """
        if not isinstance(other, DataFrameSplits):
            return False

        return (
            self.train.equals(other.train)
            if self.train is not None and other.train is not None
            else self.train is None and other.train is None
        ) and (
            self.test.equals(other.test)
            if self.test is not None and other.test is not None
            else self.test is None and other.test is None
        ) and (
            self.validation.equals(other.validation)
            if self.validation is not None and other.validation is not None
            else self.validation is None and other.validation is None
        )

    def __hash__(self) -> int:
        """Generate a hash for the DataFrameSplits instance.

        Returns
        -------
        int
            Hash value of the instance.
        """
        return hash(
            (
                id(self.train),
                id(self.test),
                id(self.validation),
            )
        )

    def equals(self, other: "DataFrameSplits") -> bool:
        """Check equality of two DataFrameSplits instances.

        Parameters
        ----------
        other : DataFrameSplits
            Another DataFrameSplits instance to compare with.

        Returns
        -------
        bool
            True if both instances are equal, False otherwise.
        """
        return self.__eq__(other)

    def union(self, df: "DataFrameSplits") -> "DataFrameSplits":
        """Combine train, test, and validation DataFrames into the current DataFrameSplits instance."""
        if df.train is not None and self.train is not None:
            self.train = pd.concat([self.train, df.train], ignore_index=True)
            self.train = self.train.drop_duplicates().reset_index(drop=True)
        elif df.train is not None:
            self.train = df.train
        elif self.train is not None:
            pass  # Keep the existing train DataFrame

        if df.validation is not None and self.validation is not None:
            self.validation = pd.concat([self.validation, df.validation], ignore_index=True)
            self.validation = self.validation.drop_duplicates().reset_index(drop=True)
        elif df.validation is not None:
            self.validation = df.validation
        elif self.validation is not None:
            pass  # Keep the existing validation DataFrame

        if df.test is not None and self.test is not None:
            self.test = pd.concat([self.test, df.test], ignore_index=True)
            self.test = self.test.drop_duplicates().reset_index(drop=True)
        elif df.test is not None:
            self.test = df.test
        elif self.test is not None:
            pass  # Keep the existing test DataFrame

        return self

    @classmethod
    def from_dict(cls, data: dict[str, pd.DataFrame]) -> "DataFrameSplits":
        """Create a DataFrameSplits instance from a dictionary.

        Parameters
        ----------
        data : dict[str, pd.DataFrame]
            Dictionary containing 'train', 'test', and optional 'validation' DataFrames.

        Returns
        -------
        DataFrameSplits
            A new instance of DataFrameSplits.
        """
        result = cls(
            train=data.get("train"),
            test=data.get("test"),
            validation=data.get("validation"),
        )
        return result

    def to_dataset_dict(self) -> DatasetDict:
        """Convert the DataFrameSplits instance to a Hugging Face DatasetDict.

        Returns
        -------
        DatasetDict
            A DatasetDict containing the train, test, and optional validation datasets.
        """
        ds_dict = {}
        if self.train is not None:
            ds_dict["train"] = Dataset.from_pandas(self.train.reset_index(drop=True))
        if self.validation is not None:
            ds_dict["validation"] = Dataset.from_pandas(self.validation.reset_index(drop=True))
        if self.test is not None:
            ds_dict["test"] = Dataset.from_pandas(self.test.reset_index(drop=True))

        ds = DatasetDict(ds_dict)
        return ds

    @property
    def shape(self) -> str:
        """Return the shapes of the train, test, and validation DataFrames as a string.

        Returns
        -------
        str
            A string representation of the shapes of the DataFrames.
        """
        return f"train: {self.train.shape if self.train is not None else 'None'}, test: {self.test.shape if self.test is not None else 'None'}, validation: {self.validation.shape if self.validation is not None else 'None'}"


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
