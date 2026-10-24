
"""Containers for Hugging Face datasets grouped by split."""

from dataclasses import dataclass
import math
from typing import Any

import pandas as pd
from datasets import Dataset, DatasetDict, IterableDataset, IterableDatasetDict, concatenate_datasets


def _freeze(value: Any) -> Any:
    """Convert dataset values and feature metadata to hashable forms."""
    if isinstance(value, dict):
        return tuple(sorted((_freeze(key), _freeze(item)) for key, item in value.items()))
    if isinstance(value, list | tuple):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    if isinstance(value, float) and math.isnan(value):
        return (float, "nan")
    try:
        hash(value)
    except TypeError:
        return repr(value)
    return value


def _dataset_contents(dataset: Dataset) -> tuple[Any, ...]:
    """Return a comparable, hashable snapshot of a Dataset's logical data."""
    return (
        _freeze(dataset.features.to_dict()),
        tuple(
            tuple(_freeze(row.get(column)) for column in dataset.column_names)
            for row in dataset.to_list()
        ),
    )


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
            (
                self.train.equals(other.train)
                if self.train is not None and other.train is not None
                else self.train is None and other.train is None
            )
            and (
                self.test.equals(other.test)
                if self.test is not None and other.test is not None
                else self.test is None and other.test is None
            )
            and (
                self.validation.equals(other.validation)
                if self.validation is not None and other.validation is not None
                else self.validation is None and other.validation is None
            )
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

    def has_split(self, split: str) -> bool:
        """Check whether the given split is present in this instance.

        Parameters
        ----------
        split : str
            Name of the split (e.g., "train", "test", or "validation").

        Returns
        -------
        bool
            True if the split is not None, False otherwise.
        """
        return getattr(self, split) is not None

    def get_split(self, split: str) -> pd.DataFrame | None:
        """Retrieve the DataFrame for the given split.

        Parameters
        ----------
        split : str
            Name of the split (e.g., "train", "test", or "validation").

        Returns
        -------
        pd.DataFrame | None
            The DataFrame corresponding to the split, or None if it doesn't exist.
        """
        return getattr(self, split)

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


@dataclass
class DatasetSplits:
    """Container for train, test, and optional regular datasets."""

    train: Dataset | None = None
    test: Dataset | None = None
    validation: Dataset | None = None

    def __eq__(self, other: object) -> bool:
        """Compare split names, schemas, and Arrow table contents."""
        if not isinstance(other, DatasetSplits):
            return False

        return all(
            (left is None and right is None)
            or (
                left is not None
                and right is not None
                and _dataset_contents(left) == _dataset_contents(right)
            )
            for left, right in (
                (self.train, other.train),
                (self.test, other.test),
                (self.validation, other.validation),
            )
        )

    def __hash__(self) -> int:
        """Hash split schemas and values consistently with equality."""
        return hash(
            tuple(
                None if dataset is None else _dataset_contents(dataset)
                for dataset in (self.train, self.test, self.validation)
            )
        )

    def equals(self, other: object) -> bool:
        """Return whether another split container has the same split contents."""
        return self == other

    @classmethod
    def from_dataset_dict(cls, data: DatasetDict) -> "DatasetSplits":
        """Create split properties from a regular DatasetDict."""
        return cls(
            train=data.get("train"),
            test=data.get("test"),
            validation=data.get("validation"),
        )

    def to_dataset_dict(self) -> DatasetDict:
        """Return the present splits as a regular DatasetDict."""
        datasets_by_split = {
            name: dataset
            for name in ("train", "test", "validation")
            if (dataset := getattr(self, name)) is not None
        }
        return DatasetDict(datasets_by_split)

    def has_split(self, split: str) -> bool:
        """Return whether a known split is present."""
        if split not in ("train", "test", "validation"):
            raise ValueError(f"Invalid split: {split!r}")
        return getattr(self, split) is not None

    def get_split(self, split: str) -> Dataset | None:
        """Return a known regular split, or None if it is absent."""
        if split not in ("train", "test", "validation"):
            raise ValueError(f"Invalid split: {split!r}")
        return getattr(self, split)

    def union(self, other: "DatasetSplits") -> "DatasetSplits":
        """Return splits concatenated with the corresponding splits in other."""
        combined = {}
        for name in ("train", "test", "validation"):
            left = getattr(self, name)
            right = getattr(other, name)
            if left is not None and right is not None:
                combined[name] = concatenate_datasets([left, right])
            else:
                combined[name] = left if left is not None else right
        return DatasetSplits(**combined)

    @property
    def shape(self) -> str:
        """Return row and column counts for each split."""
        def dimensions(dataset: Dataset | None) -> tuple[int, int] | None:
            if dataset is None:
                return None
            return dataset.num_rows, dataset.num_columns

        return (
            f"train: {dimensions(self.train)}, test: {dimensions(self.test)}, validation: {dimensions(self.validation)}"
        )


@dataclass
class IterableDatasetSplits:
    """Container for streaming train, test, and optional validation datasets.

    Streaming split operations avoid consuming datasets; equality and hashing
    therefore compare object identity rather than records.
    """

    train: IterableDataset | None = None
    test: IterableDataset | None = None
    validation: IterableDataset | None = None

    def __eq__(self, other: object) -> bool:
        """Compare the identities and positions of streaming splits."""
        if not isinstance(other, IterableDatasetSplits):
            return False
        return all(
            left is right
            for left, right in (
                (self.train, other.train),
                (self.test, other.test),
                (self.validation, other.validation),
            )
        )

    def __hash__(self) -> int:
        """Hash the identities of the contained streaming datasets."""
        return hash((id(self.train), id(self.test), id(self.validation)))

    def equals(self, other: object) -> bool:
        """Return whether another container holds the same stream objects."""
        return self == other

    @classmethod
    def from_iterable_dataset_dict(cls, data: IterableDatasetDict) -> "IterableDatasetSplits":
        """Create split properties from a Hugging Face IterableDatasetDict."""
        return cls(
            train=data.get("train"),
            test=data.get("test"),
            validation=data.get("validation"),
        )

    def to_iterable_dataset_dict(self) -> IterableDatasetDict:
        """Return the present splits as a streaming IterableDatasetDict."""
        return IterableDatasetDict(
            {name: dataset for name in ("train", "test", "validation") if (dataset := getattr(self, name)) is not None}
        )

    def has_split(self, split: str) -> bool:
        """Return whether a known streaming split is present."""
        if split not in ("train", "test", "validation"):
            raise ValueError(f"Invalid split: {split!r}")
        return getattr(self, split) is not None

    def get_split(self, split: str) -> IterableDataset | None:
        """Return a known streaming split, or None if it is absent."""
        if split not in ("train", "test", "validation"):
            raise ValueError(f"Invalid split: {split!r}")
        return getattr(self, split)

    @property
    def shape(self) -> str:
        """Return column counts for streams, whose row counts are unknown."""
        def dimensions(dataset: IterableDataset | None) -> tuple[None, int] | None:
            if dataset is None:
                return None
            return None, dataset.num_columns

        return (
            f"train: {dimensions(self.train)}, test: {dimensions(self.test)}, validation: {dimensions(self.validation)}"
        )
