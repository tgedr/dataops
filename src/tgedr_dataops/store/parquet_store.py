"""Provides the ParquetStore class for interacting with Parquet files using a filesystem interface."""

import logging
from abc import ABC
from typing import Any
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs
from pyarrow.fs import FileInfo, FileType

from tgedr_dataops_abs.store import Store


logger = logging.getLogger(__name__)


class ParquetStore(Store, ABC):
    """ParquetStore provides methods to interact with Parquet files using a filesystem interface."""

    __PARQUET_ENGINE = "pyarrow"

    def _ensure_key_parent(self, key: str) -> None:
        """Ensure that the directory(parent) for the given key exists.

        Args:
            key (str): The file path or identifier for the Parquet file.

        """
        logger.info(f"[_ensure_key_parent|in] ({key})")

        parent = str(Path(key).parent)
        info: FileInfo = self._fs.get_file_info(parent)
        if info.type != FileType.Directory:
            self._fs.create_dir(parent, recursive=True)
            logger.info(f"[_ensure_key_parent] created key parent: {parent}")
        else:
            logger.info(f"[_ensure_key_parent] key parent already exists: {parent}")  # pragma: no cover
        logger.info("[_ensure_key_parent|out]")

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the ParquetStore with an optional configuration dictionary.

        Args:
            config (dict[str, int | str | float] | None): Optional configuration for the store.

        """
        self._config = config
        self._fs = fs.LocalFileSystem()

    def get(self, key: str, cols: list[str] | None = None) -> pd.DataFrame:
        """Retrieve a DataFrame from a Parquet file specified by the given key.

        Args:
            key (str): The file path or identifier for the Parquet file.
            cols (list or None): Optional list of column names to read from the Parquet file.

        Returns:
            pd.DataFrame: The loaded DataFrame from the Parquet file.

        """
        logger.info(f"[get|in] ({key}, {cols})")
        result = pd.read_parquet(key, engine=self.__PARQUET_ENGINE, columns=cols)
        logger.info(f"[get|out] => {result}")
        return result

    def delete(self, key: str) -> None:
        """Delete a Parquet file or directory specified by the given key.

        Args:
            key (str): The file path or identifier for the Parquet file or directory.

        """
        logger.info(f"[delete|in] ({key})")
        info = self._fs.get_file_info(key).type.name
        if info != "NotFound":
            if self._fs.get_file_info(key).type.name == "Directory":
                self._fs.delete_dir(key)
            else:
                self._fs.delete_file(key)
        logger.info("[delete|out]")

    def save(self, df: pd.DataFrame, key: str, partition_fields: list[str] | None = None, append: bool = False) -> None:
        """Save a DataFrame to a Parquet file, optionally partitioned by specified fields.

        Uses PyArrow's native dataset API for efficient appending without loading entire dataset into memory.
        For partitioned datasets, new data is written as additional files in the partition structure.

        Args:
            df (pd.DataFrame): The DataFrame to save.
            key (str): The file path or identifier for the Parquet file or dataset directory.
            partition_fields (list[str] | None): Optional list of fields to partition by.
                When specified, creates a partitioned dataset structure.
            append (bool): If True, append to existing dataset. If False, replace. Default is False.

        Note:
            - Append mode for single files will read existing data into memory before writing
            - Append mode for partitioned datasets writes new partition files without reading existing data
            - For large datasets, use partition_fields to enable efficient appending

        """
        logger.info(f"[save|in] shape={df.shape}, key={key}, partition_fields={partition_fields}, append={append}")

        info: FileInfo = self._fs.get_file_info(key)
        key_exists: bool = info.type != FileType.NotFound

        # Convert DataFrame to PyArrow Table
        table = pa.Table.from_pandas(df)

        if partition_fields:
            if not append and key_exists:
                logger.info(f"[save] overwriting existing partitioned dataset at {key}")
                self.delete(key)
            pq.write_to_dataset(
                table,
                root_path=key,
                partition_cols=partition_fields,
                filesystem=self._fs,
                existing_data_behavior="overwrite_or_ignore" if append else "error",
            )
            logger.info(f"[save] saved/appended partitioned dataset to {key}")
        else:
            if append and key_exists:
                existing_table = pq.read_table(key, filesystem=self._fs)
                table = pa.concat_tables([existing_table, table])
            elif not append and key_exists:
                logger.info(f"[save] overwriting existing file at {key}")
                self.delete(key)

            pq.write_table(table, key, filesystem=self._fs)
            logger.info(f"[save] saved/appended dataset to {key}")

        logger.info("[save|out]")

    def update(
        self,
        df: pd.DataFrame,
        key: str,
        key_fields: list[str],
        partition_fields: list[str] | None = None
    ) -> None:
        """Update rows in a pandas DataFrame stored in Parquet format.

        Updates matching rows based on key fields by merging with existing data.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame containing updates.
        key : str
            Location/URL/path where data is persisted.
        key_fields : list[str]
            Primary fields used to match rows for updating.
        partition_fields : list[str] | None, optional
            Optional list of fields to partition by when saving.

        """
        logger.info(
            f"[update|in] ({df.shape}, {key}, {key_fields}, {partition_fields})"
        )
        logger.debug(f"[update|in] ({df}, {key}, {key_fields}, {partition_fields})")

        try:
            df0 = self.get(key) # circumvent the case where the file doesn't exist yet
        except FileNotFoundError:
            df0 = pd.DataFrame()

        if df0.empty:
            logger.info(f"[update] no existing data at {key}, saving new data")
            self.save(df, key, partition_fields=partition_fields)
        else:
            match = pd.merge(df0.reset_index(), df.reset_index(), on=key_fields)
            if match.empty:
                logger.info(f"[update] no matching rows found for key fields {key_fields}, appending new data")
                self.save(df, key, partition_fields=partition_fields, append=True)
            else:
                index_left = match["index_x"]
                index_right = match["index_y"]
                # update matching rows
                df0.iloc[index_left] = df.iloc[index_right]
                if len(match) < len(df):
                    logger.info(
                        f"[update] some rows not matched for key fields {key_fields}, appending unmatched rows"
                    )
                    df_unmatched = df.iloc[~index_right]
                    df0 = pd.concat([df0, df_unmatched], ignore_index=True)
                self.save(df0, key, partition_fields=partition_fields)

        logger.info("[update|out]")
