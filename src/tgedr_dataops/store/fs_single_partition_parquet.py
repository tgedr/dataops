"""Filesystem-based single partition Parquet store implementation.

This module provides an abstract base class for storing and retrieving data
in Parquet format with optional single partition support across different
filesystem implementations (local, S3, etc.).
"""
from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from tgedr_dataops_abs.store import Store, StoreException


logger = logging.getLogger(__name__)


def pandas_mapper(arrow_type: pa.DataType) -> pd.api.extensions.ExtensionDtype | None:
    """Map PyArrow types to pandas nullable types.

    Parameters
    ----------
    arrow_type : pa.DataType
        PyArrow data type to map.

    Returns
    -------
    pd.api.extensions.ExtensionDtype or None
        Corresponding pandas nullable dtype, or None for default behavior.
    """
    if pa.types.is_int64(arrow_type):
        return pd.Int64Dtype()
    if pa.types.is_float64(arrow_type):
        return pd.Float64Dtype()
    if pa.types.is_string(arrow_type):
        return pd.StringDtype()   # pragma: no cover
    # suggest default behavior
    return None


class FsSinglePartitionParquetStore(Store, ABC):
    """Abstract store implementation for Parquet files with optional single partition.

    This class provides persistence on Parquet files with an optional single partition,
    regardless of the underlying filesystem location (local, S3, etc.).
    """

    @property
    @abstractmethod
    def fs(self) -> Any:
        """Abstract property providing a filesystem implementation.

        Returns
        -------
        Any
            Filesystem implementation (e.g., LocalFileSystem, S3FileSystem).
        """
        raise NotImplementedError

    @abstractmethod
    def _rmdir(self, key: str) -> None:
        """Remove a directory.

        Parameters
        ----------
        key : str
            Directory path to remove.
        """
        raise NotImplementedError

    @abstractmethod
    def _exists(self, key: str) -> bool:
        """Check if a path exists.

        Parameters
        ----------
        key : str
            Path to check for existence.

        Returns
        -------
        bool
            True if path exists, False otherwise.
        """
        raise NotImplementedError

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the FsSinglePartitionParquetStore.

        Parameters
        ----------
        config : dict[str, Any], optional
            Configuration dictionary.
        """
        Store.__init__(self, config)
        self._fs = None

    def get(
        self,
        key: str,
        filter_func: Any | None = None,
        filters: list[tuple[str, str, list[str]]] | None = None,
        schema: pa.Schema | None = None,
    ) -> pd.DataFrame:
        """Read a pandas DataFrame from Parquet storage.

        Reads data from the specified location, optionally enforcing a schema
        and allowing filtering of data.

        Parameters
        ----------
        key : str
            Location/URL/path where data is persisted.
        filter_func : callable, optional
            Filter expression (see PyArrow Table.filter documentation).
        filters : list[tuple[str, str, list[str]]], optional
            Filter expression for read_table (see PyArrow parquet.read_table documentation).
        schema : pa.Schema, optional
            Data schema to enforce while reading.

        Returns
        -------
        pd.DataFrame
            The loaded DataFrame.
        """
        schema_msg_segment = "0" if schema is None else str(len(schema))
        logger.info(f"[get|in] ({key}, {filter_func}, {filters}, schema len:{schema_msg_segment})")
        logger.debug(f"[get|in] ({key}, {filter_func}, {filters}, {schema})")
        table = pq.read_table(key, filesystem=self.fs, filters=filters, schema=schema)
        if filter_func is not None:
            table = table.filter(filter_func)
        result = table.to_pandas(types_mapper=pandas_mapper)
        logger.info(f"[get|out] => {result.shape}")
        return result

    def delete(
        self,
        key: str,
        partition_field: str | None = None,
        partition_values: list[str] | None = None,
        kv_dict: dict[str, list[Any]] | None = None,
        schema: pa.Schema = None,
    ) -> None:
        """Delete partitions or data from Parquet storage.

        Removes partitions (full or partial), deletes specific values, or removes
        an entire dataset from Parquet storage.

        Parameters
        ----------
        key : str
            Location/URL/path where data is persisted.
        partition_field : str, optional
            Name of the partition field in the dataset.
        partition_values : list[str], optional
            Partition values to delete.
        kv_dict : dict[str, list[Any]], optional
            Key-value map defining fields and array of values for deletion filter.
        schema : pa.Schema, optional
            Data schema to enforce if reading is required.
        """
        schema_msg_segment = "0" if schema is None else str(len(schema))
        logger.info(
            f"[delete|in] ({key}, {partition_field}, {partition_values}, {kv_dict}, schema len:{schema_msg_segment})"
        )
        logger.debug(f"[delete|in] ({key}, {partition_field}, {partition_values}, {kv_dict}, {schema})")

        if partition_values is not None and partition_field is not None:
            self._remove_partitions(key, partition_field=partition_field, partition_values=partition_values)
        elif kv_dict is not None and partition_field is not None:
            table = pq.read_table(key, filesystem=self.fs, schema=schema)
            for k, v in kv_dict.items():
                filter_condition = ~pc.is_in(pc.field(k), pa.array(v))
                table = table.filter(filter_condition)
            self.delete(key, schema=schema)
            pq.write_to_dataset(
                table,
                root_path=key,
                partition_cols=[partition_field],
                existing_data_behavior="delete_matching",
                filesystem=self.fs,
                schema=schema,
            )
        else:
            self._rmdir(key)

        logger.info("[delete|out]")

    def save(
        self,
        df: pd.DataFrame,
        key: str,
        partition_field: str | None = None,
        append: bool = False,
        replace_partitions: bool = False,
        schema: Any = None,
    ) -> None:
        """Save a pandas DataFrame in Parquet format.

        Saves data to the specified location with optional partitioning,
        append, or replace behavior.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to be saved.
        key : str
            Location/URL/path where data should be persisted.
        partition_field : str, optional
            Name of the partition field in the dataset.
        append : bool, default False
            If True, data will be appended; otherwise will overwrite.
        replace_partitions : bool, default False
            If True, partitions will be replaced, deleting existing data in those partitions.
        schema : pa.Schema, optional
            Data schema to enforce while writing.

        Raises
        ------
        StoreException
            If both append and replace_partitions are True.
        """
        schema_msg_segment = "0" if schema is None else str(len(schema))
        logger.info(
            f"[save|in] ({df.shape}, {key}, {partition_field}, {append}, {replace_partitions}, schema len:{schema_msg_segment})"
        )
        logger.debug(f"[save|in] ({df}, {key}, {partition_field}, {append}, {replace_partitions}, {schema})")

        if schema is not None and isinstance(schema, pa.lib.Schema):
            # we will order the columns based on the schema
            columns = list(schema.names)
            df = df[columns]

        if replace_partitions and append:
            raise StoreException("cannot request for replace_partitions and append at the same time")

        if append:
            pq.write_to_dataset(
                pa.Table.from_pandas(df, preserve_index=False),
                root_path=key,
                partition_cols=[partition_field],
                filesystem=self.fs,
                schema=schema,
            )
        elif replace_partitions:
            partitions = df[partition_field].unique().tolist()
            self._remove_partitions(key, partition_field, partitions)
            pq.write_to_dataset(
                pa.Table.from_pandas(df, preserve_index=False),
                root_path=key,
                partition_cols=[partition_field],
                existing_data_behavior="delete_matching",
                filesystem=self.fs,
                schema=schema,
            )
        else:
            self.delete(key)
            pq.write_to_dataset(
                pa.Table.from_pandas(df, preserve_index=False),
                root_path=key,
                partition_cols=[partition_field],
                existing_data_behavior="delete_matching",
                filesystem=self.fs,
                schema=schema,
            )
        logger.info("[save|out]")

    def _remove_partitions(self, key: str, partition_field: str, partition_values: list[str]) -> None:
        """Remove specific partitions from the dataset.

        Parameters
        ----------
        key : str
            Root path of the dataset.
        partition_field : str
            Name of the partition field.
        partition_values : list[str]
            List of partition values to remove.
        """
        logger.debug(f"[_remove_partitions|in] ({key}, {partition_field}, {partition_values})")

        for partition_value in partition_values:
            partition_key = f"{partition_field}={partition_value}"
            partition_path = str(Path(key) / partition_key)
            self._rmdir(partition_path)

        logger.debug("[_remove_partitions|out]")

    def update(
        self,
        df: pd.DataFrame,
        key: str,
        key_fields: list[str],
        partition_field: str | None = None,
        schema: Any = None,
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
        partition_field : str, optional
            Name of the partition field to enforce while saving.
        schema : pa.Schema, optional
            Data schema to enforce while reading and saving.
        """
        schema_msg_segment = "0" if schema is None else str(len(schema))
        logger.info(
            f"[update|in] ({df.shape}, {key}, {key_fields}, {partition_field}, schema len:{schema_msg_segment})"
        )
        logger.debug(f"[update|in] ({df}, {key}, {key_fields}, {partition_field}, {schema})")

        df0 = self.get(key, schema=schema)
        match = pd.merge(df0.reset_index(), df.reset_index(), on=key_fields)
        index_left = match["index_x"]
        index_right = match["index_y"]
        df0.iloc[index_left] = df.iloc[index_right]
        self.save(df0, key, partition_field=partition_field, schema=schema)

        logger.info("[update|out]")
