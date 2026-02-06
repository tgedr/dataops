"""S3 implementation of single partition parquet store."""
import logging
import s3fs
from typing import Any
import pandas as pd
import pyarrow as pa

from tgedr_dataops.store.fs_single_partition_parquet import FsSinglePartitionParquetStore
from tgedr_dataops.commons.utils_fs import remove_s3_protocol


logger = logging.getLogger(__name__)


class S3FsSinglePartitionParquetStore(FsSinglePartitionParquetStore):  # pragma: no cover
    """FsSinglePartitionParquetStore implementation using aws s3 file system."""

    CONFIG_KEY_AWS_ACCESS_KEY_ID: str = "aws_access_key_id"
    CONFIG_KEY_AWS_SECRET_ACCESS_KEY: str = "aws_secret_access_key"  # noqa: S105
    CONFIG_KEY_AWS_SESSION_TOKEN: str = "aws_session_token"  # noqa: S105

    @property
    def fs(self) -> Any:
        """Get the S3 filesystem instance.

        Returns
        -------
        Any
            The s3fs.S3FileSystem instance for S3 operations.
        """
        if self._fs is None:
            if (self._config is not None) and all(
                element in list(self._config.keys())
                for element in [
                    self.CONFIG_KEY_AWS_ACCESS_KEY_ID,
                    self.CONFIG_KEY_AWS_SECRET_ACCESS_KEY,
                    self.CONFIG_KEY_AWS_SESSION_TOKEN,
                ]
            ):
                self._fs = s3fs.S3FileSystem(
                    key=self._config[self.CONFIG_KEY_AWS_ACCESS_KEY_ID],
                    secret=self._config[self.CONFIG_KEY_AWS_SECRET_ACCESS_KEY],
                    token=self._config[self.CONFIG_KEY_AWS_SESSION_TOKEN],
                )
            else:
                self._fs = s3fs.S3FileSystem()
        return self._fs

    def _rmdir(self, key: str) -> None:
        """Remove a directory (prefix) from S3.

        Parameters
        ----------
        key : str
            S3 path/prefix to delete.
        """
        if self.fs.isdir(key):
            self.fs.delete(key, recursive=True)

    def _exists(self, key: str) -> bool:
        """Check if a path exists in S3.

        Parameters
        ----------
        key : str
            S3 path to check for existence.

        Returns
        -------
        bool
            True if path exists in S3, False otherwise.
        """
        return self.fs.get_file_info(key).type.name != "NotFound"

    def get(
        self,
        key: str,
        filter_func: callable | None = None,
        filters: list[tuple[str, str, list[str]]] | None = None,
        schema: pa.Schema = None,
    ) -> pd.DataFrame:
        """Retrieve data from S3 parquet store.

        Parameters
        ----------
        key : str
            S3 path to the parquet data (s3:// protocol will be removed).
        filter_func : callable, optional
            Row filter function.
        filters : list[tuple[str, str, list[str]]], optional
            Partition filters.
        schema : pa.Schema, optional
            PyArrow schema for reading.

        Returns
        -------
        pd.DataFrame
            The loaded DataFrame.
        """
        return super().get(key=remove_s3_protocol(key), filter_func=filter_func, filters=filters, schema=schema)

    def delete(
        self,
        key: str,
        partition_field: str | None = None,
        partition_values: list[str] | None = None,
        kv_dict: dict[str, list[Any]] | None = None,
        schema: pa.Schema = None,
    ) -> None:
        """Delete data from S3 parquet store.

        Parameters
        ----------
        key : str
            S3 path to the parquet data (s3:// protocol will be removed).
        partition_field : str, optional
            Field used for partitioning.
        partition_values : list[str], optional
            List of partition values to delete.
        kv_dict : dict[str, list[Any]], optional
            Dictionary of key-value filters for deletion.
        schema : pa.Schema, optional
            PyArrow schema.
        """
        super().delete(
            key=remove_s3_protocol(key),
            partition_field=partition_field,
            partition_values=partition_values,
            kv_dict=kv_dict,
            schema=schema,
        )

    def save(
        self,
        df: pd.DataFrame,
        key: str,
        partition_field: str | None = None,
        append: bool = False,
        replace_partitions: bool = False,
        schema: Any = None,
    ) -> None:
        """Save DataFrame to S3 parquet store.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to save.
        key : str
            S3 path to save the parquet data (s3:// protocol will be removed).
        partition_field : str, optional
            Field to partition by.
        append : bool, default False
            If True, append to existing data.
        replace_partitions : bool, default False
            If True, replace existing partitions.
        schema : Any, optional
            Schema for the data.
        """
        super().save(
            df=df,
            key=remove_s3_protocol(key),
            partition_field=partition_field,
            append=append,
            replace_partitions=replace_partitions,
            schema=schema,
        )

    def update(
        self,
        df: pd.DataFrame,
        key: str,
        key_fields: list[str],
        partition_field: str | None = None,
        schema: Any = None,
    ) -> None:
        """Update data in S3 parquet store.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame with updated data.
        key : str
            S3 path to the parquet data (s3:// protocol will be removed).
        key_fields : list[str]
            List of fields to use as keys for matching records.
        partition_field : str, optional
            Field used for partitioning.
        schema : Any, optional
            Schema for the data.
        """
        super().update(
            df=df, key=remove_s3_protocol(key), key_fields=key_fields, partition_field=partition_field, schema=schema
        )
