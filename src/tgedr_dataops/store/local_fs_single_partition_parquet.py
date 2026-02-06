"""Local filesystem implementation for single partition Parquet storage.

This module provides LocalFsSinglePartitionParquetStore, which implements
single partition Parquet storage using the local file system.
"""
import logging
from typing import Any
from pyarrow import fs

from tgedr_dataops.store.fs_single_partition_parquet import FsSinglePartitionParquetStore


logger = logging.getLogger(__name__)


class LocalFsSinglePartitionParquetStore(FsSinglePartitionParquetStore):
    """FsSinglePartitionParquetStore implementation using local file system."""

    @property
    def fs(self) -> Any:
        """Get the PyArrow local filesystem instance.

        Returns
        -------
        Any
            The filesystem instance for local file operations.
        """
        if self._fs is None:
            self._fs = fs.LocalFileSystem()
        return self._fs

    def _rmdir(self, key: str) -> None:
        """Remove a directory recursively.

        Parameters
        ----------
        key : str
            Path to the directory to delete.
        """
        if self.fs.get_file_info(key).type.name == "Directory":
            self.fs.delete_dir(key)

    def _exists(self, key: str) -> bool:
        """Check if a path exists on the local filesystem.

        Parameters
        ----------
        key : str
            Path to check for existence.

        Returns
        -------
        bool
            True if path exists, False otherwise.
        """
        return self.fs.get_file_info(key).type.name != "NotFound"
