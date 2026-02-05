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
        if self._fs is None:
            self._fs = fs.LocalFileSystem()
        return self._fs

    def _rmdir(self, key: str) -> None:
        if self.fs.get_file_info(key).type.name == "Directory":
            self.fs.delete_dir(key)

    def _exists(self, key: str) -> bool:
        return self.fs.get_file_info(key).type.name != "NotFound"
