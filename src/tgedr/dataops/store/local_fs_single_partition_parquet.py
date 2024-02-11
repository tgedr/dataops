import logging
from pyarrow import fs

from tgedr.dataops.store.fs_single_partition_parquet import FsSinglePartitionParquetStore


logger = logging.getLogger(__name__)


class LocalFsSinglePartitionParquetStore(FsSinglePartitionParquetStore):
    @property
    def fs(self):
        if self._fs is None:
            self._fs = fs.LocalFileSystem()
        return self._fs

    def _rmdir(self, key):
        if self.fs.get_file_info(key).type.name == "Directory":
            self.fs.delete_dir(key)

    def _exists(self, key) -> bool:
        return self.fs.get_file_info(key).type.name != "NotFound"
