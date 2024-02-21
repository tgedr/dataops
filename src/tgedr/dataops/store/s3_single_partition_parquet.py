import logging
from typing import Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from tgedr.dataops.store.store import StoreException
from tgedr.dataops.store.fs_single_partition_parquet import FsSinglePartitionParquetStore


logger = logging.getLogger(__name__)


class S3FsSinglePartitionParquetStore(FsSinglePartitionParquetStore):
    CONFIG_KEY_AWS_ACCESS_KEY_ID: str = "aws_access_key_id"
    CONFIG_KEY_AWS_SECRET_ACCESS_KEY: str = "aws_secret_access_key"
    CONFIG_KEY_AWS_SESSION_TOKEN: str = "aws_session_token"

    @property
    def fs(self):
        if self._fs is None:
            if all(
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

    def _rmdir(self, key):
        # if self.fs.get_file_info(key).type.name == 'Directory':
        if self.fs.isdir(key):
            self.fs.delete(key, recursive=True)

    def _exists(self, key) -> bool:
        return self.fs.get_file_info(key).type.name != "NotFound"

    def get(self, key: str, use_legacy_dataset: bool = False) -> pd.DataFrame:
        logger.info(f"[get|in] ({key}, {use_legacy_dataset})")
        result = pq.read_table(key, filesystem=s3fs.S3FileSystem()).to_pandas()
        logger.info(f"[get|out] => {result}")
        return result

    def save(
        self,
        df: pd.DataFrame,
        key: str,
        partition_field: Optional[str] = None,
        append: bool = False,
        replace_partitions: bool = False,
    ):
        logger.info(f"[save|in] ({df}, {key}, {partition_field}, {append}, {replace_partitions})")
        if replace_partitions and append:
            raise StoreException(f"cannot request for replace_partitions and append at the same time")

        if append:
            pq.write_to_dataset(
                pa.Table.from_pandas(df), root_path=key, partition_cols=[partition_field], filesystem=self.fs
            )
        elif replace_partitions:
            partitions = df[partition_field].unique().tolist()
            self._remove_partitions(key, partition_field, partitions)
            pq.write_to_dataset(
                pa.Table.from_pandas(df),
                root_path=key,
                partition_cols=[partition_field],
                existing_data_behavior="delete_matching",
                filesystem=self.fs,
            )
        else:
            self.delete(key)
            pq.write_to_dataset(
                pa.Table.from_pandas(df),
                root_path=key,
                partition_cols=[partition_field],
                existing_data_behavior="delete_matching",
                filesystem=self.fs,
            )
        logger.info("[save|out]")
