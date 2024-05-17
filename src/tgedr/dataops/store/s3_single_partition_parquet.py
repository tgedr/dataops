import logging
import s3fs
import logging
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow as pa

from tgedr.dataops.store.fs_single_partition_parquet import FsSinglePartitionParquetStore
from src.nn.gs.ss.dataops.commons.utils_fs import remove_s3_protocol


logger = logging.getLogger(__name__)


class S3FsSinglePartitionParquetStore(FsSinglePartitionParquetStore):  # pragma: no cover
    """FsSinglePartitionParquetStore implementation using aws s3 file system"""

    CONFIG_KEY_AWS_ACCESS_KEY_ID: str = "aws_access_key_id"
    CONFIG_KEY_AWS_SECRET_ACCESS_KEY: str = "aws_secret_access_key"
    CONFIG_KEY_AWS_SESSION_TOKEN: str = "aws_session_token"

    @property
    def fs(self):
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

    def _rmdir(self, key):
        if self.fs.isdir(key):
            self.fs.delete(key, recursive=True)

    def _exists(self, key) -> bool:
        return self.fs.get_file_info(key).type.name != "NotFound"

    def get(
        self,
        key: str,
        filter: callable = None,
        filters: List[tuple[str, str, List[str]]] = None,
        schema: pa.Schema = None,
    ) -> pd.DataFrame:
        return super().get(key=remove_s3_protocol(key), filter=filter, filters=filters, schema=schema)

    def delete(
        self,
        key: str,
        partition_field: Optional[str] = None,
        partition_values: Optional[List[str]] = None,
        kv_dict: Optional[Dict[str, List[Any]]] = None,
        schema: pa.Schema = None,
    ):
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
        partition_field: Optional[str] = None,
        append: bool = False,
        replace_partitions: bool = False,
        schema: Any = None,
    ):
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
        key_fields: List[str],
        partition_field: Optional[str] = None,
        schema: Any = None,
    ):
        super().update(
            df=df, key=remove_s3_protocol(key), key_fields=key_fields, partition_field=partition_field, schema=schema
        )
