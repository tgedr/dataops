from abc import ABC, abstractmethod
import logging
import os
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

from tgedr.dataops.store.store import Store, StoreException


logger = logging.getLogger(__name__)


class FsSinglePartitionParquetStore(Store, ABC):
    @property
    @abstractmethod
    def fs(self):
        raise NotImplementedError()

    @abstractmethod
    def _rmdir(self, key):
        raise NotImplementedError()

    @abstractmethod
    def _exists(self, key) -> bool:
        raise NotImplementedError()

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        Store.__init__(self, config)
        self._fs = None

    def get(self, key: str, use_legacy_dataset: bool = False, filter: callable = None) -> pd.DataFrame:
        logger.info(f"[get|in] ({key}, {use_legacy_dataset})")
        table = pq.read_table(key, filesystem=self.fs)
        if filter is not None:
            table = table.filter(filter)
        result = table.to_pandas()
        logger.info(f"[get|out] => {result}")
        return result

    def delete(
        self,
        key: str,
        partition_field: Optional[str] = None,
        partitions: Optional[List[str]] = None,
        kv_dict: Optional[Dict[str, List[Any]]] = None,
    ):
        logger.info(f"[delete|in] ({key}, {partition_field}, {partitions}, {kv_dict})")

        if partitions is not None and partition_field is not None:
            self._remove_partitions(key, partition_field=partition_field, partition_values=partitions)
        elif kv_dict is not None and partition_field is not None:
            table = pq.read_table(key, filesystem=self.fs)
            for k, v in kv_dict.items():
                filter_condition = ~pc.is_in(pc.field(k), pa.array(v))
                table = table.filter(filter_condition)
            self.delete(key)
            pq.write_to_dataset(
                table,
                root_path=key,
                partition_cols=[partition_field],
                existing_data_behavior="delete_matching",
                filesystem=self.fs,
            )
        else:
            self._rmdir(key)

        logger.info("[delete|out]")

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

    def _remove_partitions(self, key: str, partition_field: str, partition_values: List[str]):
        logger.debug(f"[_remove_partitions|in] ({key}, {partition_field}, {partition_values})")

        for partition_value in partition_values:
            partition_key = f"{partition_field}={partition_value}"
            partition_path = os.path.join(key, partition_key)
            self._rmdir(partition_path)

        logger.debug("[_remove_partitions|out]")

    def update(
        self,
        df: pd.DataFrame,
        key: str,
        key_fields: List[str],
        partition_field: Optional[str] = None,
    ):
        logger.info(f"[save|in] ({df}, {key}, {key_fields}, {partition_field})")

        df0 = self.get(key)
        match = pd.merge(df0.reset_index(), df.reset_index(), on=key_fields)
        index_left = match["index_x"]
        index_right = match["index_y"]
        df0.iloc[index_left] = df.iloc[index_right]

        self.save(df0, key, partition_field=partition_field)

        logger.info(f"[save|out]")
