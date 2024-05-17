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


def pandas_mapper(arrow_type):
    if pa.types.is_int64(arrow_type):
        return pd.Int64Dtype()
    if pa.types.is_float64(arrow_type):
        return pd.Float64Dtype()
    if pa.types.is_string(arrow_type):
        return pd.StringDtype()
    # suggest default behavior
    return None


class FsSinglePartitionParquetStore(Store, ABC):
    """abstract store implementation defining persistence on parquet files with an optional single partition,
    regardless of the location it should persist"""

    @property
    @abstractmethod
    def fs(self):
        """abstract method providing a filesystem implementation (local, s3, etc...)"""
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

    def get(
        self,
        key: str,
        filter: callable = None,
        filters: List[tuple[str, str, List[str]]] = None,
        schema: pa.Schema = None,
    ) -> pd.DataFrame:
        """
        reads a pandas dataframe from somewhere (key), depending on implementation, eventually enforcing a schema and
        allowing filtering of data

        Parameters:
        key (str): location/url/path where data should be persisted
        filter (Array or array-like or Expression): filter expression (see: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.filter)
        filters (pyarrow.compute.Expression or List[Tuple] or List[List[Tuple]]): filter expression (see: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_table.html)
        schema: data schema to enforce while reading (see: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html#pyarrow.Schema)

        Returns:
        pandas.DataFrame: the dataframe
        """
        schema_msg_segment = "0" if schema is None else str(len(schema))
        logger.info(f"[get|in] ({key}, {filter}, {filters}, schema len:{schema_msg_segment})")
        logger.debug(f"[get|in] ({key}, {filter}, {filters}, {schema})")
        table = pq.read_table(key, filesystem=self.fs, filters=filters, schema=schema)
        if filter is not None:
            table = table.filter(filter)
        result = table.to_pandas(types_mapper=pandas_mapper)
        logger.info(f"[get|out] => {result.shape}")
        return result

    def delete(
        self,
        key: str,
        partition_field: Optional[str] = None,
        partition_values: Optional[List[str]] = None,
        kv_dict: Optional[Dict[str, List[Any]]] = None,
        schema: pa.Schema = None,
    ):
        """
        removes partitions, full or partial, or deletes partial values or a full dataset
        from a parquet storage somewhere (key), depending on implementation

        Parameters:
        key (str): location/url/path where data is persisted
        partition_field (str): name of the partition field in the dataset
        partition_values (str): partition values to delete
        kv_dict (Dict[str, List[Any]]): key-value map defining the fields and array of values to become the deletion filter
        schema: data schema to enforce if reading is required (see: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html#pyarrow.Schema)
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
        partition_field: Optional[str] = None,
        append: bool = False,
        replace_partitions: bool = False,
        schema: Any = None,
    ):
        """
        saves a pandas dataframe in parquet format somewhere (key), depending on implementation

        Parameters:
        df (pandas.DataFrame): the dataframe to be saved
        key (str): location/url/path where data is persisted
        partition_field (str): name of the partition field in the dataset
        append (bool): if data should be appended, otherwise will overwrite
        replace_partitions (bool): if partitions should be replaced, this will delete the data existent on those partitions completely
        schema: data schema to enforce if reading is required (see: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html#pyarrow.Schema)
        """
        schema_msg_segment = "0" if schema is None else str(len(schema))
        logger.info(
            f"[save|in] ({df.shape}, {key}, {partition_field}, {append}, {replace_partitions}, schema len:{schema_msg_segment})"
        )
        logger.debug(f"[save|in] ({df}, {key}, {partition_field}, {append}, {replace_partitions}, {schema})")

        if schema is not None and isinstance(schema, pa.lib.Schema):
            # we will order the columns based on the schema
            columns = [col for col in schema.names]
            df = df[columns]

        if replace_partitions and append:
            raise StoreException(f"cannot request for replace_partitions and append at the same time")

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
        schema: Any = None,
    ):
        """
        updates a pandas dataframe in parquet format somewhere (key), depending on implementation

        Parameters:
        df (pandas.DataFrame): the dataframe to be saved
        key (str): location/url/path where data is persisted
        key_fields (List[str]): primary fields of the dataset used to match the rows to update with the new dataset
        partition_field (str): name of the partition field to enforce while saving
        schema: data schema to enforce while reading and saving (see: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html#pyarrow.Schema)
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

        logger.info(f"[update|out]")
