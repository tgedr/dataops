from abc import ABC
import dataclasses
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import monotonically_increasing_id
from tgedr.dataops.store.store import NoStoreException, Store, StoreException
from tgedr.dataops.commons.metadata import Metadata
from tgedr.dataops.commons.utils_spark import UtilsSpark

logger = logging.getLogger(__name__)


class SparkDeltaStore(Store, ABC):
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        Store.__init__(self, config)

    def get(self, key: str, version: str = None, **kwargs) -> DataFrame:
        logger.info(f"[get|in] ({key}, {version})")

        table = self._get_table(path=key)
        if table is None:
            raise NoStoreException(f"[get] couldn't find data in key: {key}")

        reader = UtilsSpark.get_spark_session().read.format("delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)

        result = reader.load(key)

        logger.info("[get_df|out]")
        return result

    def __get_deletion_criteria(self, df):
        logger.debug("[__get_deletion_criteria|in])")
        fields = df.dtypes
        numerics = [
            x
            for x in fields
            if x[1] in ["bigint", "int", "double", "float", "long", "decimal.Decimal"] or (x[1][:7]) == "decimal"
        ]
        dates = [x for x in fields if (x[1]) in ["datetime", "datetime.datetime"]]
        textuals = [x for x in fields if x[1] in ["string"]]
        if 0 < len(numerics):
            column = numerics[0][0]
            result = (F.col(column) > 0) | (F.col(column) <= 0)
        elif 0 < len(dates):
            column = dates[0][0]
            now = datetime.now()
            result = (F.col(column) > now) | (F.col(column) <= now)
        elif 0 < len(textuals):
            column = textuals[0][0]
            result = (F.col(column) > "a") | (F.col(column) <= "a")
        else:
            raise StoreException(
                f"[__get_deletion_criteria] failed to figure out column types handy to create a full deletion criteria"
            )

        logger.debug(f"[__get_deletion_criteria|out] = {result}")
        return result

    def delete(self, key: str, condition: Union[F.Column, str, None] = None, **kwargs) -> None:
        logger.info(f"[delete|in] ({key}, {condition})")

        spark = UtilsSpark.get_spark_session()
        """
        is_s3_operation = True if key.startswith("s3") else False
        if is_s3_operation:
        """
        delta_table = DeltaTable.forPath(spark, key)
        if condition is None:
            condition = self.__get_deletion_criteria(delta_table.toDF())
        delta_table.delete(condition=condition)
        """
        else:   # local development mostly for temporary or test purposes
            spark_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            # get spark context path
            spark_path = spark._jvm.org.apache.hadoop.fs.Path(key)
            logger.info(f"[delete] spark path is {spark_path}")
            try:
                if spark_fs.exists(spark_path):
                    spark_fs.delete(spark_path, True)
            except AnalysisException as x:
                raise StoreException(f"[delete] couldn't do it on key {key}: {x}")
        """
        logger.info("[delete|out]")

    def save(
        self,
        df: DataFrame,
        key: str,
        append: bool = False,
        partition_fields: Optional[List[str]] = None,
        metadata: Optional[Metadata] = None,
        retention_days: int = 7,
        deleted_retention_days: int = 7,
        column_descriptions: Optional[Dict[str, str]] = None,
        table_name: Optional[str] = None,
        **kwargs,
    ):
        logger.info(
            f"[save|in] ({df}, {key}, {append}, {partition_fields}, {metadata}, {retention_days}, {deleted_retention_days}, {column_descriptions}, {table_name}, {kwargs})"
        )

        if column_descriptions is not None:
            df = self._set_column_descriptions(df, column_descriptions)

        if append:
            writer = df.write.format("delta").mode("append")
        else:
            writer = df.write.format("delta").mode("overwrite")

        if partition_fields is not None:
            table = self._get_table(path=key)
            if table is not None:
                self._set_table_partitions(path=key, partition_fields=partition_fields)
            writer = writer.partitionBy(*partition_fields)

        if self._has_schema_changed(path=key, df=df):
            writer = writer.option("overwriteSchema", "true")

        if metadata:
            writer = writer.option("userMetadata", metadata)

        if table_name is not None:
            # assume we have db.table
            db = table_name.split(".")[0]
            UtilsSpark.get_spark_session().sql(f"CREATE DATABASE IF NOT EXISTS {db}")
            writer = writer.option("path", key).saveAsTable(table_name)
        else:
            writer.save(key)

        logger.info(f"[save] optimizing...")
        table = self._get_table(path=key)

        if retention_days is not None and deleted_retention_days is not None:
            self.enforce_retention_policy(
                path=key, retention_days=retention_days, deleted_retention_days=deleted_retention_days
            )
        elif retention_days is not None:
            self.enforce_retention_policy(path=key, retention_days=retention_days)

        table.optimize().executeCompaction()

        logger.info(f"[save|out]")

    def update(
        self,
        df: Any,
        key: str,
        match_fields: List[str],
        partition_fields: Optional[List[str]] = None,
        metadata: Optional[Metadata] = None,
        retention_days: int = 7,
        deleted_retention_days: int = 7,
        **kwargs,
    ):
        logger.info(
            f"[update|in] ({df}, {key}, {match_fields}, {partition_fields}, {metadata}, {retention_days}, {deleted_retention_days}, {kwargs})"
        )

        table = self._get_table(path=key)
        if table is None:
            self.save(
                df=df,
                key=key,
                partition_fields=partition_fields,
                metadata=metadata,
                retention_days=retention_days,
                deleted_retention_days=deleted_retention_days,
                **kwargs,
            )
        else:
            if partition_fields is not None:
                self._set_table_partitions(path=key, partition_fields=partition_fields)

            match_clause = None
            for field in match_fields:
                match_clause = (
                    f"current.{field} = updates.{field}"
                    if match_clause is None
                    else f"{match_clause} and current.{field} = updates.{field}"
                )
            logger.info(f"[update] match clause: {match_clause}")

            # check if the df has all the required columns
            # as we are upserting the updated columns coming in must at least match or exceed the current columns
            for column in table.toDF().columns:
                # we'll assume missing columns are nullable, typically metrics
                if column not in df.columns:
                    df = df.withColumn(column, F.lit(None).cast(T.StringType()))

            table.alias("current").merge(
                df.alias("updates"), match_clause
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            if retention_days is not None and deleted_retention_days is not None:
                self.enforce_retention_policy(
                    path=key, retention_days=retention_days, deleted_retention_days=deleted_retention_days
                )
            elif retention_days is not None:
                self.enforce_retention_policy(path=key, retention_days=retention_days)

            table.optimize().executeCompaction()

        logger.info("[UtilsDeltaTable.upsert|out]")

    def enforce_retention_policy(self, path: str, retention_days: int = 7, deleted_retention_days: int = 7):
        logger.info(f"[enforce_retention_policy|in] ({path}, {retention_days}, {deleted_retention_days})")

        retention = f"interval {retention_days} days"
        deleted_retention = f"interval {deleted_retention_days} days"

        UtilsSpark.get_spark_session().sql(
            f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES('delta.logRetentionDuration' = '{retention}', 'delta.deletedFileRetentionDuration' = '{deleted_retention}')"
        )
        logger.info("[enforce_retention_policy|out]")

    def get_latest_table_versions(self, path: str, how_many: int = 1) -> List[str]:
        """
        checks the delta table history and retrieves the latest n versions
        sorted from the newest to the oldest
        """
        logger.info(f"[get_latest_table_versions|in] ({path}, {how_many})")
        result: List[str] = []

        table = self._get_table(path=path)
        if table is not None:
            history_rows = table.history().orderBy(F.desc("timestamp")).limit(how_many)
            result = [str(x.version) for x in history_rows.collect()]

        logger.info(f"[get_latest_table_versions|out] => {result}")
        return result

    def get_metadata(self, path: str, version: str = None) -> Optional[Metadata]:
        """
        Raises
        ------
        NoStoreException
        """
        logger.info(f"[get_metadata|in] ({path}, {version})")
        table = self._get_table(path)
        if table is None:
            raise NoStoreException(f"[get_metadata] no data in path: {path}")

        result = None

        df_history = table.history().filter(F.col("userMetadata").isNotNull())
        if version is not None:
            df_history = df_history.filter(F.col("version") <= int(version))

        df_history = df_history.orderBy(F.col("version").desc())
        if not df_history.isEmpty():
            userMetadata = df_history.take(1)[0].userMetadata
            result = Metadata.from_str(userMetadata)
            if version is not None:
                result = dataclasses.replace(result, version=version)

        logger.info(f"[get_metadata|out] => ({result})")
        return result

    def _get_delta_log(self, path: str) -> DataFrame:
        logger.info(f"[_get_delta_log|in] ({path})")

        spark = UtilsSpark.get_spark_session()
        jdf = (
            spark._jvm.org.apache.spark.sql.delta.DeltaLog.forTable(spark._jsparkSession, path)
            .snapshot()
            .allFiles()
            .toDF()
        )
        result = DataFrame(jdf, spark)

        logger.info(f"[_get_delta_log|out] => {result}")
        return result

    def _get_table_partitions(self, path: str) -> List[str]:
        logger.info(f"[_get_table_partitions|in] ({path})")
        result: List[str] = []

        delta_log: DataFrame = self._get_delta_log(path=path)
        partition_keys = [
            x.keys
            for x in delta_log.select(F.map_keys(F.col("partitionValues")).alias("keys")).distinct().collect()
            if 0 < len(x)
        ]
        if 0 < len(partition_keys):
            result: List[str] = list({y for y in partition_keys for y in y})

        logger.info(f"[_get_table_partitions|out] => {result}")
        return result

    def _vacuum_now(self, path: str):
        logger.info("[_vacuum_now|in]")

        spark = UtilsSpark.get_spark_session()
        old_conf_value = spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled")
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        DeltaTable.forPath(spark, path).vacuum(0)
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", old_conf_value)

        logger.info("[_vacuum_now|out]")

    def _has_schema_changed(self, path: str, df: DataFrame) -> bool:
        logger.info(f"[_has_schema_changed|in] ({path},{df})")
        result: bool = False
        table = self._get_table(path=path)
        if table is not None:
            result = table.toDF().schema != df.schema
        logger.info(f"[_has_schema_changed|out] => {result}")
        return result

    def _set_table_partitions(self, path: str, partition_fields: List[str]) -> None:
        logger.info(f"[_set_table_partitions|in] ({path},{partition_fields})")

        spark = UtilsSpark.get_spark_session()
        # let's check partition_cols
        current_partition_fields = self._get_table_partitions(path=path)
        shall_we_repartition = sorted(partition_fields) != sorted(current_partition_fields)

        if shall_we_repartition:
            logger.info("[_set_table_partitions] going to repartition")
            new_df = spark.read.format("delta").load(path)
            new_df.write.format("delta").mode("overwrite").partitionBy(*partition_fields).option(
                "overwriteSchema", "true"
            ).save(path)
            self._vacuum_now(path)
            logger.info(
                f"[_set_table_partitions] changed partition cols from {current_partition_fields} to {partition_fields}"
            )
        logger.info("[_set_table_partitions|out]")

    def _get_table(self, path) -> Optional[DeltaTable]:
        logger.debug(f"[_get_table|in] ({path})")
        result: DeltaTable = None
        try:
            result: DeltaTable = DeltaTable.forPath(UtilsSpark.get_spark_session(), path)
        except AnalysisException as ax:
            logger.warning(f"[_get_table] couldn't load from {path}: {ax}")

        logger.debug(f"[_get_table|out] => {result}")
        return result

    def set_column_comments(self, db: str, table: str, col_comments: Dict[str, str]) -> None:
        logger.info(f"[set_column_comments|in] ({db}, {table}, {col_comments})")
        spark = UtilsSpark.get_spark_session()

        table_description: DataFrame = spark.sql(f"describe {db}.{table}").withColumn(
            "set_column_comments_id", monotonically_increasing_id()
        )
        id = table_description.filter(F.col("col_name") == "# Partitioning").collect()[0].set_column_comments_id
        table_description = table_description.filter(
            (F.col("set_column_comments_id") < F.lit(id)) & (F.col("col_name") != "")
        ).drop("set_column_comments_id")
        rows = [r.asDict() for r in table_description.collect()]
        for row in rows:
            col = row["col_name"]
            data_type = row["data_type"]
            if col in col_comments:
                new_comment = col_comments[col]
                logger.info(f"[set_column_comments] setting new comment ({new_comment}) to column {col}")
                spark.sql(f"ALTER TABLE {db}.{table} CHANGE COLUMN {col} {col} {data_type} COMMENT '{new_comment}'")

        logger.info("[set_column_comments|out]")
