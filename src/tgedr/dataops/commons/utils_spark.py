import logging
import os
from typing import Dict
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import types as T
from pyspark.context import SparkContext


logger = logging.getLogger(__name__)


class UtilsSpark:
    """class with handy functions to work with spark"""

    __ENV_KEY_PYSPARK_IS_LOCAL = "PYSPARK_IS_LOCAL"
    __ENV_KEY_NOT_AWS_CLOUD = "NOT_AWS_CLOUD"
    __DTYPES_MAP = {
        "bigint": T.LongType,
        "string": T.StringType,
        "double": T.DoubleType,
        "int": T.IntegerType,
        "boolean": T.BooleanType,
        "timestamp": T.TimestampType,
        "date": T.DateType,
    }

    @staticmethod
    def get_local_spark_session(config: dict = None) -> SparkSession:
        logger.debug(f"[get_local_spark_session|in] ({config})")
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages io.delta:delta-core_2.12:2.3.0 pyspark-shell"
        builder = (
            SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.host", "localhost")
        )

        if config is not None:
            for k, v in config.items():
                builder.config(k, v)

        spark = builder.getOrCreate()

        logger.debug(f"[get_local_spark_session|out] => {spark}")
        return spark

    @staticmethod
    def get_spark_session(config: dict = None) -> SparkSession:
        logger.debug(f"[get_spark_session|in] ({config})")

        if "1" == os.getenv(UtilsSpark.__ENV_KEY_PYSPARK_IS_LOCAL):
            spark: SparkSession = UtilsSpark.get_local_spark_session(config)
        else:
            if "1" == os.getenv(UtilsSpark.__ENV_KEY_NOT_AWS_CLOUD):
                active_session = SparkSession.getActiveSession()
            else:
                from awsglue.context import GlueContext

                glueContext = GlueContext(SparkContext.getOrCreate())
                active_session = glueContext.spark_session

            spark_config = SparkConf()

            if active_session is not None:
                former_config = active_session.sparkContext.getConf().getAll()
                for entry in former_config:
                    spark_config.set(entry[0], entry[1])
            if config is not None:
                for k, v in config.items():
                    spark_config.set(k, v)
                spark: SparkSession = SparkSession.builder.config(conf=spark_config).getOrCreate()
            else:
                spark: SparkSession = SparkSession.builder.getOrCreate()

        logger.debug(f"[get_spark_session|out] => {spark}")
        return spark

    @staticmethod
    def build_schema_from_dtypes(dtypes_schema: Dict[str, str]) -> T.StructType:
        logger.info(f"[build_schema_from_dtypes|in] ({dtypes_schema})")
        result = T.StructType()
        for field, dtype in dtypes_schema.items():
            new_type = UtilsSpark.__DTYPES_MAP[dtype]
            result.add(field, new_type(), True)

        logger.info(f"[build_schema_from_dtypes|out] => {result}")
        return result
