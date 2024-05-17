# data-ops
data operations related code

## motivation
*data-ops* is a library with tested and used code *"from the trenches"*, with the purpose to align on some standards regarding code structure and quality and to avoid reinventing small wheels

## package namespaces and its contents

#### /
- __Chain__ : chain-like abstract class (for sequential processing) ([example](test/tgedr/dataops/test_processor_chain.py))
- __Etl__ : Extract-Transform-Load abstract class to be extended and used in data pipelines ([example](test/tgedr/dataops/test_etl.py))
- __Processor__ : abstract class for data processing ([example](test/tgedr/dataops/test_processor_chain.py))
- __UtilsReflection__ : utility class to manage runtime class and implementation loading ([example](test/tgedr/dataops/test_utils_reflection.py))

#### commons
- __Dataset__: immutable class to wrap up a dataframe along with metadata ([example](test/tgedr/dataops/commons/test_dataset.py))
- __Metadata__: immutable class depicting dataset metadata ([example](test/tgedr/dataops/commons/test_metadata.py))
- __S3Connector__: base class to be extended, providing a connection session with aws s3 resources
- __utils_fs__: utility module with file system related functions ([example](test/tgedr/dataops/commons/test_utils_fs.py))
- __UtilsSpark__: utility class to work with spark, mostly helping on creating a session ([example](test/tgedr/dataops/commons/test_utils_spark.py))

#### sink
- __Sink__: abstract **sink** class defining methods (`put`and `delete`) to manage persistence of data somewhere as defined by implementing classes
- __LocalFsFileSink__: __sink__ class used to save/persist an object/file to a local fs location ([example](test/tgedr/dataops/sink/test_localfs_file_sink.py))
- __S3FileSink__: __sink__ class used to save/persist a local object/file to an s3 bucket ([example](test/tgedr/dataops/sink/test_s3_file_sink.py))

#### source
- __Source__: abstract **source** class defining methods (`list` and `get`) to manage retrieval of data from somewhere as defined by implementing classes
- __LocalFsFileSource__: __source__ class used to retrieve local objects/files to another local fs location ([example](test/tgedr/dataops/source/test_localfs_file_source.py))
- __S3FileSource__: __source__ class used to retrieve objects/files from s3 bucket to local fs location ([example](test/tgedr/dataops/source/test_s3_file_source.py))
- __S3FileCopy__: __source__ class used to copy objects/files from an s3 bucket to another s3 bucket ([example](test/tgedr/dataops/source/test_s3_copy.py))

#### store
- __Store__ : abstract class used to manage persistence, defining CRUD-like (CreateReadUpdateDelete) methods
- __FsSinglePartitionParquetStore__ : abstract __store__ implementation defining persistence on parquet files with an optional single partition, regardless of the location it should persist
- __LocalFsSinglePartitionParquetStore__ : __FsSinglePartitionParquetStore__ implementation using local file system ([example](test/tgedr/dataops/store/test_local_fs_single_partition_parquet.py))
- __S3FsSinglePartitionParquetStore__ : __FsSinglePartitionParquetStore__ implementation using aws s3 file system ([example](test/tgedr/dataops/store/MANUAL_test_s3_single_partition_parquet.py))
- __SparkDeltaStore__ : __store__ implementation for pyspark distributed processing with delta table format ([example](test/tgedr/dataops/store/test_spark_delta.py))

#### validation
- __DataValidation__ : abstract class defining a `validate` method to perform data validation, currently using Great Expectations library
- __pandas.Impl__ : __DataValidation__ implementation to validate pandas dataframes with Great Expectations library ([example](test/tgedr/dataops/validation/test_validation.py))



## installation
        `pip install tgedr-dataops`

## known issues/features to implement
- store/spark_delta.py#update method: implement schema evolution feature
