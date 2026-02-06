# data-ops

![Coverage](./coverage.svg)
[![PyPI](https://img.shields.io/pypi/v/tgedr-dataops)](https://pypi.org/project/tgedr-dataops/)



data operations related code

## motivation
*data-ops* is a library with tested and used code aligning on some standards regarding code structure and quality and to avoid reinventing the wheel.
It builds on top of *dataops-abs*.

## installation
        `pip install tgedr-dataops`

## package namespaces and its contents

#### commons
- __S3Connector__: base class to be extended, providing a connection session with aws s3 resources
- __utils_fs__: utility module with file system related functions ([example](tests/tgedr_dataops/commons/test_utils_fs.py))

#### quality
- __PandasValidation__ : __GreatExpectationsValidation__ implementation to validate pandas dataframes with Great Expectations library ([example](tests/tgedr_dataops/quality/test_pandas_validation.py))


#### sink
- __LocalFsFileSink__: __Sink__ implementation class used to save/persist an object/file to a local fs location ([example](tests/tgedr_dataops/sink/test_localfs_file_sink.py))
- __S3FileSink__: __Sink__ implementation class used to save/persist a local object/file to an s3 bucket ([example](tests/tgedr_dataops/sink/test_s3_file_sink.py))

#### source
- __AbstractS3FileSource__: abstract __Source__ class used to retrieve objects/files from s3 bucket to local fs location circumventing some formats download limitation
- __LocalFsFileSource__: __Source__ implementation class used to retrieve local objects/files to another local fs location ([example](tests/tgedr_dataops/source/test_localfs_file_source.py))
- __PdDfS3Source__: __Source__ implementation class used to read a pandas dataframe from s3, whether a csv or an excel (xslx) file ([example csv](tests/tgedr_dataops/source/test_pd_df_s3_source_csv.py), [example excel](tests/tgedr_dataops/source/test_pd_df_s3_source_excel.py))
- __S3FileCopy__: __Source__ implementation class used to copy objects/files from an s3 bucket to another s3 bucket ([example](tests/tgedr_dataops/source/test_s3_copy.py))
- __S3FileExtendedSource__: __Source__ implementation class used to retrieve objects/files from s3 bucket to local fs location with the extra method `get_metadata` providing sile metadata ("LastModified", "ContentLength", "ETag", "VersionId", "ContentType")([example](tests/tgedr_dataops/source/test_s3_file_extended_source.py))
- __S3FileSource__: __Source__ implementation class used to retrieve objects/files from s3 bucket to local fs location ([example](tests/tgedr_dataops/source/test_s3_file_source.py))

#### store
- __FsSinglePartitionParquetStore__ : abstract __Store__ implementation defining persistence on parquet files with an optional single partition, regardless of the location it should persist
- __LocalFsSinglePartitionParquetStore__ : __FsSinglePartitionParquetStore__ implementation using local file system ([example](tests/tgedr_dataops/store/test_local_fs_single_partition_parquet.py))
- __S3FsSinglePartitionParquetStore__ : __FsSinglePartitionParquetStore__ implementation using aws s3 file system ([example](tests/tgedr_dataops/store/MANUAL_test_s3_single_partition_parquet.py))
- __ParquetStore__ : __Store__ implementation class for interacting with Parquet files using a filesystem interface ([example](tests/tgedr_dataops/store/test_parquet_store.py))

## known issues/further development

- update data while changing its partition value ([check unit test](tests/tgedr_dataops/store/test_parquet_store.py#L175))

## development
- main requirements:
  - _uv_  
  - _bash_
- Clone the repository like this:

  ``` bash
  git clone git@github.com:tgedr/dataops
  ```
- cd into the folder: `cd dataops`
- install requirements: `./helper.sh reqs`



