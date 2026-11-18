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
- __utils_github__: utility module for github, with no `Source`/`Sink` dependency; raises its own `GithubApiException`/`GithubAccountNotFoundException`. Provides:
  - coordinate parsing and formatting: `parse_github_source`, `parse_github_file`, `format_github_file` (refs are pinned with a `?ref=<branch>` suffix, so branch names containing `/` stay unambiguous)
  - host resolution: `normalize_host`, `enterprise_subdomain`, `api_base_url` (public `github.com` and GitHub Enterprise Cloud `<organization>.ghe.com`)
  - url builders: `contents_url`, `raw_file_url`, `account_repositories_url`
  - `list_account_repositories(account, token, host, timeout, filters, session)`: lists the repositories of an account or organization, following pagination until the last page and returning curated metadata dicts; `RepositoryFilters` filters by `suffix`, `include_archived`, `include_forks`, `include_private`, `include_disabled` ([example](tests/tgedr_dataops/commons/test_utils_github.py))

#### quality
- __PandasValidation__ : __GreatExpectationsValidation__ implementation to validate pandas dataframes with Great Expectations library ([example](tests/tgedr_dataops/quality/test_pandas_validation.py))


#### sink
- __LocalFsFileSink__: __Sink__ implementation class used to save/persist an object/file to a local fs location ([example](tests/resources/sink/test_localfs_file_sink.py))
- __S3FileSink__: __Sink__ implementation class used to save/persist a local object/file to an s3 bucket ([example](tests/resources/sink/test_s3_file_sink.py))

#### source
- __AbstractS3FileSource__: abstract __Source__ class used to retrieve objects/files from s3 bucket to local fs location circumventing some formats download limitation
- __GithubFileSource__: __Source__ implementation class used to retrieve files from a github repository to a local fs location ([example](tests/tgedr_dataops/source/test_github_file_source.py))
  - `list`: `source` is `<owner>/<repo>` (also accepts a `raw.githubusercontent.com` url, or `<organization>.ghe.com/<owner>/<repo>`), plus optional `ref`/`branch`, `path` and `suffix`; returns `<owner>/<repo>/<path>?ref=<ref>` coordinates
  - `get`: `files` holds those coordinates (the `?ref=` suffix is optional and wins over the `branch` context key, which defaults to `main`); `target` is interpreted as a folder only when it already exists as one, otherwise as a file path, matching `LocalFsFileSource`/`S3FileSource`
  - a coordinate keeps the ref it was listed with, so repos whose default branch is not `main` (e.g. `master`, `development`) round-trip from `list` to `get` correctly
  - colliding file names in a folder target are saved as `<account>_<repo>_<name>`, so collecting many `README.md` files no longer overwrites them
  - config accepts optional `token`, `timeout`, `host` (e.g. `<organization>.ghe.com` for GitHub Enterprise Cloud with data residency) and `use_api_download`; the token can also come from `GITHUB_TOKEN`/`GH_TOKEN` or url userinfo, and is never logged

  ``` python
  from tgedr_dataops.source.github_file_source import GithubFileSource

  o = GithubFileSource()  # or GithubFileSource({"token": ..., "host": "<organization>.ghe.com"})

  files = o.list(context={"source": "jtviegas/airesdeornelas", "branch": "development", "suffix": "README.md"})
  # ['jtviegas/airesdeornelas/README.md?ref=development']

  o.get(context={"files": files, "target": "/tmp/out/README.md"})  # file target
  # ['/tmp/out/README.md']

  o.get(context={"files": files, "target": "/tmp/out"})            # existing folder target
  # ['/tmp/out/README.md']

  # or a lengthy one, downloading all README.md's from repos in 2 accounts/orgs
  accounts = {"account1": "GH_TOKEN_1", "account2": "GH_TOKEN_2"}

  dump_url = tempfile.mkdtemp()
  print(f"dump_url: {dump_url}")

  for account, token_env in accounts.items():
      token = os.getenv(token_env)
      repos = list_account_repositories(
          account,
          filters=RepositoryFilters(include_private=False, include_forks=False, include_archived=False),
          token=token,
      )
      o = GithubFileSource({"token": token})
      readmes = []
      for repo in repos:
          readmes += o.list(
              context={"source": repo["full_name"], "branch": repo["default_branch"], "suffix": "README.md"}
          )
      o.get(context={"files": readmes, "target": dump_url})

  ```
- __LocalFsFileSource__: __Source__ implementation class used to retrieve local objects/files to another local fs location ([example](tests/tgedr_dataops/source/test_localfs_file_source.py))
- __PdDfS3Source__: __Source__ implementation class used to read a pandas dataframe from s3, whether a csv or an excel (xslx) file ([example csv](tests/tgedr_dataops/source/test_pd_df_s3_source_csv.py), [example excel](tests/tgedr_dataops/source/test_pd_df_s3_source_excel.py))
- __S3FileCopy__: __Source__ implementation class used to copy objects/files from an s3 bucket to another s3 bucket ([example](tests/tgedr_dataops/source/test_s3_copy.py))
- __S3FileExtendedSource__: __Source__ implementation class used to retrieve objects/files from s3 bucket to local fs location with the extra method `get_metadata` providing sile metadata ("LastModified", "ContentLength", "ETag", "VersionId", "ContentType")([example](tests/tgedr_dataops/source/test_s3_file_extended_source.py))
- __S3FileSource__: __Source__ implementation class used to retrieve objects/files from s3 bucket to local fs location ([example](tests/tgedr_dataops/source/test_s3_file_source.py))

#### store
- __DataFrameSplits__ : dataclass container for `train`, `test`, and optional `validation` pandas DataFrames, providing helpers to build, merge, and convert splits to/from Hugging Face datasets
- __FsSinglePartitionParquetStore__ : abstract __Store__ implementation defining persistence on parquet files with an optional single partition, regardless of the location it should persist
- __HuggingFaceDatasetStore__ : __Store__ implementation class for interacting with Hugging Face Hub datasets, supporting CRUD operations on datasets and their splits ([example](tests/tgedr_dataops/store/test_hf_dataset.py))
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



