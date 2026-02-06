"""Module for reading pandas DataFrames from S3 sources.

This module provides:
- PdDfS3Source: class for reading CSV and Excel files from S3 into pandas DataFrames.
"""
from io import StringIO
import logging
from typing import Any, ClassVar
import pandas as pd

from tgedr_dataops.commons.utils_fs import process_s3_url
from tgedr_dataops.source.abstract_s3_file_source import AbstractS3FileSource
from tgedr_dataops_abs.source import SourceException

logger = logging.getLogger()


class PdDfS3Source(AbstractS3FileSource):
    """class used to read a pandas dataframe from a csv file in s3."""

    CONTEXT_KEY_FILE_FORMAT = "file_format"
    CONTEXT_KEY_SEPARATOR = "sep"
    CONTEXT_KEY_NO_HEADER = "no_header"
    CONTEXT_KEY_COLUMN_NAMES = "column_names"
    CONTEXT_KEY_SCHEMA_TYPES = "schema_types"
    DEFAULT_FORMAT = "csv"
    FORMATS: ClassVar[list[str]] = ["csv", "xlsx"]
    DEFAULT_SEPARATOR = ","

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the PdDfS3Source with optional configuration.

        Parameters
        ----------
        config : dict[str, Any], optional
            Configuration dictionary for S3 connection, by default None.
        """
        super().__init__(config=config)

    def get(self, context: dict[str, Any] | None = None) -> pd.DataFrame:
        """Retrieve and load a pandas DataFrame from S3.

        Supports CSV and Excel formats.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing:
            - 'source' or CONTEXT_KEY_URL: S3 URL
            - 'file_format': File format (csv or xlsx), defaults to csv
            - 'sep': CSV separator, defaults to comma
            - 'no_header': If present, CSV has no header row
            - 'column_names': List of column names for CSV
            - 'schema_types': Dictionary of column types for CSV

        Returns
        -------
        pd.DataFrame
            The loaded pandas DataFrame.

        Raises
        ------
        SourceException
            If source URL is missing or if file_format is unsupported.
        """
        logger.info(f"[get|in] ({context})")
        result: pd.DataFrame = None

        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        _format: str = self.DEFAULT_FORMAT
        if self.CONTEXT_KEY_FILE_FORMAT in context:
            _format = context[self.CONTEXT_KEY_FILE_FORMAT]
            if _format not in self.FORMATS:
                raise SourceException(f"[get] invalid format: {_format}")

        result = self.__read_csv(context=context) if "csv" == _format else self.__read_excel(context=context)

        logger.info(f"[get|out] => {result}")
        return result

    def __read_csv(self, context: dict[str, Any] | None = None) -> pd.DataFrame:
        """Read a CSV file from S3 into a pandas DataFrame.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary with S3 URL and CSV reading options.

        Returns
        -------
        pd.DataFrame
            The loaded DataFrame from CSV.
        """
        logger.info(f"[__read_csv|in] ({context})")

        _, bucket, key = process_s3_url(context[self.CONTEXT_KEY_URL])

        obj = self._client.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")

        header = 0 if self.CONTEXT_KEY_NO_HEADER not in context else None
        names = context.get(self.CONTEXT_KEY_COLUMN_NAMES, None)
        dtype = context.get(self.CONTEXT_KEY_SCHEMA_TYPES, None)
        sep = context.get(self.CONTEXT_KEY_SEPARATOR, self.DEFAULT_SEPARATOR)

        result: pd.DataFrame = pd.read_csv(StringIO(data), sep=sep, header=header, names=names, dtype=dtype)

        logger.info(f"[__read_csv|out] => {result}")
        return result

    def __read_excel(self, context: dict[str, Any] | None = None) -> pd.DataFrame:
        """Read an Excel file from S3 into a pandas DataFrame.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing S3 URL.

        Returns
        -------
        pd.DataFrame
            The loaded DataFrame from Excel.
        """
        logger.info(f"[__read_excel|in] ({context})")
        src = context[self.CONTEXT_KEY_URL]
        result: pd.DataFrame = pd.read_excel(src, engine="openpyxl")
        logger.info(f"[__read_excel|out] => {result}")
        return result
