from io import StringIO
import logging
from typing import Any, Dict, Optional
import pandas as pd

from tgedr.dataops.commons.utils_fs import process_s3_url
from tgedr.dataops.source.abstract_s3_file_source import AbstractS3FileSource
from tgedr.dataops.source.source import SourceException

logger = logging.getLogger()


class PdDfS3Source(AbstractS3FileSource):
    """class used to read a pandas dataframe from a csv file in s3"""

    CONTEXT_KEY_FILE_FORMAT = "file_format"
    CONTEXT_KEY_SEPARATOR = "sep"
    CONTEXT_KEY_NO_HEADER = "no_header"
    CONTEXT_KEY_COLUMN_NAMES = "column_names"
    CONTEXT_KEY_SCHEMA_TYPES = "schema_types"
    DEFAULT_FORMAT = "csv"
    DEFAULT_SEPARATOR = ","

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config=config)

    def get(self, context: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """retrieves a pandas dataframe, by default reading it from a csv,
        you can ask for a different format using the context key 'file_format' (available formats: csv)"""
        logger.info(f"[get|in] ({context})")
        result: pd.DataFrame = None

        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        protocol, bucket, key = process_s3_url(context[self.CONTEXT_KEY_URL])

        obj = self._client.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read().decode("utf-8")

        header = 0 if self.CONTEXT_KEY_NO_HEADER not in context else None
        names = None if self.CONTEXT_KEY_COLUMN_NAMES not in context else context[self.CONTEXT_KEY_COLUMN_NAMES]
        dtype = None if self.CONTEXT_KEY_SCHEMA_TYPES not in context else context[self.CONTEXT_KEY_SCHEMA_TYPES]
        sep = (
            self.DEFAULT_SEPARATOR if self.CONTEXT_KEY_SEPARATOR not in context else context[self.CONTEXT_KEY_SEPARATOR]
        )

        result = pd.read_csv(StringIO(data), sep=sep, header=header, names=names, dtype=dtype)

        logger.info(f"[get|out] => {result}")
        return result