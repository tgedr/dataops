"""Abstract base class for S3 file sources.

This module provides:
- AbstractS3FileSource: abstract class for reading file sources from S3.
"""
from abc import ABC
import logging
from typing import Any, Optional

from tgedr_dataops.commons.s3_connector import S3Connector
from tgedr_dataops.commons.utils_fs import process_s3_url
from tgedr_dataops_abs.source import Source, SourceException


logger = logging.getLogger()


class AbstractS3FileSource(Source, S3Connector, ABC):
    """abstract class used to read file sources from s3"""

    CONTEXT_KEY_URL = "url"
    CONTEXT_KEY_SUFFIX = "suffix"

    def __init__(self, config: Optional[dict[str, Any]] = None):
        Source.__init__(self, config=config)
        S3Connector.__init__(self)

    def list(self, context: Optional[dict[str, Any]] = None) -> list[str]:
        logger.info(f"[list|in] ({context})")

        result: list[str] = []
        if self.CONTEXT_KEY_URL not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_URL}")

        protocol, bucket, key = process_s3_url(context[self.CONTEXT_KEY_URL])

        objs = self._client.list_objects_v2(Bucket=bucket, Prefix=key)
        result = [
            (protocol + bucket + "/" + entry["Key"]) for entry in objs["Contents"] if not (entry["Key"]).endswith("/")
        ]

        if self.CONTEXT_KEY_SUFFIX in context:
            suffix: str = context[self.CONTEXT_KEY_SUFFIX]
            result = [f for f in result if f.endswith(suffix)]

        logger.debug(f"[list|out] => {result}")
        logger.info(f"[list|out] => result len: {len(result)}")
        return result
