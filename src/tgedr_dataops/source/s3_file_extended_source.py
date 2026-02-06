"""S3 file extended source module for retrieving objects from S3 with metadata support.

This module provides:
- S3FileExtendedSource: class for retrieving S3 objects/files with metadata extraction
"""
import logging
from typing import Any, ClassVar

from tgedr_dataops.source.s3_file_source import S3FileSource
from tgedr_dataops_abs.source import SourceException
from tgedr_dataops.commons.utils_fs import process_s3_path


logger = logging.getLogger(__name__)


class S3FileExtendedSource(S3FileSource):
    """Class used to retrieve objects/files from S3 bucket to local filesystem."""

    METADATA_KEYS: ClassVar[list[str]] = ["LastModified", "ContentLength", "ETag", "VersionId", "ContentType"]

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the S3FileExtendedSource.

        Parameters
        ----------
        config : dict[str, Any], optional
            Configuration dictionary containing optional AWS credentials.
        """
        super().__init__(config=config)

    def get_metadata(self, context: dict[str, Any] | None = None) -> dict[str, Any]:
        """Retrieve metadata for an S3 object.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' S3 URL.

        Returns
        -------
        dict[str, Any]
            Dictionary containing object metadata including size, last modified time, etc.

        Raises
        ------
        SourceException
            If source context is missing.
        """
        logger.info(f"[get_metadata|in] ({context})")

        result: dict[str, Any] = {}
        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        bucket, key = process_s3_path(context[self.CONTEXT_KEY_SOURCE])

        o = self._client.head_object(Bucket=bucket, Key=key)

        for key in list(o.keys()):
            if key in self.METADATA_KEYS:
                if key == "LastModified":
                    result[key] = int(o[key].timestamp())
                else:
                    result[key] = o[key]

        logger.info(f"[get_metadata|out] => result len: {len(result)}")
        return result
