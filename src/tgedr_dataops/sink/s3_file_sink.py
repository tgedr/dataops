"""S3 sink implementation for persisting files to S3.

This module provides:
- S3FileSink: sink class for saving/persisting files to S3 buckets.
"""
import logging
from pathlib import Path
from typing import Any
from tgedr_dataops.commons.s3_connector import S3Connector

from tgedr_dataops_abs.sink import Sink, SinkException
from tgedr_dataops.commons.utils_fs import remove_s3_protocol


logger = logging.getLogger(__name__)


class S3FileSink(Sink, S3Connector):
    """sink class used to save/persist a local object/file to an s3 bucket."""

    CONTEXT_SOURCE_PATH = "source"
    CONTEXT_TARGET_PATH = "target"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the S3FileSink.

        Parameters
        ----------
        config : dict[str, Any], optional
            Configuration dictionary.
        """
        Sink.__init__(self, config=config)
        S3Connector.__init__(self)

    def put(self, context: dict[str, Any] | None = None) -> Any:
        """Upload a local file to S3 bucket.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' (local file) and 'target' (S3 URL) paths.

        Raises
        ------
        SinkException
            If source or target context is missing, or if source is a directory.
        """
        logger.info(f"[put|in] ({context})")

        if self.CONTEXT_SOURCE_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_SOURCE_PATH}")
        if self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        source = context[self.CONTEXT_SOURCE_PATH]
        if Path(source).is_dir():
            raise SinkException("source can't be a folder, must be a file")

        target = remove_s3_protocol(context[self.CONTEXT_TARGET_PATH])
        target_elements = target.split("/")
        target_bucket = target_elements[0]
        target_key = "/".join(target_elements[1:])

        if target_key.endswith("/"):
            target_file = Path(source).name
            target_key = target_key + target_file

        logger.info(f"[put] uploading {source} to key: {target_key} in bucket: {target_bucket}")
        self._client.upload_file(Filename=source, Bucket=target_bucket, Key=target_key)

        logger.info("[put|out]")

    def delete(self, context: dict[str, Any] | None = None) -> None:
        """Delete an object from S3 bucket.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'target' (S3 URL) path.

        Raises
        ------
        SinkException
            If target context is missing.
        """
        logger.info(f"[delete|in] ({context})")

        if self.CONTEXT_TARGET_PATH not in context:
            raise SinkException(f"you must provide context for {self.CONTEXT_TARGET_PATH}")

        target = remove_s3_protocol(context[self.CONTEXT_TARGET_PATH])
        target_elements = target.split("/")
        bucket = target_elements[0]
        key = "/".join(target_elements[1:])

        response = self._client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"[delete] response: {response}")
        logger.info("[delete|out]")
