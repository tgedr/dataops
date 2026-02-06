"""Module for copying files between S3 buckets.

This module provides:
- S3FileCopy: class for copying objects/files from one S3 bucket to another
"""
import logging
from pathlib import Path
from typing import Any
from tgedr_dataops.commons.s3_connector import S3Connector

from tgedr_dataops_abs.source import Source, SourceException
from tgedr_dataops.commons.utils_fs import process_s3_path, resolve_s3_protocol


logger = logging.getLogger(__name__)


class S3FileCopy(Source, S3Connector):
    """class used to copy objects/files from an s3 bucket to another s3 bucket."""

    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"
    CONTEXT_KEY_FILES = "files"
    CONTEXT_KEY_SUFFIX = "suffix"
    CONTEXT_KEY_PRESERVE_SOURCE_KEY = "preserve_source_key"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the S3FileCopy source.

        Parameters
        ----------
        config : dict[str, Any], optional
            Configuration dictionary containing optional AWS credentials.
        """
        Source.__init__(self, config=config)
        S3Connector.__init__(self)

    def list(self, context: dict[str, Any] | None = None) -> list[str]:
        """List objects in an S3 bucket matching the given prefix.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' (source S3 URL) and optionally 'suffix' to filter results.

        Returns
        -------
        list[str]
            List of S3 URLs matching the criteria.

        Raises
        ------
        SourceException
            If the 'source' key is missing from the context.
        """
        logger.info(f"[list|in] ({context})")

        result: list[str] = []
        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        s3_protocol: str = resolve_s3_protocol(context[self.CONTEXT_KEY_SOURCE])
        protocol = "" if s3_protocol is None else s3_protocol

        bucket, key = process_s3_path(context[self.CONTEXT_KEY_SOURCE])

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

    def get(self, context: dict[str, Any] | None = None) -> Any:
        """Copy a file from one S3 location to another.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' (source S3 URL) and 'target' (target S3 URL).

        Returns
        -------
        Any
            The target S3 URL.

        Raises
        ------
        SourceException
            If source or target context is missing.
        """
        logger.info(f"[get|in] ({context})")

        result: list[str] = []
        if self.CONTEXT_KEY_FILES not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_FILES}")
        if self.CONTEXT_KEY_TARGET not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_TARGET}")

        preserve_source_key = False
        if self.CONTEXT_KEY_PRESERVE_SOURCE_KEY in context:
            preserve_source_key = str(context[self.CONTEXT_KEY_PRESERVE_SOURCE_KEY]).lower() in ["1", "true"]
        logger.info(f"[get] preserve_source_key: {preserve_source_key}")

        target_bucket, target_key = process_s3_path(context[self.CONTEXT_KEY_TARGET])
        s3_protocol: str = resolve_s3_protocol(context[self.CONTEXT_KEY_TARGET])
        protocol = "" if s3_protocol is None else s3_protocol

        files = context[self.CONTEXT_KEY_FILES]
        for file in files:
            src_bucket, src_key = process_s3_path(file)
            src = {"Bucket": src_bucket, "Key": src_key}

            target_key = target_key.rstrip("/") if target_key.endswith("/") else target_key
            if preserve_source_key:
                key = str(Path(target_key) / src_key)
            else:
                src_key_leaf = Path(src_key).name
                key = str(Path(target_key) / src_key_leaf)

            logger.info(
                f"[get] copying... src bucket: {src_bucket}   src key: {src_key}   target bucket: {target_bucket}   target key: {key}"
            )
            self._client.copy(src, target_bucket, key)

            result.append(str(Path(protocol) / target_bucket / key))

        logger.info(f"[get|out] => {result}")
        return result
