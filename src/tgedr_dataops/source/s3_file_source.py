"""S3 file source module for retrieving objects from S3 buckets.

This module provides:
- S3FileSource: A source implementation for listing and downloading files from S3.
"""
import logging
from pathlib import Path
from typing import Any
from tgedr_dataops.commons.s3_connector import S3Connector

from tgedr_dataops_abs.source import Source, SourceException
from tgedr_dataops.commons.utils_fs import remove_s3_protocol, resolve_s3_protocol


logger = logging.getLogger(__name__)


class S3FileSource(Source, S3Connector):
    """class used to retrieve objects/files from s3 bucket to local fs location."""

    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"
    CONTEXT_KEY_FILES = "files"
    CONTEXT_KEY_SUFFIX = "suffix"

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the S3FileSource.

        Parameters
        ----------
        config : dict[str, Any], optional
            Configuration dictionary containing optional AWS credentials.
        """
        Source.__init__(self, config=config)
        S3Connector.__init__(self)

    def list(self, context: dict[str, Any] | None = None) -> list[str]:
        """List files in the S3 bucket.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' (S3 URL) and optionally 'suffix' for filtering.

        Returns
        -------
        list[str]
            List of S3 object URLs in the source bucket/prefix.

        Raises
        ------
        SourceException
            If source context is missing.
        """
        logger.info(f"[list|in] ({context})")

        result: list[str] = []
        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")

        s3_protocol: str = resolve_s3_protocol(context[self.CONTEXT_KEY_SOURCE])
        protocol = "" if s3_protocol is None else s3_protocol

        path = remove_s3_protocol(context[self.CONTEXT_KEY_SOURCE])
        path_elements = path.split("/")
        bucket = path_elements[0]
        key = "/".join(path_elements[1:])

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
        """Download a file from S3 to local filesystem.

        Parameters
        ----------
        context : dict[str, Any], optional
            Context dictionary containing 'source' (S3 URL) and 'target' (local path).

        Returns
        -------
        Any
            The local target file path.

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

        files = context[self.CONTEXT_KEY_FILES]
        target = context[self.CONTEXT_KEY_TARGET]

        target_is_dir: bool = Path(target).is_dir()
        target = target.rstrip("/") if target.endswith("/") else target

        for file in files:
            path_elements = remove_s3_protocol(file).split("/")
            bucket = path_elements[0]
            key = "/".join(path_elements[1:])
            filename = path_elements[-1]
            local_file = str(Path(target) / filename) if target_is_dir else target

            # assure we have that path there
            local_folder = Path(local_file).parent
            if not local_folder.is_dir():
                local_folder.mkdir(parents=True)

            logger.info(f"[get] bucket: {bucket}   key: {key}   file: {file}   local_file: {local_file}")
            self._client.download_file(Bucket=bucket, Key=key, Filename=local_file)
            result.append(local_file)

        logger.info(f"[get|out] => {result}")
        return result
