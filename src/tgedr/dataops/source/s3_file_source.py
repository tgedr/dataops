import logging
import os
from typing import Any, Dict, Optional
from tgedr.dataops.commons.s3_connector import S3Connector
from tgedr.dataops.source.source import Source, SourceException


logger = logging.getLogger(__name__)


class S3FileSource(Source, S3Connector):
    CONTEXT_KEY_SOURCE = "source"
    CONTEXT_KEY_TARGET = "target"

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        Source.__init__(self, config=config)
        S3Connector.__init__(self)

    def __derive_local_file(self, target: str, isdir: bool, file: str):
        logger.info(f"[__derive_local_file|in] ({target}, {isdir}, {file})")

        if isdir:
            result = os.path.join(target, file)
            basedir = os.path.dirname(result)
            if not os.path.exists(basedir):
                logger.info(f"[__derive_local_file] creating folder: {basedir}")
                os.mkdir(basedir)
        else:
            result = target

        logger.info(f"[__derive_local_file|out] => {result}")
        return result

    def get(self, context: Optional[Dict[str, Any]] = None) -> Any:
        logger.info(f"[get|in] ({context})")

        if self.CONTEXT_KEY_SOURCE not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_SOURCE}")
        if self.CONTEXT_KEY_TARGET not in context:
            raise SourceException(f"you must provide context for {self.CONTEXT_KEY_TARGET}")

        path = context[self.CONTEXT_KEY_SOURCE]
        path_elements = path.split("/")
        bucket = path_elements[0]
        key = "/".join(path_elements[1:])
        target = context[self.CONTEXT_KEY_TARGET]
        target_is_dir = os.path.isdir(target)

        objs = self._client.list_objects_v2(Bucket=bucket, Prefix=key)
        files = [entry["Key"] for entry in objs["Contents"] if not (entry["Key"]).endswith("/")]

        for file in files:
            logger.info(f"[get] found file: {file}")
            local_file = self.__derive_local_file(target=target, isdir=target_is_dir, file=file)
            logger.info(f"[get] bucket: {bucket}   key: {key}   file: {file}   local_file: {local_file}")
            self._client.download_file(Bucket=bucket, Key=file, Filename=local_file)

        logger.info("[get|out]")
